"""
Pytest configuration and shared fixtures for the test suite.
"""

import os
import socket
import tempfile
import threading
import time

import pytest
from flask import jsonify

from malla.config import AppConfig
from malla.models.user import UserRole
from malla.services.auth_service import AuthService

# Import the application factory
from src.malla.web_ui import create_app
from tests.fixtures.database_fixtures import DatabaseFixtures
from tests.fixtures.traceroute_graph_data import get_sample_graph_data


@pytest.fixture(scope="session")
def worker_id(request):
    """Get the worker ID for pytest-xdist parallel execution.

    Returns 'master' when not running in parallel, or the worker ID when running
    with pytest-xdist.
    """
    if hasattr(request.config, "workerinput"):
        return request.config.workerinput["workerid"]
    else:
        return "master"


class TestFlaskApp:
    """Test Flask app using the real application with test fixtures."""

    def __init__(self, port=None):
        if port is None:
            # Find an available port
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", 0))
                port = s.getsockname()[1]

        self.port = port
        self.server_thread = None

        # Create a temporary database for testing
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()

        # Build an AppConfig overriding only the database path
        self._cfg = AppConfig(
            database_file=self.temp_db.name,
            host="127.0.0.1",
            port=self.port,
            debug=False,
        )

        # Create the real Flask app with injected config
        self.app = create_app(self._cfg)

        # Set up test data
        self._setup_test_data()

        # Add test-specific API routes
        self._setup_test_routes()

    def _setup_test_data(self):
        """Set up test data in the database."""
        # Initialize database with test fixtures
        db_fixtures = DatabaseFixtures()
        db_fixtures.create_test_database(self.temp_db.name)

    def _setup_test_routes(self):
        """Set up additional test-specific routes."""

        @self.app.route("/api/traceroute/graph")
        def api_traceroute_graph():
            """Serve sample graph data for tests."""
            return jsonify(get_sample_graph_data())

    def start(self):
        """Start the test server in a background thread."""
        if self.server_thread and self.server_thread.is_alive():
            return

        def run_server():
            self.app.run(
                host="127.0.0.1", port=self.port, debug=False, use_reloader=False
            )

        self.server_thread = threading.Thread(target=run_server, daemon=True)
        self.server_thread.start()

        # Wait for server to start
        max_attempts = 50
        for _ in range(max_attempts):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    result = s.connect_ex(("127.0.0.1", self.port))
                    if result == 0:
                        break
            except Exception:
                pass
            time.sleep(0.1)
        else:
            raise RuntimeError(f"Test server failed to start on port {self.port}")

    def stop(self):
        """Stop the test server and clean up."""
        if self.server_thread and self.server_thread.is_alive():
            # Flask's development server doesn't have a clean shutdown method
            # In a real test environment, you might want to use a proper WSGI server
            pass

        # Clean up temporary database
        try:
            os.unlink(self.temp_db.name)
        except FileNotFoundError:
            pass

    @property
    def url(self):
        """Get the base URL for the test server."""
        return f"http://127.0.0.1:{self.port}"


# Legacy GenericTestServer class for backward compatibility
class GenericTestServer:
    """Generic test server for E2E tests - legacy wrapper around TestFlaskApp."""

    def __init__(self, port=None):
        self._test_app = TestFlaskApp(port)
        self.port = self._test_app.port
        self.app = self._test_app.app
        self.server_thread = None

    def add_route(self, path, handler, methods=None):
        """Add a custom route to the server."""
        if methods is None:
            methods = ["GET"]

        self.app.add_url_rule(
            path,
            endpoint=f"custom_{path.replace('/', '_').replace('-', '_')}",
            view_func=handler,
            methods=methods,
        )

    def add_api_route(self, path, data_provider, methods=None):
        """Add a custom API route that returns JSON data."""
        if methods is None:
            methods = ["GET"]

        def make_api_handler(provider):
            def api_handler():
                if callable(provider):
                    data = provider()
                else:
                    data = provider
                return jsonify(data)

            return api_handler

        self.app.add_url_rule(
            path,
            endpoint=f"api_{path.replace('/', '_').replace('-', '_')}",
            view_func=make_api_handler(data_provider),
            methods=methods,
        )

    def start(self):
        """Start the test server."""
        self._test_app.start()
        self.server_thread = self._test_app.server_thread

    def stop(self):
        """Stop the test server."""
        self._test_app.stop()

    @property
    def url(self):
        """Get the base URL for the test server."""
        return self._test_app.url


@pytest.fixture(scope="session")
def test_server(worker_id):
    """Provide a test server instance for the entire test session."""
    # Use different ports for different workers to avoid conflicts
    base_port = 15000
    if worker_id == "master":
        port = base_port
    else:
        # Extract worker number from worker_id (e.g., "gw0" -> 0)
        worker_num = int(worker_id.replace("gw", ""))
        port = base_port + worker_num + 1

    server = TestFlaskApp(port)
    server.start()

    yield server

    server.stop()


@pytest.fixture(scope="session")
def generic_test_server(worker_id):
    """Provide a generic test server instance for backward compatibility."""
    # Use different ports for different workers to avoid conflicts
    base_port = 16000
    if worker_id == "master":
        port = base_port
    else:
        # Extract worker number from worker_id (e.g., "gw0" -> 0)
        worker_num = int(worker_id.replace("gw", ""))
        port = base_port + worker_num + 1

    server = GenericTestServer(port)
    server.start()

    yield server

    server.stop()


@pytest.fixture(scope="function")
def temp_database():
    """Provide a temporary database for individual tests."""
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_db.close()

    # Set up test data
    db_fixtures = DatabaseFixtures()
    db_fixtures.create_test_database(temp_db.name)

    yield temp_db.name

    # Clean up
    try:
        os.unlink(temp_db.name)
    except FileNotFoundError:
        pass


@pytest.fixture(scope="function")
def test_client():
    """Provide a Flask test client for unit tests."""
    # Create a temporary database
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_db.close()

    # Build a config object pointing at the temporary DB
    cfg = AppConfig(database_file=temp_db.name)

    # Reset module-level table-creation flags so that lazy-init tables
    # (like channel_directory, custom_dashboards) are re-created in the fresh temp database.
    import malla.database.channel_directory_repository as _cd_repo
    import malla.database.dashboard_repository as _dash_repo

    _cd_repo._TABLE_CREATED = False
    _dash_repo._TABLE_CREATED = False

    try:
        # Create the app with injected config
        app = create_app(cfg)
        app.config["TESTING"] = True

        # Set up test data
        db_fixtures = DatabaseFixtures()
        db_fixtures.create_test_database(temp_db.name)

        with app.test_client() as client:
            yield client
    finally:
        # Clean up temporary database
        try:
            os.unlink(temp_db.name)
        except FileNotFoundError:
            pass


@pytest.fixture(scope="session")
def test_server_url(test_server):
    """Provide the test server URL."""
    return test_server.url


@pytest.fixture(scope="session")
def traceroute_graph_url(test_server):
    """Provide the traceroute graph URL from the test server."""
    return f"{test_server.url}/traceroute-graph"


@pytest.fixture(scope="session")
def map_url(test_server):
    """Provide the map URL from the test server."""
    return f"{test_server.url}/map"


@pytest.fixture(scope="session")
def map_server(test_server):
    """Provide a map server instance for backward compatibility."""
    return test_server


@pytest.fixture(scope="function")
def app():
    """Provide a Flask app instance for unit tests."""
    # Create a temporary database
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    temp_db.close()

    # Build a config object pointing at the temporary DB
    cfg = AppConfig(database_file=temp_db.name)

    # Reset module-level table-creation flags so that lazy-init tables
    # (like channel_directory, custom_dashboards) are re-created in the fresh temp database.
    import malla.database.channel_directory_repository as _cd_repo
    import malla.database.dashboard_repository as _dash_repo

    _cd_repo._TABLE_CREATED = False
    _dash_repo._TABLE_CREATED = False

    try:
        # Create the app with injected config
        app = create_app(cfg)
        app.config["TESTING"] = True

        # Set up test data
        db_fixtures = DatabaseFixtures()
        db_fixtures.create_test_database(temp_db.name)

        yield app
    finally:
        # Clean up temporary database
        try:
            os.unlink(temp_db.name)
        except FileNotFoundError:
            pass


@pytest.fixture(scope="function")
def client(app):
    """Create a test client for the Flask application."""
    return app.test_client()


@pytest.fixture(scope="function")
def operator_client(app):
    """Create a test client authenticated as an operator user.

    This fixture creates a test user with OPERATOR role and logs them in,
    returning an authenticated client that can access protected endpoints.
    """
    with app.app_context():
        # Create a test operator user
        test_user = AuthService.create_user(
            username="test_operator",
            password="test_password_123",
            role=UserRole.OPERATOR,
        )
        if test_user is None:
            # User may already exist from a previous test, try to get it
            from malla.database.connection import get_db_connection

            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM users WHERE username = ?", ("test_operator",)
            )
            row = cursor.fetchone()
            conn.close()
            if not row:
                raise RuntimeError("Failed to create test operator user")

    client = app.test_client()

    # Log in the test user
    response = client.post(
        "/login",
        data={
            "username": "test_operator",
            "password": "test_password_123",
        },
        follow_redirects=False,
    )

    # Login should redirect to dashboard
    assert response.status_code == 302, (
        f"Login failed with status {response.status_code}: "
        f"{response.get_data(as_text=True)}"
    )

    return client


class TestHelpers:
    """Helper methods for testing API responses and common assertions."""

    def assert_api_response_structure(
        self, response, expected_keys=None, status_code=200
    ):
        """Assert that an API response has the expected structure and status code.

        Args:
            response: Flask test client response object
            expected_keys: List of keys that should be present in the JSON response
            status_code: Expected HTTP status code (default: 200)
        """
        assert response.status_code == status_code, (
            f"Expected status code {status_code}, got {response.status_code}. "
            f"Response data: {response.get_data(as_text=True)}"
        )

        # Check if response is JSON
        assert response.is_json, (
            f"Response is not JSON: {response.get_data(as_text=True)}"
        )

        data = response.get_json()
        assert data is not None, "Response JSON data is None"

        # Check for expected keys if provided
        if expected_keys:
            for key in expected_keys:
                assert key in data, (
                    f"Expected key '{key}' not found in response. Available keys: {list(data.keys())}"
                )

        return data


@pytest.fixture(scope="function")
def helpers():
    """Provide test helper methods."""
    return TestHelpers()
