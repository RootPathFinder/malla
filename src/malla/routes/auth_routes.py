"""
Authentication routes for login, logout, and user management.
"""

import logging
from functools import wraps

from flask import (
    Blueprint,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_login import (
    current_user,
    login_required,
    login_user,
    logout_user,
)

from ..models.user import UserRole
from ..services.auth_service import AuthService

logger = logging.getLogger(__name__)

auth_bp = Blueprint("auth", __name__)


def role_required(role: UserRole):
    """Decorator to require a minimum role level for access."""

    def decorator(f):
        @wraps(f)
        @login_required
        def decorated_function(*args, **kwargs):
            if not current_user.has_role(role):
                if request.is_json:
                    return jsonify(
                        {
                            "error": "Insufficient permissions",
                            "required_role": role.value,
                            "your_role": current_user.role.value,
                        }
                    ), 403
                flash("You don't have permission to access this page.", "danger")
                return redirect(url_for("main.dashboard"))
            return f(*args, **kwargs)

        return decorated_function

    return decorator


def admin_required(f):
    """Decorator to require admin role."""
    return role_required(UserRole.ADMIN)(f)


def operator_required(f):
    """Decorator to require operator or higher role."""
    return role_required(UserRole.OPERATOR)(f)


# ============================================================================
# Page Routes
# ============================================================================


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    """Login page and form handler."""
    if current_user.is_authenticated:
        return redirect(url_for("main.dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        remember = request.form.get("remember", False)

        if not username or not password:
            flash("Please enter both username and password.", "warning")
            return render_template("login.html")

        user = AuthService.authenticate(username, password)
        if user:
            login_user(user, remember=bool(remember))
            flash(f"Welcome back, {user.username}!", "success")

            # Redirect to next page or home
            next_page = request.args.get("next")
            if next_page and next_page.startswith("/"):
                return redirect(next_page)
            return redirect(url_for("main.dashboard"))

        flash("Invalid username or password.", "danger")
        logger.warning(f"Failed login attempt for username: {username}")

    return render_template("login.html")


@auth_bp.route("/logout")
@login_required
def logout():
    """Log out the current user."""
    username = current_user.username
    logout_user()
    flash("You have been logged out.", "info")
    logger.info(f"User '{username}' logged out")
    return redirect(url_for("main.dashboard"))


@auth_bp.route("/profile")
@login_required
def profile():
    """User profile page."""
    return render_template("profile.html", user=current_user)


@auth_bp.route("/profile/password", methods=["POST"])
@login_required
def change_password():
    """Change current user's password."""
    current_password = request.form.get("current_password", "")
    new_password = request.form.get("new_password", "")
    confirm_password = request.form.get("confirm_password", "")

    # Validate current password
    if not AuthService.verify_password(current_password, current_user.password_hash):
        flash("Current password is incorrect.", "danger")
        return redirect(url_for("auth.profile"))

    # Validate new password
    if len(new_password) < 8:
        flash("New password must be at least 8 characters long.", "warning")
        return redirect(url_for("auth.profile"))

    if new_password != confirm_password:
        flash("New passwords do not match.", "warning")
        return redirect(url_for("auth.profile"))

    # Update password
    if AuthService.update_password(current_user.id, new_password):
        flash("Password updated successfully.", "success")
        logger.info(f"User '{current_user.username}' changed their password")
    else:
        flash("Failed to update password.", "danger")

    return redirect(url_for("auth.profile"))


@auth_bp.route("/api/preferences", methods=["GET"])
@login_required
def get_preferences():
    """Get current user's preferences."""
    prefs = AuthService.get_user_preferences(current_user.id)
    return jsonify({"preferences": prefs})


@auth_bp.route("/api/preferences", methods=["POST"])
@login_required
def set_preferences():
    """Set current user's preferences (replaces all)."""
    try:
        data = request.get_json()
        if not data or "preferences" not in data:
            return jsonify({"error": "Missing preferences data"}), 400

        if AuthService.set_user_preferences(current_user.id, data["preferences"]):
            return jsonify({"success": True, "preferences": data["preferences"]})
        return jsonify({"error": "Failed to save preferences"}), 500
    except Exception as e:
        logger.error(f"Error setting preferences: {e}")
        return jsonify({"error": str(e)}), 500


@auth_bp.route("/api/preferences/<key>", methods=["PUT"])
@login_required
def update_preference(key: str):
    """Update a single preference."""
    try:
        data = request.get_json()
        if data is None or "value" not in data:
            return jsonify({"error": "Missing value"}), 400

        if AuthService.update_user_preference(current_user.id, key, data["value"]):
            return jsonify({"success": True, "key": key, "value": data["value"]})
        return jsonify({"error": "Failed to save preference"}), 500
    except Exception as e:
        logger.error(f"Error updating preference {key}: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# User Management Routes (Admin Only)
# ============================================================================


@auth_bp.route("/users")
@admin_required
def user_list():
    """List all users (admin only)."""
    users = AuthService.list_users(include_inactive=True)
    return render_template("users.html", users=users)


@auth_bp.route("/users/create", methods=["GET", "POST"])
@admin_required
def create_user():
    """Create a new user (admin only)."""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        role = request.form.get("role", "viewer")

        if not username or not password:
            flash("Username and password are required.", "warning")
            return render_template("user_create.html", roles=UserRole)

        if len(password) < 8:
            flash("Password must be at least 8 characters long.", "warning")
            return render_template("user_create.html", roles=UserRole)

        try:
            user_role = UserRole(role)
        except ValueError:
            user_role = UserRole.VIEWER

        user = AuthService.create_user(username, password, user_role)
        if user:
            flash(f"User '{username}' created successfully.", "success")
            logger.info(
                f"Admin '{current_user.username}' created user '{username}' with role '{role}'"
            )
            return redirect(url_for("auth.user_list"))

        flash("Failed to create user. Username may already exist.", "danger")

    return render_template("user_create.html", roles=UserRole)


@auth_bp.route("/users/<int:user_id>/edit", methods=["GET", "POST"])
@admin_required
def edit_user(user_id: int):
    """Edit a user (admin only)."""
    user = AuthService.get_user(user_id)
    if not user:
        flash("User not found.", "danger")
        return redirect(url_for("auth.user_list"))

    if request.method == "POST":
        action = request.form.get("action")

        if action == "update_role":
            new_role = request.form.get("role", "viewer")
            try:
                role = UserRole(new_role)
                # Prevent removing the last admin
                if (
                    user.role == UserRole.ADMIN
                    and role != UserRole.ADMIN
                    and AuthService.count_admins() <= 1
                ):
                    flash("Cannot demote the last admin user.", "warning")
                else:
                    AuthService.update_role(user_id, role)
                    flash(f"Role updated to '{role.value}'.", "success")
                    logger.info(
                        f"Admin '{current_user.username}' changed role of '{user.username}' to '{role.value}'"
                    )
            except ValueError:
                flash("Invalid role.", "danger")

        elif action == "reset_password":
            new_password = request.form.get("new_password", "")
            if len(new_password) >= 8:
                AuthService.update_password(user_id, new_password)
                flash("Password reset successfully.", "success")
                logger.info(
                    f"Admin '{current_user.username}' reset password for '{user.username}'"
                )
            else:
                flash("Password must be at least 8 characters.", "warning")

        elif action == "deactivate":
            # Prevent self-deactivation
            if user_id == current_user.id:
                flash("You cannot deactivate your own account.", "warning")
            # Prevent deactivating the last admin
            elif user.role == UserRole.ADMIN and AuthService.count_admins() <= 1:
                flash("Cannot deactivate the last admin user.", "warning")
            else:
                AuthService.deactivate_user(user_id)
                flash(f"User '{user.username}' deactivated.", "success")
                logger.info(
                    f"Admin '{current_user.username}' deactivated user '{user.username}'"
                )
                return redirect(url_for("auth.user_list"))

        # Refresh user data
        user = AuthService.get_user(user_id)

    return render_template("user_edit.html", user=user, roles=UserRole)


# ============================================================================
# API Routes
# ============================================================================


@auth_bp.route("/api/auth/status")
def api_auth_status():
    """Get current authentication status."""
    if current_user.is_authenticated:
        return jsonify(
            {
                "authenticated": True,
                "user": current_user.to_dict(),
            }
        )
    return jsonify(
        {
            "authenticated": False,
            "user": None,
        }
    )


@auth_bp.route("/api/auth/login", methods=["POST"])
def api_login():
    """API login endpoint."""
    data = request.get_json() or {}
    username = data.get("username", "")
    password = data.get("password", "")
    remember = data.get("remember", False)

    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400

    user = AuthService.authenticate(username, password)
    if user:
        login_user(user, remember=remember)
        return jsonify(
            {
                "success": True,
                "user": user.to_dict(),
            }
        )

    return jsonify({"error": "Invalid username or password"}), 401


@auth_bp.route("/api/auth/logout", methods=["POST"])
@login_required
def api_logout():
    """API logout endpoint."""
    logout_user()
    return jsonify({"success": True})


@auth_bp.route("/api/users")
@admin_required
def api_list_users():
    """API endpoint to list users (admin only)."""
    users = AuthService.list_users(include_inactive=True)
    return jsonify(
        {
            "users": [u.to_dict() for u in users],
        }
    )


@auth_bp.route("/api/users", methods=["POST"])
@admin_required
def api_create_user():
    """API endpoint to create a user (admin only)."""
    data = request.get_json() or {}
    username = data.get("username", "").strip()
    password = data.get("password", "")
    role = data.get("role", "viewer")

    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400

    if len(password) < 8:
        return jsonify({"error": "Password must be at least 8 characters"}), 400

    try:
        user_role = UserRole(role)
    except ValueError:
        return jsonify({"error": f"Invalid role: {role}"}), 400

    user = AuthService.create_user(username, password, user_role)
    if user:
        logger.info(
            f"Admin '{current_user.username}' created user '{username}' via API"
        )
        return jsonify({"success": True, "user": user.to_dict()}), 201

    return jsonify({"error": "Failed to create user. Username may already exist."}), 400
