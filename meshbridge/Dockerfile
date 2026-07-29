FROM python:3.12-slim

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
COPY config.sample.yaml ./config.sample.yaml

RUN pip install --no-cache-dir -e ".[meshcore]"

ENV MESHBRIDGE_CONFIG=/config/config.yaml
VOLUME ["/config"]

CMD ["meshbridge", "-c", "/config/config.yaml"]
