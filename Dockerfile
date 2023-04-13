#
# ./stac2resto.py /data/catalog.json http://admin:admin@localhost:5252 10
#
# Build the container with latest from image `--pull` flag
#
#   docker build --pull -t jjrom/stac2resto -f ./Dockerfile .
#
# Run ingest of 10 items for collection high_tide_comp_20p locally without ssl
#
#   docker run -t --rm -e DEVEL=true -e MAX_FEATURE=10 jjrom/stac2resto:latest
#
FROM python:3.6.10-alpine3.11

ENV DEFAULT_TIMEOUT=25 \
    # Set to run in local mode
    DEVEL=false \
    # resto auth token for id=100
    RESTO_ADMIN_AUTH_TOKEN=eyJzdWIiOiIxMDAiLCJpYXQiOjE2MzQ5MTg3NDYsImV4cCI6MjQ5ODkxODc0Nn0.jOYxKv5FFK4VD3N9PN5ZSIqAHSwz1gAkC3o-LrDGHiM \
    # Set ssl verify option for requests module
    SSL_VERIFY=false \
    # Prevents Python from writing pyc files to disc
    PYTHONDONTWRITEBYTECODE=1 \
    # Prevents Python from buffering stdout and stder
    # Fixes emtpy logs issue `docker logs -f ID` or `kubectl -n processing logs -f ID`
    PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app

# Create a group
RUN addgroup -g 1000 --system ingestor \
    # Create user
    && adduser \
        --disabled-password \
        --gecos "" \
        --home "$(pwd)" \
        --ingroup ingestor \
        --no-create-home \
        --uid 1000 \
        --system \
        ingestor

RUN apk add --verbose --progress --no-cache \
  ca-certificates \
  bash \
  && pip3 install environs requests tqdm configparser urllib3 colorlog validators


ADD ./app/stac2resto /app

RUN chown -Rv ingestor: /app

USER ingestor

ENTRYPOINT [ "python", "/app/stac2resto" ]
