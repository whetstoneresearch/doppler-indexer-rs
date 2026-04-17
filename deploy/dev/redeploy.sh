#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/doppler-indexer}"
COMPOSE_FILE="$APP_DIR/compose.yaml"
SERVICE_NAME="doppler-indexer-dev"

if [[ -z "${IMAGE_REF:-}" ]]; then
  echo "IMAGE_REF is required" >&2
  exit 1
fi

mkdir -p "$APP_DIR/data"

if [[ ! -f "$APP_DIR/.env" ]]; then
  echo "Missing $APP_DIR/.env" >&2
  exit 1
fi

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Missing $COMPOSE_FILE" >&2
  exit 1
fi

if [[ -n "${GHCR_TOKEN:-}" ]]; then
  if [[ -z "${GHCR_USERNAME:-}" ]]; then
    echo "GHCR_USERNAME is required when GHCR_TOKEN is set" >&2
    exit 1
  fi
  printf '%s' "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin
fi

IMAGE_REF="$IMAGE_REF" docker compose -f "$COMPOSE_FILE" pull
IMAGE_REF="$IMAGE_REF" docker compose -f "$COMPOSE_FILE" up -d --remove-orphans

sleep 10

status="$(docker inspect --format '{{.State.Status}}' "$SERVICE_NAME" 2>/dev/null || true)"
if [[ "$status" != "running" ]]; then
  docker compose -f "$COMPOSE_FILE" logs --tail 200 >&2 || true
  echo "Container $SERVICE_NAME is not running (status=$status)" >&2
  exit 1
fi

docker compose -f "$COMPOSE_FILE" ps
