## Doppler Indexer

The Doppler Indexer provides structured, queryable data for tokens created through [Doppler](https://doppler.lol).

## Status

We are actively rebuilding our indexing infrastructure in Rust.

This repository is a work in progress and not yet ready for production use.

## Production Version

For the current production indexer, see [doppler-indexer](https://github.com/whetstoneresearch/doppler-indexer).

For API usage and public endpoints, refer to the [Doppler Documentation](https://docs.doppler.lol/reference/overview).

## Database Connections

- Local Docker Postgres can use a plaintext `DATABASE_URL`, typically with `sslmode=disable`.
- Managed PostgreSQL deployments should use a TLS-enabled `DATABASE_URL`, such as DigitalOcean's default `sslmode=require` connection string.

## Dev Deployment

The intended dev deployment path is:

1. Build a Docker image from this repo.
2. Push the image to GHCR.
3. SSH into the dev server.
4. Pull the new image and restart the service with Docker Compose.

Deployment assets live under `deploy/dev`.

- `deploy/dev/compose.yaml`: runtime definition for the dev server
- `deploy/dev/.env.example`: required runtime env vars
- `deploy/dev/redeploy.sh`: idempotent remote redeploy script

The server must provide its own `deploy/dev/.env` equivalent at:

`/opt/doppler-indexer/.env`

The GitHub Actions deploy workflow expects these secrets:

- `DEV_SERVER_HOST`
- `DEV_SERVER_USER`
- `DEV_SERVER_SSH_KEY`

The workflow builds on merge to `main` and pushes images to `ghcr.io/${owner}/doppler-indexer-rs`.

## Questions

Join the [Doppler Telegram community](https://doppler.lol/telegram) or open an issue in this repository to get in touch.
