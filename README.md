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

## Container Deployment

This repository publishes a container image suitable for self-hosting.

The intended container flow is:

1. Build or pull the image.
2. Provide runtime environment variables.
3. Mount a persistent `data/` directory.
4. Run the container with Docker Compose or your preferred orchestrator.

Generic deployment examples live under `deploy`.

- `deploy/compose.example.yaml`: example Compose service definition
- `deploy/.env.example`: runtime env var template

On pushes to `main`, the `Publish Image` workflow builds and pushes the image to:

- `ghcr.io/${owner}/doppler-indexer-rs:<git-sha>`
- `ghcr.io/${owner}/doppler-indexer-rs:main`

## Questions

Join the [Doppler Telegram community](https://doppler.lol/telegram) or open an issue in this repository to get in touch.
