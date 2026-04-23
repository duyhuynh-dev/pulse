# Pulse EC2 Deployment

This deploy target runs the entire Pulse stack on a single EC2 instance with Docker Compose:

- `web` on `https://$APP_DOMAIN`
- `api` on `https://$API_DOMAIN`
- `worker` on `https://$WORKER_DOMAIN`
- `caddy` handling TLS and reverse proxying

The stack assumes you are keeping:

- `Supabase` for auth and Postgres
- `Resend` for email
- `Inngest Cloud` for orchestration

## 1. Prepare the EC2 host

Use an Ubuntu 24.04 or similar Linux host with:

- Docker Engine
- Docker Compose plugin
- ports `80` and `443` open in the security group
- your DNS records pointing to the instance:
  - `$APP_DOMAIN`
  - `$API_DOMAIN`
  - `$WORKER_DOMAIN`

## 2. Create the EC2 env file

Copy the example file and fill in real values:

```bash
cp deploy/ec2/.env.ec2.example deploy/ec2/.env.ec2
```

Important production values:

- `DATABASE_URL`
  Usually your Supabase Postgres connection string with `asyncpg`
- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY`
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `OAUTH_STATE_SECRET`
- `INTERNAL_INGEST_SECRET`
- `RESEND_API_KEY`
- `DIGEST_FROM_EMAIL`
- `TICKETMASTER_API_KEY` if you want live Ticketmaster ingestion
- `GEMINI_API_KEY` if you want live AI-backed worker tasks
- `INNGEST_SIGNING_KEY` so the worker exposes the Inngest serve route

## 3. Build and boot the stack

From the repo root:

```bash
docker compose --env-file deploy/ec2/.env.ec2 -f deploy/ec2/docker-compose.yml up --build -d
```

Useful follow-up commands:

```bash
docker compose --env-file deploy/ec2/.env.ec2 -f deploy/ec2/docker-compose.yml ps
docker compose --env-file deploy/ec2/.env.ec2 -f deploy/ec2/docker-compose.yml logs -f api
docker compose --env-file deploy/ec2/.env.ec2 -f deploy/ec2/docker-compose.yml logs -f worker
docker compose --env-file deploy/ec2/.env.ec2 -f deploy/ec2/docker-compose.yml logs -f web
```

## 4. Health checks

After boot, verify:

```bash
curl https://$API_DOMAIN/healthz
curl https://$WORKER_DOMAIN/healthz
open https://$APP_DOMAIN
```

## 5. Inngest Cloud sync

The Python worker exposes the default Inngest FastAPI serve route at:

```text
https://$WORKER_DOMAIN/api/inngest
```

In Inngest Cloud:

- point your app sync / serve URL at `https://$WORKER_DOMAIN/api/inngest`
- make sure the signing key in Inngest matches `INNGEST_SIGNING_KEY`

## 6. Smoke tests

### Supply sync

```bash
curl -X POST "https://$WORKER_DOMAIN/v1/supply/sync" \
  -H "x-pulse-ingest-secret: $INTERNAL_INGEST_SECRET"
```

### Scheduled digest dry run

```bash
curl -X POST "https://$WORKER_DOMAIN/v1/digests/run-scheduled?dry_run=true&now_override=2026-04-21T13:05:00%2B00:00" \
  -H "x-pulse-ingest-secret: $INTERNAL_INGEST_SECRET"
```

### Scheduled digest real send

Remove `dry_run=true` once:

- `RESEND_API_KEY` is valid
- `DIGEST_FROM_EMAIL` uses a verified sender
- at least one signed-in user has digest preferences enabled

## 7. Updating the stack

Pull new code, then rebuild:

```bash
git pull
docker compose --env-file deploy/ec2/.env.ec2 -f deploy/ec2/docker-compose.yml up --build -d
```

## Notes

- `web` is fully on EC2 in this setup; `Vercel` is not used.
- The `api` talks to the `worker` over the internal Docker network with `http://worker:8001`.
- The `worker` talks back to the `api` internally with `http://api:8000`.
- Caddy terminates TLS automatically once your domains resolve to the EC2 instance.
