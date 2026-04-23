# Pulse MVP

Pulse is a map-first, personalized event discovery MVP for New York City. The product combines:

- `Next.js` for the consumer web experience
- `FastAPI` for APIs and backend integrations
- `Inngest + PydanticAI` for controlled agentic workflows
- `MapLibre + OpenStreetMap` for the live map stack

## Workspace

- `apps/web`: Next.js application
- `services/api`: FastAPI application and Alembic migrations
- `services/worker`: Inngest jobs, AI tasks, and ingestion/ranking logic

## Local setup

1. Install `uv`.
2. If `pnpm` is not already installed, enable it with `corepack`.
3. Copy `.env.example` values into service-specific `.env` files.
4. Install web dependencies from the repo root with `pnpm install`.
5. Install Python dependencies with:

```bash
corepack prepare pnpm@10.10.0 --activate
pnpm install
cd services/api && uv sync
cd ../worker && uv sync
```

## Run locally

```bash
pnpm dev
```

Run the Python services in separate terminals:

```bash
cd services/api && uv run uvicorn app.main:app --reload --port 8000
cd services/worker && uv run python -m app.main
```

## Validation

```bash
pnpm --filter @pulse/web build
cd services/api && .venv/bin/pytest
cd ../worker && .venv/bin/pytest
```

## Supply ingestion

- `services/worker` now runs a daily supply sync that combines curated NYC venue candidates with `Ticketmaster` results when `TICKETMASTER_API_KEY` is set.
- The worker posts normalized candidates into `services/api` through `/v1/internal/ingest/candidates`.
- Set the same `INTERNAL_INGEST_SECRET` in both services if you want the ingest route locked down outside local dev.
- The map can be manually regenerated from the latest catalog with `POST /v1/recommendations/refresh` or the in-app `Refresh picks` button.

## Weekly digests

- Manual preview sends use `POST /v1/digest/send-preview` and require `RESEND_API_KEY`.
- Scheduled delivery runs through the worker and calls `POST /v1/internal/digests/send-weekly` on the API.
- The worker cron checks every 15 minutes and the API decides whether each user is actually due based on:
  - `weekly_digest_enabled`
  - `digest_day`
  - `digest_time_local`
  - user `timezone`

## Scheduled digest smoke test

Use the worker endpoint to test the scheduled flow immediately instead of waiting for cron:

```bash
curl -X POST "http://127.0.0.1:8001/v1/digests/run-scheduled?dry_run=true&now_override=2026-04-21T13:05:00%2B00:00" \
  -H "x-pulse-ingest-secret: ${INTERNAL_INGEST_SECRET}"
```

Why that timestamp:

- `2026-04-21T13:05:00+00:00` is Tuesday `09:05` in `America/New_York`
- that falls inside the current 15-minute delivery window for a user whose digest is due at `Tuesday 09:00`

What to expect:

- `dry_run=true` returns who would receive the digest without actually sending email
- removing `dry_run=true` sends the real scheduled digest, assuming `RESEND_API_KEY` is configured
- the worker endpoint requires the same `INTERNAL_INGEST_SECRET` used for supply sync

## EC2 deployment

- A full single-instance `EC2 + Docker Compose + Caddy` deployment scaffold now lives under [deploy/ec2/README.md](/Users/duyhuynh/Desktop/project/ig-location-suggestion-app/deploy/ec2/README.md).
- That stack runs `web`, `api`, and `worker` together on one box while keeping `Supabase`, `Resend`, and `Inngest Cloud` as managed services.
- The production compose file is at [deploy/ec2/docker-compose.yml](/Users/duyhuynh/Desktop/project/ig-location-suggestion-app/deploy/ec2/docker-compose.yml).

## Notes

- The implementation uses coarse user anchors by default. Exact browser location is session-only.
- Gemini is wrapped behind a provider abstraction to make a later Anthropic migration low-risk.
- Travel time in MVP is heuristic, not route API based.
- Live auth now expects `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY`, `SUPABASE_URL`, `SUPABASE_ANON_KEY`, and `OAUTH_STATE_SECRET`.
- Live Ticketmaster ingestion is optional and only activates when `TICKETMASTER_API_KEY` is present.
