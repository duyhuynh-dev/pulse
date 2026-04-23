import logging
import os

from fastapi import FastAPI
import inngest.fast_api

from app.jobs.workflows import daily_supply_ingestion, inngest_client, reddit_profile_sync, weekly_recommendations

app = FastAPI(title="Pulse Worker", version="0.1.0")
logger = logging.getLogger("pulse-worker")

if os.getenv("INNGEST_SIGNING_KEY"):
    inngest.fast_api.serve(
        app,
        inngest_client,
        [daily_supply_ingestion, weekly_recommendations, reddit_profile_sync],
    )
else:
    logger.warning("INNGEST_SIGNING_KEY is missing; starting worker without Inngest serve routes.")


@app.get("/healthz")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}
