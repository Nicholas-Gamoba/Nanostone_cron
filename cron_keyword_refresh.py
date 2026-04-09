#!/usr/bin/env python3
import os
import sys
import asyncio
import logging
import httpx
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

SERP_API_URL = os.getenv("SERP_API_URL")  # https://nanostoneserp-api.onrender.com
VERCEL_URL = os.getenv("VERCEL_URL")  # https://your-app.vercel.app
CRON_SECRET = os.getenv("CRON_SECRET")


async def fetch_keywords() -> list:
    """Fetch all keywords from Vercel /api/keywords"""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{VERCEL_URL}/api/keywords",
            headers={"X-Cron-Secret": CRON_SECRET},
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json().get("keywords", [])


async def trigger_serp_search(keyword_id: int, keyword: str) -> bool:
    """
    Calls POST /api/serp/search on the serp_api.
    The serp_api runs the DataForSEO fetch in the background
    and fires the webhook to Vercel when done.
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{SERP_API_URL}/api/serp/search",
                json={
                    "keyword_id": keyword_id,
                    "keyword": keyword,
                    "country": "Denmark",
                    "language": "Danish",
                },
                headers={"X-Callback-URL": VERCEL_URL},
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()
            logger.info(f"Triggered '{keyword}' — job_id: {data.get('job_id')}")
            return True

    except Exception as e:
        logger.error(f"Failed to trigger search for '{keyword}': {e}")
        return False


async def main():
    logger.info("=== SERP Weekly Refresh Started ===")
    start_time = datetime.now()

    try:
        keywords = await fetch_keywords()
        logger.info(f"Found {len(keywords)} keywords to refresh")
    except Exception as e:
        logger.error(f"Failed to fetch keywords: {e}")
        sys.exit(1)

    if not keywords:
        logger.info("No keywords found, exiting")
        return

    succeeded = 0
    failed = 0

    for kw in keywords:
        success = await trigger_serp_search(kw["id"], kw["keyword"])
        if success:
            succeeded += 1
        else:
            failed += 1
        # Small gap between triggers — serp_api handles the heavy lifting async
        await asyncio.sleep(5)

    duration = datetime.now() - start_time
    logger.info(
        f"=== Complete — {succeeded} triggered, {failed} failed in {duration} ==="
    )

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
