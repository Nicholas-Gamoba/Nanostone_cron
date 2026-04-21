#!/usr/bin/env python3
import os
import sys
import time
import logging
import requests
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

NANOSTONE_SERP_API_URL = os.getenv("NANOSTONE_SERP_API_URL")
CRON_SECRET = os.getenv("CRON_SECRET")
REFRESH_SETTLE_SECONDS = int(os.getenv("REFRESH_SETTLE_SECONDS", 600))


def call(endpoint: str, label: str) -> dict:
    url = f"{NANOSTONE_SERP_API_URL}/api/serp/{endpoint}"
    logger.info(f"→ {label}: POST {url}")
    r = requests.post(
        url,
        headers={"Content-Type": "application/json", "X-Cron-Secret": CRON_SECRET},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    logger.info(f"← {label}: {data}")
    return data


def main():
    if not NANOSTONE_SERP_API_URL:
        logger.error("NANOSTONE_SERP_API_URL is not set")
        sys.exit(1)
    if not CRON_SECRET:
        logger.error("CRON_SECRET is not set")
        sys.exit(1)

    logger.info("=== SERP Refresh Started ===")
    start_time = datetime.now()

    # 1. Kick off bulk refresh (returns immediately, runs async via postbacks)
    try:
        data = call("refresh-all", "Bulk refresh")
        logger.info(f"Refresh triggered: {data}")
    except Exception as e:
        logger.error(f"Failed to trigger refresh: {e}")
        sys.exit(1)

    # 2. Wait for postbacks to settle before comparing
    logger.info(f"Waiting {REFRESH_SETTLE_SECONDS}s for postbacks to settle...")
    time.sleep(REFRESH_SETTLE_SECONDS)

    # 3. Compare latest two runs — queues regressions and fires 3h recheck task
    try:
        result = call("check-regressions", "Regression check")
        lost = result.get("lost", [])
        moved = result.get("moved_5", [])
        logger.info(f"Regressions: {len(lost)} lost, {len(moved)} moved >5 positions")
        if lost:
            logger.info(f"  Lost:  {lost}")
        if moved:
            logger.info(f"  Moved: {moved}")
    except Exception as e:
        logger.error(f"Regression check failed: {e}")
        # Non-fatal — refresh already ran successfully, don't fail the whole cron
        sys.exit(0)

    duration = datetime.now() - start_time
    logger.info(f"=== Complete in {duration} ===")


if __name__ == "__main__":
    main()
