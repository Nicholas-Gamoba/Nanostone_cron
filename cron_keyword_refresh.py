#!/usr/bin/env python3
import os
import sys
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


def main():
    if not NANOSTONE_SERP_API_URL:
        logger.error("NANOSTONE_SERP_API_URL is not set")
        sys.exit(1)
    if not CRON_SECRET:
        logger.error("CRON_SECRET is not set")
        sys.exit(1)
    logger.info("=== SERP Refresh Started ===")
    start_time = datetime.now()

    try:
        response = requests.post(
            f"{NANOSTONE_SERP_API_URL}/api/serp/refresh-all",
            headers={
                "Content-Type": "application/json",
                "X-Cron-Secret": CRON_SECRET,
            },
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        logger.info(f"Refresh triggered: {data}")

    except Exception as e:
        logger.error(f"Failed to trigger refresh: {e}")
        sys.exit(1)

    duration = datetime.now() - start_time
    logger.info(f"=== Complete in {duration} ===")


if __name__ == "__main__":
    main()
