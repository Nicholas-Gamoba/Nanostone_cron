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
MAX_REFRESH_WAIT_SECONDS = int(os.getenv("MAX_REFRESH_WAIT_SECONDS", 7200))  # 2h ceiling
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 60))


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


def get_job_status(job_id: str) -> dict:
    url = f"{NANOSTONE_SERP_API_URL}/api/serp/jobs/{job_id}"
    r = requests.get(
        url,
        headers={"X-Cron-Secret": CRON_SECRET},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def wait_for_job(job_id: str) -> bool:
    """Poll until the job completes or max wait is reached.
    Returns True if the job completed cleanly, False on timeout."""
    waited = 0
    logger.info(
        f"Polling job {job_id} every {POLL_INTERVAL_SECONDS}s "
        f"(max wait {MAX_REFRESH_WAIT_SECONDS}s)..."
    )
    last_done = -1
    stalled_polls = 0

    while waited < MAX_REFRESH_WAIT_SECONDS:
        time.sleep(POLL_INTERVAL_SECONDS)
        waited += POLL_INTERVAL_SECONDS

        try:
            status_data = get_job_status(job_id)
        except Exception as e:
            logger.warning(f"  [{waited}s] poll failed: {e} — continuing")
            continue

        status = status_data.get("status")
        done = status_data.get("keywords_done", 0)
        total = status_data.get("keywords_total", 0)
        logger.info(f"  [{waited}s] {done}/{total} — status={status}")

        if status == "complete":
            logger.info(f"Job complete after {waited}s ({done}/{total})")
            return True

        # Track stall — if processed_count hasn't moved for 15 minutes,
        # the server-side watchdog should eventually time it out, but
        # log it here so we notice in cron output.
        if done == last_done:
            stalled_polls += 1
            if stalled_polls * POLL_INTERVAL_SECONDS >= 900:
                logger.warning(
                    f"  No progress for {stalled_polls * POLL_INTERVAL_SECONDS}s "
                    f"— server watchdog should take over"
                )
        else:
            stalled_polls = 0
            last_done = done

    logger.warning(
        f"Max wait {MAX_REFRESH_WAIT_SECONDS}s reached — "
        f"proceeding to regression check anyway"
    )
    return False


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

    # 2. Wait for job to complete — poll if we have a job_id, else fall back to sleep
    job_id = data.get("job_id")
    if job_id:
        wait_for_job(job_id)
    else:
        logger.warning(
            f"refresh-all did not return job_id — "
            f"falling back to fixed {REFRESH_SETTLE_SECONDS}s sleep"
        )
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