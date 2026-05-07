import asyncio
import logging
import random
from datetime import datetime

from src.store.database import Database
from src.brain.instagram_brain import discover_instagram_keywords
from src.brain.decision_engine import select_todays_content
from src.creator.image_generator import generate_image
from src.creator.metadata_generator import generate_metadata
from src.worker.instagram_client import InstagramClient
from src.worker.scheduler import distribute_posting_times, get_daily_limits

logger = logging.getLogger(__name__)

async def run_instagram_daily_cycle(db: Database, config: dict, force: bool = False) -> None:
    """
    The complete daily agent cycle for Instagram:
    1. Research (hashtags)
    2. Decide content mix
    3. Generate images + metadata
    4. Post to Instagram
    """
    logger.info("=== Starting Instagram daily cycle ===")

    niche = config.get("niche", {})
    seed_keywords = niche.get("seed_keywords", [])

    insta_client = InstagramClient(config, db=db)

    try:
        logged_in = await insta_client.login()
        if not logged_in:
            logger.error("Instagram login failed. Skipping cycle.")
            return

        logger.info("Step 1: Instagram Research")
        keywords = await discover_instagram_keywords(seed_keywords, db, config)
        logger.info(f"Found {len(keywords)} Instagram-relevant keywords/hashtags")

        logger.info("Step 2: Decision")
        created_date_str = config.get("account", {}).get("created_date", "2026-04-24")
        created_date = datetime.strptime(created_date_str, "%Y-%m-%d")
        limits = get_daily_limits(created_date)

        # Instagram limits are usually stricter
        max_posts = min(limits.max_pins, 5)
        briefs = select_todays_content(keywords, [], max_posts, 100)

        if not briefs:
            logger.warning("No content briefs generated for Instagram. Skipping cycle.")
            return

        logger.info(f"Step 3: Generate and Post ({len(briefs)} posts)")

        for i, brief in enumerate(briefs):
            try:
                logger.info(f"Processing Instagram brief {i+1}/{len(briefs)}: {brief.target_keyword}")

                image_path, _ = await generate_image(brief, config)
                metadata = await generate_metadata(brief, config)

                caption = f"{metadata.title}\n\n{metadata.description}"
                if metadata.hashtags:
                    caption += "\n\n" + " ".join([f"#{h.replace(' ', '')}" for h in metadata.hashtags])

                success = await insta_client.post_content(image_path, caption)

                if success:
                    logger.info(f"Successfully posted to Instagram: {brief.target_keyword}")
                    db.log_action("instagram_post", {"keyword": brief.target_keyword, "status": "success"})
                else:
                    logger.warning(f"Failed to post to Instagram: {brief.target_keyword}")

                # Wait between posts (longer for Instagram)
                await asyncio.sleep(random.uniform(300, 600))

            except Exception as e:
                logger.error(f"Error processing Instagram brief '{brief.target_keyword}': {e}")
                continue

    finally:
        await insta_client.close()

    logger.info("=== Instagram daily cycle complete ===")
