import asyncio
import logging
from datetime import datetime, timezone as tz

from src.store.database import Database
from src.brain.facebook_brain import discover_facebook_keywords
from src.brain.decision_engine import select_todays_content
from src.creator.image_generator import generate_image
from src.creator.metadata_generator import generate_metadata
from src.worker.facebook_client import FacebookClient
from src.worker.scheduler import distribute_posting_times, get_daily_limits
from src.models import FacebookPost

logger = logging.getLogger(__name__)

async def run_facebook_daily_cycle(db: Database, config: dict, force: bool = False) -> None:
    """
    The complete daily agent cycle for Facebook:
    1. Research (keywords relevant to Facebook)
    2. Decide content mix
    3. Generate images + metadata
    4. Post to Facebook
    """
    logger.info("=== Starting Facebook daily cycle ===")

    niche = config.get("niche", {})
    seed_keywords = niche.get("seed_keywords", [])

    fb_client = FacebookClient(config, db=db)

    try:
        logged_in = await fb_client.login()
        if not logged_in:
            logger.error("Facebook login failed. Skipping cycle.")
            return

        logger.info("Step 1: Facebook Research")
        keywords = await discover_facebook_keywords(seed_keywords, db, config)
        logger.info(f"Found {len(keywords)} Facebook-relevant keywords")

        logger.info("Step 2: Decision")
        created_date_str = config.get("account", {}).get("created_date", "2026-04-24")
        created_date = datetime.strptime(created_date_str, "%Y-%m-%d")
        limits = get_daily_limits(created_date)

        # Re-use content selection logic
        briefs = select_todays_content(keywords, [], limits.max_pins, 100) # 100% SEO for now

        if not briefs:
            logger.warning("No content briefs generated. Skipping cycle.")
            return

        logger.info(f"Step 3: Generate and Post ({len(briefs)} posts)")

        for i, brief in enumerate(briefs):
            try:
                logger.info(f"Processing brief {i+1}/{len(briefs)}: {brief.target_keyword}")

                # Generate image
                image_path, image_hash = await generate_image(brief, config)

                # Generate metadata (can reuse PinMetadata logic for text)
                metadata = await generate_metadata(brief, config)

                post_text = f"{metadata.title}\n\n{metadata.description}"
                if metadata.hashtags:
                    post_text += "\n\n" + " ".join(metadata.hashtags)

                # Post to Facebook
                success = await fb_client.post_content(post_text, image_path)

                if success:
                    logger.info(f"Successfully posted to Facebook: {brief.target_keyword}")
                    # Log to DB (could expand Database class later for Facebook specific tables)
                    db.log_action("facebook_post", {"keyword": brief.target_keyword, "status": "success"})
                else:
                    logger.warning(f"Failed to post to Facebook: {brief.target_keyword}")

                await asyncio.sleep(60) # Wait between posts

            except Exception as e:
                logger.error(f"Error processing Facebook brief '{brief.target_keyword}': {e}")
                continue

    finally:
        await fb_client.close()

    logger.info("=== Facebook daily cycle complete ===")
