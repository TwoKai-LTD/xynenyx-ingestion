"""Main entrypoint for ingestion service."""
import asyncio
import argparse
import logging
import sys
from app.config import settings
from app.workers.ingestion_worker import IngestionWorker
from app.workers.processing_worker import ProcessingWorker
from app.workers.features_worker import FeaturesWorker

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
)

logger = logging.getLogger(__name__)


async def run_worker(mode: str) -> None:
    """Run the specified worker mode."""
    logger.info(f"Starting worker in {mode} mode")

    try:
        if mode == "ingestion":
            worker = IngestionWorker()
            result = await worker.run()
        elif mode == "processing":
            worker = ProcessingWorker()
            result = await worker.run()
        elif mode == "features":
            worker = FeaturesWorker()
            result = await worker.run()
        else:
            logger.error(f"Unknown worker mode: {mode}")
            sys.exit(1)

        logger.info(f"Worker completed successfully: {result}")
        sys.exit(0)

    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Worker failed: {e}", exc_info=True)
        sys.exit(1)


def main():
    """Main entrypoint."""
    parser = argparse.ArgumentParser(description="Xynenyx Ingestion Service")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["ingestion", "processing", "features"],
        default=None,  # Will use env var or settings default
        help="Worker mode to run",
    )

    args = parser.parse_args()

    # Determine mode: CLI arg > env var > settings default
    mode = args.mode
    if not mode:
        # Check environment variable (Railway sets this)
        import os
        mode = os.getenv("WORKER_MODE") or settings.worker_mode

    if mode not in ["ingestion", "processing", "features"]:
        logger.error(f"Invalid worker mode: {mode}. Must be one of: ingestion, processing, features")
        sys.exit(1)

    # Validate configuration
    if not settings.supabase_url or not settings.supabase_service_key:
        logger.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        sys.exit(1)

    # Run worker
    asyncio.run(run_worker(mode))


if __name__ == "__main__":
    main()

