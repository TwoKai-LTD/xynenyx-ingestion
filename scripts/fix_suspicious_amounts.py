"""One-off script to delete or flag suspicious funding amounts (>$10B)."""
import asyncio
import logging
import os
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class SuspiciousAmountsFixer:
    """Fix suspicious funding amounts."""

    def __init__(self):
        """Initialize Supabase client."""
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv(
            "SUPABASE_SERVICE_ROLE_KEY"
        )

        if not supabase_url or not supabase_key:
            raise ValueError(
                "Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in environment"
            )

        self.client: Client = create_client(supabase_url, supabase_key)

    async def fix_suspicious_amounts(self, dry_run: bool = True, threshold: float = 10_000_000_000):
        """
        Delete funding rounds with suspiciously high amounts.
        
        Args:
            dry_run: If True, only show what would be deleted
            threshold: Amount threshold in USD (default $10B)
        """
        logger.info(f"Finding funding rounds with amounts >${threshold/1_000_000_000:.1f}B...")
        
        # Get suspicious rounds
        result = (
            self.client.table("funding_rounds")
            .select("id, amount_usd, round_date, round_type, company_id")
            .gt("amount_usd", threshold)
            .execute()
        )
        
        suspicious_rounds = result.data if result.data else []
        logger.info(f"Found {len(suspicious_rounds)} suspicious funding rounds")
        
        if not suspicious_rounds:
            logger.info("No suspicious amounts found")
            return
        
        # Show what would be deleted
        logger.info("\nSuspicious funding rounds:")
        for round_data in suspicious_rounds:
            amount_billions = float(round_data.get("amount_usd", 0) or 0) / 1_000_000_000
            logger.info(
                f"  Round {round_data.get('id')}: ${amount_billions:.2f}B on {round_data.get('round_date', 'N/A')}"
            )
        
        if dry_run:
            logger.info("\nDRY RUN: Would delete these rounds. Run with dry_run=False to actually delete.")
            return
        
        # Delete suspicious rounds
        logger.info("\nDeleting suspicious funding rounds...")
        deleted_count = 0
        for round_data in suspicious_rounds:
            try:
                self.client.table("funding_rounds").delete().eq("id", round_data.get("id")).execute()
                deleted_count += 1
                logger.info(f"Deleted round {round_data.get('id')}")
            except Exception as e:
                logger.error(f"Error deleting round {round_data.get('id')}: {e}")
        
        logger.info(f"\nDeleted {deleted_count} suspicious funding rounds")


async def main():
    """Main entry point."""
    import sys
    
    dry_run = "--execute" not in sys.argv
    
    if dry_run:
        logger.info("Running in DRY RUN mode. Use --execute to actually delete.")
    else:
        logger.warning("EXECUTING: Will delete suspicious funding rounds!")
        response = input("Are you sure? (yes/no): ")
        if response.lower() != "yes":
            logger.info("Cancelled.")
            return
    
    fixer = SuspiciousAmountsFixer()
    await fixer.fix_suspicious_amounts(dry_run=dry_run)


if __name__ == "__main__":
    asyncio.run(main())

