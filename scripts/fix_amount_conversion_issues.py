"""Fix funding round amount conversion issues."""
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


class AmountConversionFixer:
    """Fix amount conversion issues."""

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

    async def fix_conversion_issues(self, dry_run: bool = True):
        """
        Fix funding rounds where amount_usd is incorrect.
        
        Args:
            dry_run: If True, only show what would be fixed
        """
        logger.info("Finding funding rounds with conversion issues...")

        # Get rounds where amount_original is large but amount_usd is small or zero
        result = (
            self.client.table("funding_rounds")
            .select("id, amount_usd, amount_original, currency, round_date")
            .gt("amount_original", 1000)  # >$1B in millions
            .lt("amount_usd", 1_000_000)  # <$1M in USD (clearly wrong)
            .execute()
        )

        rounds = result.data if result.data else []
        logger.info(f"Found {len(rounds)} rounds with potential conversion issues")

        if not rounds:
            logger.info("No conversion issues found")
            return

        to_fix = []
        to_delete = []

        for round_data in rounds:
            round_id = round_data.get("id")
            amount_usd = float(round_data.get("amount_usd", 0) or 0)
            amount_original = float(round_data.get("amount_original", 0) or 0)
            currency = round_data.get("currency", "USD")

            # If amount_original > 1,000 (millions), it's >$1B - check if it's a real error
            # Real funding rounds rarely exceed $10B, so anything >$10B in millions is suspicious
            if amount_original > 10_000:  # >$10B
                to_delete.append({
                    "id": round_id,
                    "amount_original": amount_original,
                    "amount_usd": amount_usd,
                    "reason": f"amount_original {amount_original}M is >$10B (likely extraction error or valuation)",
                })
                continue

            # Calculate expected USD: amount_original is in millions, multiply by 1M
            expected_usd = amount_original * 1_000_000

            # Apply currency conversion if needed
            if currency == "EUR":
                expected_usd = expected_usd * 1.1
            elif currency == "GBP":
                expected_usd = expected_usd * 1.25

            # If expected is >$50B, delete it (validation should have caught this)
            if expected_usd > 50_000_000_000:
                to_delete.append({
                    "id": round_id,
                    "amount_original": amount_original,
                    "expected_usd": expected_usd,
                    "reason": f"Expected amount ${expected_usd/1_000_000_000:.1f}B is >$50B (should have been rejected)",
                })
                continue

            # Fix the amount_usd
            to_fix.append({
                "id": round_id,
                "old_amount_usd": amount_usd,
                "new_amount_usd": expected_usd,
                "amount_original": amount_original,
                "currency": currency,
            })

        logger.info(f"\nWould fix {len(to_fix)} rounds")
        logger.info(f"Would delete {len(to_delete)} rounds")

        if to_fix:
            logger.info("\nRounds to fix:")
            for fix in to_fix[:10]:
                logger.info(
                    f"  Round {fix['id'][:8]}: ${fix['old_amount_usd']/1_000_000:.1f}M -> ${fix['new_amount_usd']/1_000_000:.1f}M (original: {fix['amount_original']}M)"
                )

        if to_delete:
            logger.info("\nRounds to delete:")
            for delete in to_delete[:10]:
                logger.info(
                    f"  Round {delete['id'][:8]}: {delete['reason']}"
                )

        if dry_run:
            logger.info("\nDRY RUN: Would fix/delete these rounds. Run with --execute to actually fix.")
            return

        # Fix rounds
        fixed_count = 0
        for fix in to_fix:
            try:
                self.client.table("funding_rounds").update({
                    "amount_usd": fix["new_amount_usd"],
                }).eq("id", fix["id"]).execute()
                fixed_count += 1
                logger.info(f"Fixed round {fix['id'][:8]}")
            except Exception as e:
                logger.error(f"Error fixing round {fix['id']}: {e}")

        # Delete invalid rounds
        deleted_count = 0
        for delete in to_delete:
            try:
                self.client.table("funding_rounds").delete().eq("id", delete["id"]).execute()
                deleted_count += 1
                logger.info(f"Deleted round {delete['id'][:8]}")
            except Exception as e:
                logger.error(f"Error deleting round {delete['id']}: {e}")

        logger.info(f"\nFixed {fixed_count} rounds, deleted {deleted_count} rounds")


async def main():
    """Main entry point."""
    import sys

    dry_run = "--execute" not in sys.argv

    if dry_run:
        logger.info("Running in DRY RUN mode. Use --execute to actually fix.")
    else:
        logger.warning("EXECUTING: Will fix/delete funding rounds!")
        response = input("Are you sure? (yes/no): ")
        if response.lower() != "yes":
            logger.info("Cancelled.")
            return

    fixer = AmountConversionFixer()
    await fixer.fix_conversion_issues(dry_run=dry_run)


if __name__ == "__main__":
    asyncio.run(main())

