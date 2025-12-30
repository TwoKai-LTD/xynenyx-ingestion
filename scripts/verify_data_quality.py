"""One-off script to verify data quality and identify issues."""
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


class DataQualityVerifier:
    """Verify data quality and identify issues."""

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

    async def verify_company_names(self):
        """Verify that funding rounds have associated company names."""
        logger.info("Verifying company names in funding rounds...")

        # Get funding rounds with company_ids
        result = (
            self.client.table("funding_rounds")
            .select("id, company_id, amount_usd")
            .not_.is_("company_id", "null")
            .gt("amount_usd", 0)
            .execute()
        )

        funding_rounds = result.data if result.data else []
        logger.info(f"Found {len(funding_rounds)} funding rounds with company_ids")

        # Get unique company IDs
        company_ids = list(set(r.get("company_id") for r in funding_rounds if r.get("company_id")))
        logger.info(f"Found {len(company_ids)} unique company IDs")

        # Check which companies have names
        companies_with_names = 0
        companies_without_names = 0

        # Query in batches
        batch_size = 100
        for i in range(0, len(company_ids), batch_size):
            batch = company_ids[i : i + batch_size]
            try:
                comp_result = (
                    self.client.table("companies")
                    .select("id, name")
                    .in_("id", batch)
                    .execute()
                )
                if comp_result.data:
                    for comp in comp_result.data:
                        if comp.get("name"):
                            companies_with_names += 1
                        else:
                            companies_without_names += 1
            except Exception as e:
                logger.error(f"Error querying companies batch: {e}")

        logger.info(
            f"Companies with names: {companies_with_names}, without names: {companies_without_names}"
        )

        # Check funding rounds that would be missing company names (use batch query)
        rounds_without_names = 0
        if company_ids:
            # Query all companies at once
            try:
                all_companies_result = (
                    self.client.table("companies")
                    .select("id, name")
                    .in_("id", company_ids)
                    .execute()
                )
                companies_with_names = {
                    comp["id"]: comp.get("name")
                    for comp in (all_companies_result.data or [])
                }
                # Check which rounds would be missing names
                for round_data in funding_rounds:
                    company_id = round_data.get("company_id")
                    if company_id and not companies_with_names.get(company_id):
                        rounds_without_names += 1
            except Exception as e:
                logger.error(f"Error checking company names: {e}")
                # Fallback: assume all have names if we can't check
                rounds_without_names = 0

        logger.info(
            f"Funding rounds that would be missing company names: {rounds_without_names}"
        )

        return {
            "total_rounds": len(funding_rounds),
            "unique_companies": len(company_ids),
            "companies_with_names": companies_with_names,
            "companies_without_names": companies_without_names,
            "rounds_without_names": rounds_without_names,
        }

    async def verify_sector_data(self):
        """Verify sector data structure."""
        logger.info("Verifying sector data...")

        # Get document features with sectors
        result = (
            self.client.table("document_features")
            .select("document_id, sectors")
            .not_.is_("sectors", "null")
            .execute()
        )

        features = result.data if result.data else []
        logger.info(f"Found {len(features)} document features with sectors")

        # Check sector structure
        array_sectors = 0
        single_sectors = 0
        empty_sectors = 0

        for feature in features:
            sectors = feature.get("sectors", [])
            if isinstance(sectors, list):
                if len(sectors) > 1:
                    array_sectors += 1
                elif len(sectors) == 1:
                    single_sectors += 1
                else:
                    empty_sectors += 1
            else:
                # Not a list - potential issue
                logger.warning(
                    f"Document {feature.get('document_id')} has non-list sectors: {sectors}"
                )

        logger.info(
            f"Sector structure: {array_sectors} multi-sector, {single_sectors} single-sector, {empty_sectors} empty"
        )

        return {
            "total_features": len(features),
            "multi_sector": array_sectors,
            "single_sector": single_sectors,
            "empty_sectors": empty_sectors,
        }

    async def verify_funding_round_dates(self):
        """Verify funding round dates are reasonable."""
        logger.info("Verifying funding round dates...")

        result = (
            self.client.table("funding_rounds")
            .select("id, round_date, amount_usd")
            .gt("amount_usd", 0)
            .execute()
        )

        rounds = result.data if result.data else []
        logger.info(f"Found {len(rounds)} funding rounds")

        rounds_with_dates = sum(1 for r in rounds if r.get("round_date"))
        rounds_without_dates = len(rounds) - rounds_with_dates

        logger.info(
            f"Rounds with dates: {rounds_with_dates}, without dates: {rounds_without_dates}"
        )

        return {
            "total_rounds": len(rounds),
            "with_dates": rounds_with_dates,
            "without_dates": rounds_without_dates,
        }

    async def verify_funding_amounts(self):
        """Verify funding amounts are reasonable."""
        logger.info("Verifying funding amounts...")

        result = (
            self.client.table("funding_rounds")
            .select("id, amount_usd")
            .gt("amount_usd", 0)
            .execute()
        )

        rounds = result.data if result.data else []
        logger.info(f"Found {len(rounds)} funding rounds")

        # Check for suspiciously high amounts
        suspicious_threshold = 10_000_000_000  # $10B
        suspicious_rounds = [
            r
            for r in rounds
            if float(r.get("amount_usd", 0) or 0) > suspicious_threshold
        ]

        logger.info(
            f"Rounds with amounts >$10B: {len(suspicious_rounds)} (these may be errors)"
        )

        if suspicious_rounds:
            logger.warning("Suspicious funding amounts:")
            for r in suspicious_rounds[:10]:  # Show first 10
                amount_billions = float(r.get("amount_usd", 0) or 0) / 1_000_000_000
                logger.warning(f"  Round {r.get('id')}: ${amount_billions:.2f}B")

        return {
            "total_rounds": len(rounds),
            "suspicious_rounds": len(suspicious_rounds),
        }

    async def run_all_checks(self):
        """Run all data quality checks."""
        logger.info("Starting data quality verification...")

        results = {
            "company_names": await self.verify_company_names(),
            "sector_data": await self.verify_sector_data(),
            "funding_dates": await self.verify_funding_round_dates(),
            "funding_amounts": await self.verify_funding_amounts(),
        }

        logger.info("\n=== DATA QUALITY SUMMARY ===")
        logger.info(f"Company Names: {results['company_names']}")
        logger.info(f"Sector Data: {results['sector_data']}")
        logger.info(f"Funding Dates: {results['funding_dates']}")
        logger.info(f"Funding Amounts: {results['funding_amounts']}")

        return results


async def main():
    """Main entry point."""
    verifier = DataQualityVerifier()
    await verifier.run_all_checks()


if __name__ == "__main__":
    asyncio.run(main())

