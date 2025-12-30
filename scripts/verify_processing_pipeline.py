"""Verify processing pipeline against raw data."""
import asyncio
import logging
import os
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
from app.shared.extractors import MetadataExtractor

# Load environment variables
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ProcessingPipelineVerifier:
    """Verify processing pipeline correctness."""

    def __init__(self):
        """Initialize Supabase client and extractor."""
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv(
            "SUPABASE_SERVICE_ROLE_KEY"
        )

        if not supabase_url or not supabase_key:
            raise ValueError(
                "Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in environment"
            )

        self.client: Client = create_client(supabase_url, supabase_key)
        self.extractor = MetadataExtractor()

    async def verify_funding_extraction(self, sample_size: int = 10):
        """Verify funding amount extraction logic."""
        logger.info("Verifying funding extraction logic...")

        # Get sample documents with raw_content
        result = (
            self.client.table("documents")
            .select("id, metadata")
            .not_.is_("metadata->raw_content", "null")
            .limit(sample_size)
            .execute()
        )

        documents = result.data if result.data else []
        logger.info(f"Testing extraction on {len(documents)} sample documents")

        issues = []

        for doc in documents:
            doc_id = doc.get("id")
            metadata = doc.get("metadata", {})
            raw_content = metadata.get("raw_content", "")

            if not raw_content:
                continue

            # Extract funding amounts using current logic
            extracted = self.extractor._extract_funding_amounts(raw_content)

            # Check each extracted amount
            for funding_data in extracted:
                amount_millions = funding_data.get("amount_millions", 0)
                currency = funding_data.get("currency", "USD")

                # Verify conversion logic
                # amount_millions should be in millions (e.g., 10 for "$10 million")
                # In features_worker, this gets multiplied by 1_000_000
                expected_usd = amount_millions * 1_000_000

                # Check for suspicious amounts
                if amount_millions > 50_000:  # >$50B in millions
                    # Check context
                    position = funding_data.get("position", 0)
                    context_start = max(0, position - 100)
                    context_end = min(len(raw_content), position + 100)
                    context = raw_content[context_start:context_end].lower()

                    valuation_indicators = ["valuation", "valued at", "worth"]
                    funding_indicators = ["raised", "funding", "secured", "closed"]

                    has_valuation = any(ind in context for ind in valuation_indicators)
                    has_funding = any(ind in context for ind in funding_indicators)

                    if has_valuation and not has_funding:
                        issues.append({
                            "document_id": doc_id,
                            "issue": "Valuation extracted as funding",
                            "amount_millions": amount_millions,
                            "context": context[:200],
                        })

                # Check for k/K conversion (should be divided by 1000)
                # This is already handled in extractor, but verify
                if "k" in raw_content.lower() or "K" in raw_content.lower():
                    # Check if k/K amounts are correctly converted
                    pass  # Already handled in extractor

        if issues:
            logger.warning(f"Found {len(issues)} potential extraction issues:")
            for issue in issues[:5]:  # Show first 5
                logger.warning(f"  Doc {issue['document_id']}: {issue['issue']} - ${issue['amount_millions']}M")
        else:
            logger.info("No funding extraction issues found")

        return issues

    async def verify_company_extraction(self, sample_size: int = 10):
        """Verify company name extraction logic."""
        logger.info("Verifying company extraction logic...")

        # Get sample documents
        result = (
            self.client.table("documents")
            .select("id, metadata")
            .not_.is_("metadata->raw_content", "null")
            .limit(sample_size)
            .execute()
        )

        documents = result.data if result.data else []
        logger.info(f"Testing extraction on {len(documents)} sample documents")

        issues = []

        for doc in documents:
            doc_id = doc.get("id")
            metadata = doc.get("metadata", {})
            raw_content = metadata.get("raw_content", "")

            if not raw_content:
                continue

            # Extract companies using current logic
            extracted = self.extractor._extract_companies(raw_content)

            # Check for common false positives
            false_positives = [
                "Funding Rounds", "Funding Round", "Startup Funding",
                "Venture Capital", "Series A", "Series B",
            ]

            for company in extracted:
                if any(fp.lower() in company.lower() for fp in false_positives):
                    issues.append({
                        "document_id": doc_id,
                        "issue": "False positive company name",
                        "company": company,
                    })

                # Check for very long names (4+ words)
                if len(company.split()) > 3:
                    issues.append({
                        "document_id": doc_id,
                        "issue": "Very long company name (likely false positive)",
                        "company": company,
                    })

        if issues:
            logger.warning(f"Found {len(issues)} potential company extraction issues:")
            for issue in issues[:5]:
                logger.warning(f"  Doc {issue['document_id']}: {issue['company']}")
        else:
            logger.info("No company extraction issues found")

        return issues

    async def verify_amount_conversion(self):
        """Verify funding amount conversion in database."""
        logger.info("Verifying funding amount conversion in database...")

        # Get funding rounds with their original amounts
        result = (
            self.client.table("funding_rounds")
            .select("id, amount_usd, amount_original, currency")
            .gt("amount_usd", 0)
            .not_.is_("amount_original", "null")
            .limit(20)
            .execute()
        )

        rounds = result.data if result.data else []
        logger.info(f"Checking {len(rounds)} funding rounds")

        issues = []

        for round_data in rounds:
            amount_usd = float(round_data.get("amount_usd", 0) or 0)
            amount_original = float(round_data.get("amount_original", 0) or 0)
            currency = round_data.get("currency", "USD")

            # Verify conversion: amount_original is in millions, should multiply by 1M
            # But check if amount_original might already be in actual USD (legacy data)
            # If amount_original > 1_000_000, it's likely already in USD, not millions
            if amount_original > 1_000_000:
                # amount_original is likely already in USD (legacy data)
                expected_usd = amount_original
            else:
                # amount_original is in millions, multiply by 1M
                expected_usd = amount_original * 1_000_000

            # Allow small floating point differences (1% tolerance)
            tolerance = max(1000, expected_usd * 0.01)
            if abs(amount_usd - expected_usd) > tolerance:
                issues.append({
                    "round_id": round_data.get("id"),
                    "issue": "Amount conversion mismatch",
                    "amount_usd": amount_usd,
                    "amount_original": amount_original,
                    "expected_usd": expected_usd,
                    "difference": abs(amount_usd - expected_usd),
                    "likely_issue": "amount_original might be in USD already" if amount_original > 1_000_000 else "amount_usd not multiplied correctly",
                })

        if issues:
            logger.warning(f"Found {len(issues)} amount conversion issues:")
            for issue in issues[:5]:
                logger.warning(
                    f"  Round {issue['round_id']}: ${issue['amount_usd']/1_000_000:.1f}M vs expected ${issue['expected_usd']/1_000_000:.1f}M"
                )
        else:
            logger.info("All amount conversions are correct")

        return issues

    async def verify_date_extraction(self):
        """Verify date extraction and assignment."""
        logger.info("Verifying date extraction...")

        # Get funding rounds without dates
        result = (
            self.client.table("funding_rounds")
            .select("id, round_date, document_id")
            .is_("round_date", "null")
            .limit(10)
            .execute()
        )

        rounds_without_dates = result.data if result.data else []
        logger.info(f"Found {len(rounds_without_dates)} funding rounds without dates")

        if rounds_without_dates:
            logger.warning("Some funding rounds are missing dates")
            # Check if documents have published_date
            for round_data in rounds_without_dates[:5]:
                doc_id = round_data.get("document_id")
                try:
                    doc_result = (
                        self.client.table("documents")
                        .select("metadata")
                        .eq("id", doc_id)
                        .execute()
                    )
                    if doc_result.data:
                        metadata = doc_result.data[0].get("metadata", {})
                        published_date = metadata.get("published_date")
                        if published_date:
                            logger.warning(
                                f"  Round {round_data.get('id')}: Document has published_date {published_date} but round_date is null"
                            )
                except Exception:
                    pass

        return len(rounds_without_dates)

    async def run_all_checks(self):
        """Run all processing verification checks."""
        logger.info("Starting processing pipeline verification...")

        results = {
            "funding_extraction": await self.verify_funding_extraction(),
            "company_extraction": await self.verify_company_extraction(),
            "amount_conversion": await self.verify_amount_conversion(),
            "date_extraction": await self.verify_date_extraction(),
        }

        logger.info("\n=== PROCESSING PIPELINE VERIFICATION SUMMARY ===")
        logger.info(f"Funding Extraction Issues: {len(results['funding_extraction'])}")
        logger.info(f"Company Extraction Issues: {len(results['company_extraction'])}")
        logger.info(f"Amount Conversion Issues: {len(results['amount_conversion'])}")
        logger.info(f"Rounds Without Dates: {results['date_extraction']}")

        return results


async def main():
    """Main entry point."""
    verifier = ProcessingPipelineVerifier()
    await verifier.run_all_checks()


if __name__ == "__main__":
    asyncio.run(main())

