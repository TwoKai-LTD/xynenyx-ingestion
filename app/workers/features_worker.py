"""Features worker - extract and store structured features."""

import logging
import time
from typing import List, Dict, Any, Optional
from uuid import UUID
from datetime import datetime
import dateparser
from app.shared.clients import SupabaseClient
from app.shared.extractors import MetadataExtractor, normalize_name
from app.config import settings

logger = logging.getLogger(__name__)


class FeaturesWorker:
    """Worker for extracting and storing features."""

    def __init__(self):
        """Initialize features worker."""
        self.supabase_client = SupabaseClient()
        self.extractor = MetadataExtractor()
        self.batch_size = settings.batch_size

    async def run(self) -> Dict[str, Any]:
        """
        Run features worker.

        Returns:
            Dictionary with feature extraction results
        """
        start_time = time.time()
        logger.info("Starting features worker...")

        # Get documents ready for feature extraction
        # Process in batches to avoid memory issues
        documents = self.supabase_client.list_documents_ready_for_features(
            limit=self.batch_size
        )
        logger.info(
            f"Found {len(documents)} documents for feature extraction (batch size: {self.batch_size})"
        )

        if not documents:
            logger.info("No documents for feature extraction")
            return {
                "status": "completed",
                "documents_processed": 0,
                "companies_created": 0,
                "investors_created": 0,
                "funding_rounds_created": 0,
                "errors": 0,
                "duration_seconds": time.time() - start_time,
            }

        documents_processed = 0
        companies_created = 0
        investors_created = 0
        funding_rounds_created = 0
        errors = 0

        for doc in documents:
            try:
                result = await self._extract_features(doc)
                documents_processed += 1
                companies_created += result.get("companies_created", 0)
                investors_created += result.get("investors_created", 0)
                funding_rounds_created += result.get("funding_rounds_created", 0)
            except Exception as e:
                errors += 1
                logger.error(
                    f"Error extracting features for document {doc['id']}: {e}",
                    exc_info=True,
                )

        duration = time.time() - start_time
        logger.info(
            f"Feature extraction complete: {documents_processed} documents, "
            f"{companies_created} companies, {investors_created} investors, "
            f"{funding_rounds_created} funding rounds, {errors} errors, "
            f"duration: {duration:.2f}s"
        )

        return {
            "status": "completed",
            "documents_processed": documents_processed,
            "companies_created": companies_created,
            "investors_created": investors_created,
            "funding_rounds_created": funding_rounds_created,
            "errors": errors,
            "duration_seconds": duration,
        }

    async def _extract_features(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and store features for a document."""
        document_id = UUID(document["id"])
        metadata = document.get("metadata", {})
        raw_content = metadata.get("raw_content", "")

        if not raw_content:
            logger.warning(f"No raw_content for document {document_id}, skipping")
            self.supabase_client.mark_features_extracted(document_id)
            return {}

        logger.debug(f"Extracting features for document: {document_id}")

        # Extract metadata
        extracted_metadata = self.extractor.extract(raw_content, metadata)

        # Normalize and store companies
        company_ids = []
        companies = extracted_metadata.get("companies", [])
        for company_name in companies:
            try:
                normalized = normalize_name(company_name)
                company = self.supabase_client.create_company(company_name, normalized)
                company_ids.append(UUID(company["id"]))
            except Exception as e:
                logger.warning(f"Error creating company {company_name}: {e}")

        # Normalize and store investors
        investor_ids = []
        investors = extracted_metadata.get("investors", [])
        seen_investors = set()
        for investor_data in investors:
            investor_name = investor_data.get("name", "")
            if not investor_name or investor_name in seen_investors:
                continue
            seen_investors.add(investor_name)
            try:
                normalized = normalize_name(investor_name)
                investor = self.supabase_client.create_investor(
                    investor_name, normalized
                )
                if investor and investor.get("id"):
                    investor_ids.append(UUID(investor["id"]))
            except Exception as e:
                logger.warning(
                    f"Error creating investor {investor_name}: {e}", exc_info=True
                )

        # Create funding rounds
        funding_round_ids = []
        funding_amounts = extracted_metadata.get("funding_amounts", [])
        dates = extracted_metadata.get("dates", [])

        for funding_data in funding_amounts:
            try:
                amount_millions = funding_data.get("amount_millions", 0)
                currency = funding_data.get("currency", "USD")
                round_type = funding_data.get("round")
                funding_position = funding_data.get("position")

                # Convert millions to actual USD amount
                # amount_millions is already in millions (e.g., 10 for "$10 million")
                # So we need to multiply by 1,000,000 to get actual USD
                amount_usd_base = amount_millions * 1_000_000

                # Convert to USD (simplified - in production, use real exchange rates)
                amount_usd = amount_usd_base
                if currency == "EUR":
                    amount_usd = amount_usd_base * 1.1
                elif currency == "GBP":
                    amount_usd = amount_usd_base * 1.25

                # Validation: reject extremely high amounts (>$50B) that are likely extraction errors
                # Largest funding rounds in history are typically $10-20B
                # Amounts >$50B are almost certainly valuations, not funding amounts
                if amount_usd > 50_000_000_000:  # >$50B
                    logger.warning(
                        f"Skipping funding round with amount ${amount_usd/1_000_000_000:.1f}B "
                        f"(likely a valuation, not funding) for document {document_id}"
                    )
                    continue  # Skip this funding round

                # Match company to funding round using proximity
                company_id = None
                if company_ids and funding_position is not None and companies:
                    # Find company name closest to funding amount
                    closest_company_idx = None
                    min_distance = float("inf")
                    for idx, company_name in enumerate(companies):
                        try:
                            # Find company position in content (case-insensitive)
                            company_pos = raw_content.lower().find(company_name.lower())
                            if company_pos != -1:
                                distance = abs(company_pos - funding_position)
                                # Prefer companies within 200 chars of funding amount
                                if distance < min_distance and distance < 200:
                                    min_distance = distance
                                    closest_company_idx = idx
                        except Exception:
                            continue

                    if closest_company_idx is not None and closest_company_idx < len(
                        company_ids
                    ):
                        company_id = company_ids[closest_company_idx]

                # Fallback to first company if no proximity match
                if not company_id and company_ids:
                    company_id = company_ids[0]

                # Get lead investor if available
                lead_investor_id = None
                for inv_data in investors:
                    if inv_data.get("role") == "lead" and investor_ids:
                        lead_investor_id = investor_ids[0]
                        break

                # Try to find a date near the funding amount
                # Use proximity matching if position is available
                round_date = None
                funding_position = funding_data.get("position")

                if dates and funding_position is not None:
                    # Find the closest date to the funding amount
                    closest_date = None
                    min_distance = float("inf")
                    for date_str in dates:
                        try:
                            # Find date position in content (simplified - could be improved)
                            date_pos = raw_content.find(date_str)
                            if date_pos != -1:
                                distance = abs(date_pos - funding_position)
                                if (
                                    distance < min_distance and distance < 500
                                ):  # Within 500 chars
                                    min_distance = distance
                                    closest_date = date_str
                        except Exception:
                            continue

                    if closest_date:
                        try:
                            parsed_date = datetime.fromisoformat(
                                closest_date.replace("Z", "+00:00")
                            )
                            round_date = parsed_date.date().isoformat()
                        except Exception:
                            pass

                # Fallback to first extracted date
                if not round_date and dates:
                    try:
                        parsed_date = datetime.fromisoformat(
                            dates[0].replace("Z", "+00:00")
                        )
                        round_date = parsed_date.date().isoformat()
                    except Exception:
                        pass

                # Fallback to article published_date from metadata
                if not round_date and metadata.get("published_date"):
                    try:
                        parsed = dateparser.parse(metadata["published_date"])
                        if parsed:
                            round_date = parsed.date().isoformat()
                    except Exception:
                        pass

                funding_round = self.supabase_client.create_funding_round(
                    document_id=document_id,
                    company_id=company_id,
                    amount_usd=amount_usd,
                    amount_original=amount_millions,
                    currency=currency,
                    round_type=round_type,
                    round_date=round_date,
                    lead_investor_id=lead_investor_id,
                    investor_ids=investor_ids if investor_ids else None,
                    metadata=funding_data,
                )
                if funding_round and funding_round.get("id"):
                    funding_round_ids.append(UUID(funding_round["id"]))
            except Exception as e:
                logger.warning(f"Error creating funding round: {e}", exc_info=True)

        # Extract sectors
        sectors = []
        sector_data = extracted_metadata.get("sectors", [])
        for sector_info in sector_data:
            sectors.append(sector_info.get("sector", ""))

        # Extract keywords (simple - could be enhanced)
        keywords = []
        # Could add keyword extraction logic here

        # Create document features record
        self.supabase_client.create_document_features(
            document_id=document_id,
            company_ids=company_ids if company_ids else None,
            investor_ids=investor_ids if investor_ids else None,
            funding_round_ids=funding_round_ids if funding_round_ids else None,
            sectors=sectors if sectors else None,
            keywords=keywords if keywords else None,
            metadata=extracted_metadata,
        )

        # Mark document as having features extracted
        self.supabase_client.mark_features_extracted(document_id)

        logger.debug(f"Features extracted for document {document_id}")

        return {
            "companies_created": len(set(company_ids)),
            "investors_created": len(set(investor_ids)),
            "funding_rounds_created": len(funding_round_ids),
        }
