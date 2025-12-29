#!/usr/bin/env python3
"""
Cleanup script for bad funding data created before the fixes.

This script:
1. Fixes funding round amounts (multiplies by 1M if clearly in millions)
2. Fixes missing round_date by extracting from document metadata
3. Deletes funding rounds with clearly invalid amounts
4. Deletes bad company names (false positives)
5. Marks documents for re-processing
6. Optionally re-runs feature extraction
"""
import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
from uuid import UUID

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.shared.clients import SupabaseClient
from app.workers.features_worker import FeaturesWorker
from app.config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataCleanup:
    """Cleanup bad funding data."""
    
    def __init__(self):
        """Initialize cleanup."""
        self.supabase_client = SupabaseClient()
        self.features_worker = FeaturesWorker()
    
    async def run(self, dry_run: bool = True, fix_amounts: bool = True, 
                  fix_dates: bool = True, delete_bad_companies: bool = True, reprocess: bool = False):
        """
        Run cleanup.
        
        Args:
            dry_run: If True, only show what would be done without making changes
            fix_amounts: Fix funding amounts by multiplying by 1M
            fix_dates: Extract and set round_date from document metadata
            delete_bad_companies: Delete companies with bad names
            reprocess: Re-run feature extraction on affected documents
        """
        logger.info(f"Starting cleanup (dry_run={dry_run})...")
        
        # Step 1: Fix funding round amounts
        if fix_amounts:
            await self._fix_funding_amounts(dry_run)
        
        # Step 2: Fix missing round dates
        if fix_dates:
            await self._fix_round_dates(dry_run)
        
        # Step 3: Delete bad companies
        if delete_bad_companies:
            await self._delete_bad_companies(dry_run)
        
        # Step 4: Mark documents for re-processing
        if reprocess:
            await self._mark_for_reprocessing(dry_run)
            
            if not dry_run:
                # Step 5: Re-run feature extraction
                logger.info("Re-running feature extraction...")
                result = await self.features_worker.run()
                logger.info(f"Feature extraction complete: {result}")
        
        logger.info("Cleanup complete!")
    
    async def _fix_funding_amounts(self, dry_run: bool):
        """Fix funding round amounts."""
        logger.info("Analyzing funding rounds...")
        
        # Get all funding rounds
        result = self.supabase_client.client.table("funding_rounds").select("*").execute()
        funding_rounds = result.data if result.data else []
        
        logger.info(f"Found {len(funding_rounds)} funding rounds")
        
        # Identify rounds that need fixing
        to_fix = []
        to_delete = []
        to_flag = []  # Suspiciously high amounts
        
        for fr in funding_rounds:
            amount_usd = fr.get("amount_usd")
            if amount_usd is None:
                continue
            
            amount_usd = float(amount_usd)
            
            # Rounds with amounts < $10,000 are likely wrong (should be millions)
            if amount_usd < 10_000:
                # Check if amount_original suggests it's in millions
                amount_original = fr.get("amount_original")
                if amount_original:
                    amount_original = float(amount_original)
                    # If original is similar to USD, it's likely in millions
                    if abs(amount_usd - amount_original) < 0.01:
                        # This is likely "$X million" stored as X instead of X * 1M
                        new_amount = amount_usd * 1_000_000
                        to_fix.append({
                            "id": fr["id"],
                            "old_amount": amount_usd,
                            "new_amount": new_amount,
                            "reason": "Amount appears to be in millions"
                        })
                    else:
                        # Amount is too small and doesn't match original - likely invalid
                        to_delete.append({
                            "id": fr["id"],
                            "amount": amount_usd,
                            "reason": "Amount too small and doesn't match pattern"
                        })
                else:
                    # No original amount - likely invalid
                    to_delete.append({
                        "id": fr["id"],
                        "amount": amount_usd,
                        "reason": "Amount too small with no original amount"
                    })
            # Rounds between $10K and $1M might be correct or might be wrong
            # We'll leave these alone for now
            
            # Delete amounts >$100B - almost certainly wrong (confusing valuation with funding, or parsing errors)
            # Largest funding rounds in history are typically $10-20B, not hundreds of billions
            if amount_usd > 100_000_000_000:  # >$100B
                to_delete.append({
                    "id": fr["id"],
                    "amount": amount_usd,
                    "reason": f"Amount >$100B (${amount_usd/1_000_000_000:.1f}B) - likely extraction error (valuation vs funding)"
                })
            # Flag suspiciously high amounts (>$10B but <=$100B) - review needed
            elif amount_usd > 10_000_000_000:
                to_flag.append({
                    "id": fr["id"],
                    "amount": amount_usd,
                    "amount_original": fr.get("amount_original"),
                    "company_name": fr.get("company_id"),  # Will look up later
                    "reason": "Suspiciously high amount (>$10B, <=$100B) - review recommended"
                })
        
        logger.info(f"Found {len(to_fix)} funding rounds to fix")
        logger.info(f"Found {len(to_delete)} funding rounds to delete")
        logger.info(f"Found {len(to_flag)} funding rounds with suspiciously high amounts")
        
        if dry_run:
            logger.info("\n=== DRY RUN - Would fix the following ===")
            for fix in to_fix[:10]:  # Show first 10
                logger.info(f"  ID: {fix['id']}, {fix['old_amount']} -> {fix['new_amount']} ({fix['reason']})")
            if len(to_fix) > 10:
                logger.info(f"  ... and {len(to_fix) - 10} more")
            
            logger.info("\n=== DRY RUN - Would delete the following ===")
            for delete in to_delete[:10]:  # Show first 10
                logger.info(f"  ID: {delete['id']}, Amount: {delete['amount']} ({delete['reason']})")
            if len(to_delete) > 10:
                logger.info(f"  ... and {len(to_delete) - 10} more")
            
            logger.info("\n=== DRY RUN - Suspiciously high amounts (review needed) ===")
            for flag in to_flag[:10]:  # Show first 10
                logger.info(f"  ID: {flag['id']}, Amount: ${flag['amount']:,.0f}, Original: {flag['amount_original']} ({flag['reason']})")
            if len(to_flag) > 10:
                logger.info(f"  ... and {len(to_flag) - 10} more")
        else:
            # Fix amounts
            for fix in to_fix:
                try:
                    self.supabase_client.client.table("funding_rounds").update({
                        "amount_usd": fix["new_amount"]
                    }).eq("id", fix["id"]).execute()
                    logger.debug(f"Fixed funding round {fix['id']}: {fix['old_amount']} -> {fix['new_amount']}")
                except Exception as e:
                    logger.error(f"Error fixing funding round {fix['id']}: {e}")
            
            # Delete invalid rounds
            for delete in to_delete:
                try:
                    self.supabase_client.client.table("funding_rounds").delete().eq("id", delete["id"]).execute()
                    logger.debug(f"Deleted funding round {delete['id']}")
                except Exception as e:
                    logger.error(f"Error deleting funding round {delete['id']}: {e}")
            
            logger.info(f"Fixed {len(to_fix)} funding rounds")
            logger.info(f"Deleted {len(to_delete)} funding rounds")
            if to_flag:
                logger.warning(f"Found {len(to_flag)} funding rounds with suspiciously high amounts (>$10B) - review recommended")
                # Optionally delete or flag these - for now just log
                for flag in to_flag:
                    logger.warning(f"  Suspicious: ID {flag['id']}, Amount: ${flag['amount']:,.0f}")
    
    async def _fix_round_dates(self, dry_run: bool):
        """Fix missing round_date by extracting from document metadata."""
        logger.info("Analyzing funding rounds for missing dates...")
        
        # Get funding rounds with NULL round_date
        result = self.supabase_client.client.table("funding_rounds").select(
            "id, document_id, round_date"
        ).is_("round_date", "null").execute()
        
        funding_rounds = result.data if result.data else []
        logger.info(f"Found {len(funding_rounds)} funding rounds with NULL round_date")
        
        if not funding_rounds:
            logger.info("No funding rounds need date fixes")
            return
        
        # Get document IDs
        document_ids = list(set([fr["document_id"] for fr in funding_rounds]))
        
        # Get documents with metadata
        docs_result = self.supabase_client.client.table("documents").select(
            "id, metadata, created_at"
        ).in_("id", document_ids).execute()
        
        documents = {doc["id"]: doc for doc in (docs_result.data if docs_result.data else [])}
        
        # Map funding rounds to documents
        to_fix = []
        fixed_ids = set()
        
        for fr in funding_rounds:
            fr_id = fr["id"]
            doc_id = fr["document_id"]
            doc = documents.get(doc_id)
            
            if not doc:
                continue
            
            metadata = doc.get("metadata", {})
            published_date = metadata.get("published_date")
            
            # Try published_date first
            if published_date:
                try:
                    import dateparser
                    parsed = dateparser.parse(published_date)
                    if parsed:
                        round_date = parsed.date().isoformat()
                        to_fix.append({
                            "id": fr_id,
                            "round_date": round_date,
                            "source": "published_date"
                        })
                        fixed_ids.add(fr_id)
                except Exception as e:
                    logger.debug(f"Error parsing date {published_date}: {e}")
            
            # Fallback to document created_at if no published_date
            if fr_id not in fixed_ids:
                try:
                    created_at = doc.get("created_at")
                    if created_at:
                        from datetime import datetime
                        parsed = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        round_date = parsed.date().isoformat()
                        to_fix.append({
                            "id": fr_id,
                            "round_date": round_date,
                            "source": "created_at"
                        })
                        fixed_ids.add(fr_id)
                except Exception as e:
                    logger.debug(f"Error parsing created_at: {e}")
        
        logger.info(f"Found {len(to_fix)} funding rounds that can have dates fixed")
        
        if dry_run:
            logger.info("\n=== DRY RUN - Would fix dates for the following ===")
            for fix in to_fix[:10]:  # Show first 10
                logger.info(f"  ID: {fix['id']}, Date: {fix['round_date']} (from {fix['source']})")
            if len(to_fix) > 10:
                logger.info(f"  ... and {len(to_fix) - 10} more")
        else:
            # Fix dates
            for fix in to_fix:
                try:
                    self.supabase_client.client.table("funding_rounds").update({
                        "round_date": fix["round_date"]
                    }).eq("id", fix["id"]).execute()
                    logger.debug(f"Fixed date for funding round {fix['id']}: {fix['round_date']}")
                except Exception as e:
                    logger.error(f"Error fixing date for funding round {fix['id']}: {e}")
            
            logger.info(f"Fixed dates for {len(to_fix)} funding rounds")
    
    async def _delete_bad_companies(self, dry_run: bool):
        """Delete companies with bad names (false positives)."""
        logger.info("Analyzing companies...")
        
        # Get all companies
        result = self.supabase_client.client.table("companies").select("*").execute()
        companies = result.data if result.data else []
        
        logger.info(f"Found {len(companies)} companies")
        
        # Bad company name patterns (expanded)
        bad_patterns = [
            "was caught in",
            "funding rounds fell",
            "opportunities",
            "that had raised",
            "Battlefield Cup",
            "funding rounds",
            "startup funding",
            "venture capital",
            "continues to diversify",
            "like Netflix",
            "said Friday that",
            "must build",
            "to become",
            "raised",
            "secured",
            "last December",
            "stories",
            "activity",
            "conceded that",
            "could lose the",
            "has faced its",
            "acceleration programs across",
            "is defensible when",
            "companies likely testing",
            "companies to try",
            "comes waltzing into",
            "is also rumored",
            "afloat and his",
            "that uses AI",
            "that monitors hacking",
            "could corroborate the",
            "wrote in its",
            "customers who use",
            "that makes money",
            "and technology of",
            "said in",
            "will be searching",
            "reporter at techcrunch",
            "to fix it",
            "skipped that step",
            "with little success",
            "should pursue venture",
            "that produces lithium",
            "Locusview sold for",
            "with some friends",
            "Dazzle raises",
            "Resolve AI hits",
            "Koala\nfor",
        ]
        
        # Also check for very short names or names that are common words
        to_delete = []
        
        for company in companies:
            name = company.get("name", "").lower()
            
            # Check against bad patterns
            if any(pattern.lower() in name for pattern in bad_patterns):
                to_delete.append({
                    "id": company["id"],
                    "name": company["name"],
                    "reason": "Matches bad pattern"
                })
                continue
            
            # Check for very short names
            if len(name) < 3:
                to_delete.append({
                    "id": company["id"],
                    "name": company["name"],
                    "reason": "Name too short"
                })
                continue
            
            # Check for names with 4+ words (usually false positives)
            if len(name.split()) >= 4:
                to_delete.append({
                    "id": company["id"],
                    "name": company["name"],
                    "reason": "Too many words (4+)"
                })
                continue
            
            # Check for names that are just common words
            common_words = [
                "the", "this", "that", "these", "those", "today", "yesterday",
                "delaware", "stock", "ipo", "mtv", "ngl", "ftc", "cisco", "talos",
                "battlefield", "insider", "paramount", "netflix", "openai", "elevenlabs",
            ]
            if name in common_words:
                # Check if company has funding rounds
                fr_result = self.supabase_client.client.table("funding_rounds").select("id").eq("company_id", company["id"]).execute()
                funding_count = len(fr_result.data) if fr_result.data else 0
                if funding_count == 0:
                    to_delete.append({
                        "id": company["id"],
                        "name": company["name"],
                        "reason": "Common word with no funding rounds"
                    })
        
        logger.info(f"Found {len(to_delete)} bad companies to delete")
        
        if dry_run:
            logger.info("\n=== DRY RUN - Would delete the following companies ===")
            for delete in to_delete[:20]:  # Show first 20
                logger.info(f"  ID: {delete['id']}, Name: '{delete['name']}' ({delete['reason']})")
            if len(to_delete) > 20:
                logger.info(f"  ... and {len(to_delete) - 20} more")
        else:
            # Delete bad companies
            for delete in to_delete:
                try:
                    # First, set company_id to NULL in funding_rounds that reference this company
                    self.supabase_client.client.table("funding_rounds").update({
                        "company_id": None
                    }).eq("company_id", delete["id"]).execute()
                    
                    # Then delete the company
                    self.supabase_client.client.table("companies").delete().eq("id", delete["id"]).execute()
                    logger.debug(f"Deleted company {delete['id']}: {delete['name']}")
                except Exception as e:
                    logger.error(f"Error deleting company {delete['id']}: {e}")
            
            logger.info(f"Deleted {len(to_delete)} bad companies")
    
    async def _mark_for_reprocessing(self, dry_run: bool):
        """Mark documents for re-processing by setting features_extracted=false."""
        logger.info("Finding documents to mark for re-processing...")
        
        # Get all documents that have features_extracted=true
        result = self.supabase_client.client.table("documents").select("id").eq("features_extracted", True).execute()
        documents = result.data if result.data else []
        
        logger.info(f"Found {len(documents)} documents with features_extracted=true")
        
        if dry_run:
            logger.info(f"Would mark {len(documents)} documents for re-processing")
        else:
            # Mark for re-processing in batches
            batch_size = 100
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                try:
                    # Update all documents in batch
                    for doc in batch:
                        self.supabase_client.client.table("documents").update({
                            "features_extracted": False
                        }).eq("id", doc["id"]).execute()
                    logger.info(f"Marked batch {i//batch_size + 1} for re-processing ({len(batch)} documents)")
                except Exception as e:
                    logger.error(f"Error marking batch for re-processing: {e}")
            
            logger.info(f"Marked {len(documents)} documents for re-processing")


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Cleanup bad funding data")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Only show what would be done (default: True)")
    parser.add_argument("--execute", action="store_true",
                        help="Actually execute the cleanup (overrides --dry-run)")
    parser.add_argument("--no-fix-amounts", action="store_true",
                        help="Skip fixing funding amounts")
    parser.add_argument("--no-fix-dates", action="store_true",
                        help="Skip fixing missing round dates")
    parser.add_argument("--no-delete-companies", action="store_true",
                        help="Skip deleting bad companies")
    parser.add_argument("--reprocess", action="store_true",
                        help="Re-run feature extraction after cleanup")
    
    args = parser.parse_args()
    
    dry_run = not args.execute
    
    cleanup = DataCleanup()
    await cleanup.run(
        dry_run=dry_run,
        fix_amounts=not args.no_fix_amounts,
        fix_dates=not args.no_fix_dates,
        delete_bad_companies=not args.no_delete_companies,
        reprocess=args.reprocess
    )


if __name__ == "__main__":
    asyncio.run(main())

