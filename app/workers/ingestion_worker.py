"""Ingestion worker - RSS feed parsing and document creation."""
import asyncio
import logging
import time
import hashlib
from typing import List, Dict, Any
from datetime import datetime
from uuid import UUID
from app.shared.clients import SupabaseClient
from app.shared.pipeline import RSSParser, HTMLParser
from app.config import settings

logger = logging.getLogger(__name__)


class IngestionWorker:
    """Worker for ingesting RSS feeds and creating documents."""

    def __init__(self):
        """Initialize ingestion worker."""
        self.supabase_client = SupabaseClient()
        self.rss_parser = RSSParser()
        self.html_parser = HTMLParser()
        self.batch_size = settings.batch_size

    async def run(self) -> Dict[str, Any]:
        """
        Run ingestion worker.

        Returns:
            Dictionary with ingestion results
        """
        start_time = time.time()
        logger.info("Starting ingestion worker...")

        # Get active feeds
        feeds = self.supabase_client.list_feeds(status="active")
        logger.info(f"Found {len(feeds)} active feeds")

        if not feeds:
            logger.info("No active feeds found")
            return {
                "status": "completed",
                "feeds_processed": 0,
                "articles_ingested": 0,
                "articles_failed": 0,
                "duration_seconds": time.time() - start_time,
            }

        total_articles = 0
        total_errors = 0
        feed_results = []

        for feed in feeds:
            try:
                result = await self._ingest_feed(feed)
                total_articles += result.get("articles_ingested", 0)
                total_errors += result.get("articles_failed", 0)
                feed_results.append(result)
            except Exception as e:
                logger.error(f"Error ingesting feed {feed['name']}: {e}", exc_info=True)
                total_errors += 1
                feed_results.append({
                    "feed_id": feed["id"],
                    "feed_name": feed["name"],
                    "status": "error",
                    "error": str(e),
                })

        duration = time.time() - start_time
        logger.info(
            f"Ingestion complete: {total_articles} articles ingested, {total_errors} errors, "
            f"duration: {duration:.2f}s"
        )

        return {
            "status": "completed",
            "feeds_processed": len(feeds),
            "articles_ingested": total_articles,
            "articles_failed": total_errors,
            "duration_seconds": duration,
            "feed_results": feed_results,
        }

    async def _ingest_feed(self, feed: Dict[str, Any]) -> Dict[str, Any]:
        """Ingest a single feed."""
        feed_id = feed["id"]
        feed_url = feed["url"]
        feed_name = feed["name"]
        user_id = feed.get("user_id", settings.system_user_id)

        logger.info(f"Ingesting feed: {feed_name} ({feed_url})")

        try:
            # Parse RSS feed
            feed_data = self.rss_parser.parse_feed(feed_url)
            entries = feed_data.get("entries", [])

            if not entries:
                logger.info(f"No new entries in feed {feed_name}")
                return {
                    "feed_id": feed_id,
                    "feed_name": feed_name,
                    "articles_ingested": 0,
                    "articles_failed": 0,
                    "status": "completed",
                }

            articles_ingested = 0
            articles_failed = 0

            # Process entries in batches
            for i in range(0, len(entries), self.batch_size):
                batch = entries[i : i + self.batch_size]
                tasks = [self._process_entry(entry, feed_name, feed_url, feed_id, user_id) for entry in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        articles_failed += 1
                        logger.error(f"Error processing entry: {result}")
                    elif result:
                        articles_ingested += 1

            # Update feed status
            self.supabase_client.update_feed(
                UUID(feed_id),
                {
                    "last_ingested_at": datetime.now().isoformat(),
                    "article_count": feed.get("article_count", 0) + articles_ingested,
                    "status": "active",
                    "error_message": None,
                }
            )

            logger.info(f"Feed {feed_name}: {articles_ingested} ingested, {articles_failed} failed")

            return {
                "feed_id": feed_id,
                "feed_name": feed_name,
                "articles_ingested": articles_ingested,
                "articles_failed": articles_failed,
                "status": "completed",
            }

        except Exception as e:
            logger.error(f"Error ingesting feed {feed_name}: {e}")
            # Update feed status to error
            self.supabase_client.update_feed(
                UUID(feed_id),
                {
                    "status": "error",
                    "error_message": str(e),
                }
            )
            raise

    async def _process_entry(
        self,
        entry: Dict[str, Any],
        feed_name: str,
        feed_url: str,
        feed_id: str,
        user_id: str,
    ) -> bool:
        """Process a single RSS entry and create document."""
        try:
            article_url = entry.get("link", "")
            article_title = entry.get("title", "Untitled")

            # Generate unique s3_key for deduplication
            # Use article URL hash for better deduplication across feeds
            import hashlib
            url_hash = hashlib.md5(article_url.encode()).hexdigest()[:12]
            article_id = entry.get("id", "")
            if not article_id:
                # Fallback: use URL slug or hash
                article_id = article_url.split("/")[-1] or url_hash
            
            # s3_key format: rss://{feed_id}/{article_id_hash}
            # This ensures same article from same feed is deduplicated
            s3_key = f"rss://{feed_id}/{url_hash}"

            # Check if document already exists (deduplication)
            existing = self.supabase_client.get_document_by_s3_key(s3_key)
            if existing:
                logger.debug(f"Document already exists for article: {article_url} (s3_key: {s3_key})")
                return False  # Skip, already processed

            # Fetch HTML content
            content = await self.html_parser.extract_content(article_url)
            if not content:
                # Fallback to description
                content = entry.get("description", "")
                if not content:
                    logger.warning(f"No content available for article: {article_url}")
                    return False

            # Create document record with status='pending'
            try:
                document = self.supabase_client.create_document(
                    user_id=user_id,
                    name=article_title,
                    s3_key=s3_key,
                    content_type="text/html",
                    metadata={
                        "feed_name": feed_name,
                        "feed_url": feed_url,
                        "article_url": article_url,
                        "article_id": article_id,
                        "published_date": entry.get("published_date"),
                        "raw_content": content,  # Store raw content for processing
                    },
                )

                logger.debug(f"Created document: {document['id']} for article: {article_title}")
                return True
            except Exception as e:
                # Handle duplicate key error (race condition)
                if "duplicate" in str(e).lower() or "unique" in str(e).lower() or "23505" in str(e):
                    logger.debug(f"Document already exists (race condition): {article_url}")
                    return False
                raise

        except Exception as e:
            logger.error(f"Error processing entry {entry.get('link', 'unknown')}: {e}", exc_info=True)
            return False

