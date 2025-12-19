"""Shared pipeline utilities."""
import feedparser
import httpx
from bs4 import BeautifulSoup
from typing import Dict, Any, List, Optional
from datetime import datetime
from app.config import settings


class RSSParser:
    """Parser for RSS and Atom feeds."""

    def __init__(self):
        """Initialize RSS parser."""
        self.timeout = settings.rss_request_timeout
        self.max_retries = settings.rss_max_retries

    def parse_feed(self, feed_url: str) -> Dict[str, Any]:
        """Parse an RSS or Atom feed."""
        try:
            feed = feedparser.parse(feed_url)

            if feed.bozo and feed.bozo_exception:
                raise ValueError(f"Failed to parse feed: {feed.bozo_exception}")

            return {
                "title": feed.feed.get("title", ""),
                "link": feed.feed.get("link", ""),
                "description": feed.feed.get("description", ""),
                "entries": self._parse_entries(feed.entries),
            }
        except Exception as e:
            raise ValueError(f"Error parsing RSS feed {feed_url}: {str(e)}") from e

    def _parse_entries(self, entries: List) -> List[Dict[str, Any]]:
        """Parse feed entries."""
        parsed_entries = []
        seen_urls = set()

        for entry in entries:
            entry_id = entry.get("id", "")
            link = entry.get("link", "")
            title = entry.get("title", "")
            description = entry.get("description", "")

            published_date = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    published_date = datetime(*entry.published_parsed[:6]).isoformat()
                except Exception:
                    pass

            unique_id = link or entry_id
            if not unique_id or unique_id in seen_urls:
                continue

            seen_urls.add(unique_id)

            parsed_entries.append({
                "id": entry_id,
                "link": link,
                "title": title,
                "description": description,
                "published_date": published_date,
                "published_parsed": entry.get("published_parsed"),
            })

        return parsed_entries


class HTMLParser:
    """Parser for extracting main content from HTML pages."""

    def __init__(self):
        """Initialize HTML parser."""
        self.timeout = settings.html_request_timeout
        self.max_retries = settings.html_max_retries
        self.user_agent = settings.html_user_agent

    async def extract_content(self, url: str) -> Optional[str]:
        """Extract main content from an HTML page."""
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            for attempt in range(self.max_retries):
                try:
                    response = await client.get(
                        url,
                        headers={"User-Agent": self.user_agent},
                    )
                    response.raise_for_status()

                    soup = BeautifulSoup(response.text, "html.parser")

                    for script in soup(["script", "style", "nav", "footer", "header", "aside"]):
                        script.decompose()

                    content_selectors = [
                        "article",
                        "main",
                        ".content",
                        ".post",
                        ".entry",
                        ".article-content",
                        "[role='main']",
                    ]

                    content = None
                    for selector in content_selectors:
                        elements = soup.select(selector)
                        if elements:
                            content = elements[0]
                            break

                    if not content:
                        content = soup.find("body")

                    if not content:
                        return None

                    text = content.get_text(separator="\n", strip=True)
                    lines = [line.strip() for line in text.split("\n") if line.strip()]
                    return "\n".join(lines)

                except httpx.TimeoutException:
                    if attempt < self.max_retries - 1:
                        continue
                    return None
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        continue
                    print(f"Error extracting content from {url}: {e}")
                    return None

        return None


class Chunker:
    """Chunker for splitting documents into chunks."""

    def __init__(self):
        """Initialize chunker."""
        from llama_index.core.node_parser import SentenceSplitter
        self.splitter = SentenceSplitter(
            chunk_size=settings.chunk_size,
            chunk_overlap=settings.chunk_overlap,
            paragraph_separator="\n\n",
        )

    def chunk_document(
        self,
        text: str,
        metadata: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Split a document into chunks."""
        from llama_index.core import Document

        # Remove raw_content from metadata before chunking (it's too large)
        chunk_metadata = {k: v for k, v in metadata.items() if k != 'raw_content'}
        doc = Document(text=text, metadata=chunk_metadata)
        nodes = self.splitter.get_nodes_from_documents([doc])

        chunks = []
        for idx, node in enumerate(nodes):
            token_count = len(node.text) // 4

            chunks.append({
                "content": node.text,
                "metadata": {**metadata, **node.metadata},
                "token_count": token_count,
                "chunk_index": idx,
            })

        return chunks

