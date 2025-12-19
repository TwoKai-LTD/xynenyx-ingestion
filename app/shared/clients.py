"""Shared clients for Supabase and LLM service."""
from typing import List, Optional
from uuid import UUID
import asyncio
import httpx
from supabase import create_client, Client
from app.config import settings


class SupabaseClient:
    """Supabase client wrapper for ingestion operations."""

    def __init__(self):
        """Initialize Supabase client."""
        self.client: Client = create_client(
            settings.supabase_url,
            settings.supabase_service_key,
        )

    def create_document(
        self,
        user_id: str,
        name: str,
        s3_key: str,
        content_type: str = "text/html",
        metadata: Optional[dict] = None,
    ) -> dict:
        """Create a document record with status='pending'."""
        result = (
            self.client.table("documents")
            .insert(
                {
                    "user_id": user_id,
                    "name": name,
                    "s3_key": s3_key,
                    "content_type": content_type,
                    "status": "pending",
                    "metadata": metadata or {},
                    "features_extracted": False,
                }
            )
            .execute()
        )
        return result.data[0] if result.data else {}

    def update_document_status(
        self,
        document_id: UUID,
        status: str,
        chunk_count: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Update document status."""
        update_data: dict = {"status": status}
        if chunk_count is not None:
            update_data["chunk_count"] = chunk_count
        if error_message:
            update_data["error_message"] = error_message

        self.client.table("documents").update(update_data).eq("id", str(document_id)).execute()

    def get_document(self, document_id: UUID) -> Optional[dict]:
        """Get a document by ID."""
        result = self.client.table("documents").select("*").eq("id", str(document_id)).execute()
        return result.data[0] if result.data else None

    def list_documents_by_status(
        self,
        status: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[dict]:
        """List documents by status."""
        result = (
            self.client.table("documents")
            .select("*")
            .eq("status", status)
            .order("created_at", desc=False)
            .limit(limit)
            .offset(offset)
            .execute()
        )
        return result.data if result.data else []

    def list_documents_ready_for_features(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> List[dict]:
        """List documents ready for feature extraction (status='ready' AND features_extracted=false)."""
        result = (
            self.client.table("documents")
            .select("*")
            .eq("status", "ready")
            .eq("features_extracted", False)
            .order("created_at", desc=False)
            .limit(limit)
            .offset(offset)
            .execute()
        )
        return result.data if result.data else []

    def insert_chunks(self, chunks: List[dict]) -> None:
        """Insert document chunks in batch."""
        if not chunks:
            return

        formatted_chunks = []
        for chunk in chunks:
            formatted_chunk = {
                "document_id": str(chunk["document_id"]),
                "chunk_index": chunk["chunk_index"],
                "content": chunk["content"],
                "embedding": chunk.get("embedding"),
                "token_count": chunk.get("token_count"),
                "metadata": chunk.get("metadata", {}),
            }
            formatted_chunks.append(formatted_chunk)

        self.client.table("document_chunks").insert(formatted_chunks).execute()

    def list_feeds(self, status: str = "active") -> List[dict]:
        """List active feeds."""
        result = (
            self.client.table("feeds")
            .select("*")
            .eq("status", status)
            .execute()
        )
        return result.data if result.data else []

    def get_feed(self, feed_id: UUID) -> Optional[dict]:
        """Get a feed by ID."""
        result = self.client.table("feeds").select("*").eq("id", str(feed_id)).execute()
        return result.data[0] if result.data else None

    def update_feed(
        self,
        feed_id: UUID,
        updates: dict,
    ) -> None:
        """Update a feed."""
        self.client.table("feeds").update(updates).eq("id", str(feed_id)).execute()

    def mark_features_extracted(self, document_id: UUID) -> None:
        """Mark document as having features extracted."""
        self.client.table("documents").update({"features_extracted": True}).eq("id", str(document_id)).execute()

    def create_company(self, name: str, normalized_name: str, aliases: Optional[List[str]] = None) -> dict:
        """Create or get company record."""
        # Check if exists
        result = self.client.table("companies").select("*").eq("normalized_name", normalized_name).execute()
        if result.data:
            return result.data[0]

        # Create new (handle duplicate key errors)
        try:
            result = (
                self.client.table("companies")
                .insert(
                    {
                        "name": name,
                        "normalized_name": normalized_name,
                        "aliases": aliases or [],
                    }
                )
                .execute()
            )
            return result.data[0] if result.data else {}
        except Exception as e:
            # If insert fails due to race condition, try to get again
            if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                result = self.client.table("companies").select("*").eq("normalized_name", normalized_name).execute()
                if result.data:
                    return result.data[0]
            raise

    def create_investor(self, name: str, normalized_name: str, aliases: Optional[List[str]] = None) -> dict:
        """Create or get investor record."""
        # Check if exists
        result = self.client.table("investors").select("*").eq("normalized_name", normalized_name).execute()
        if result.data:
            return result.data[0]

        # Create new (handle duplicate key errors)
        try:
            result = (
                self.client.table("investors")
                .insert(
                    {
                        "name": name,
                        "normalized_name": normalized_name,
                        "aliases": aliases or [],
                    }
                )
                .execute()
            )
            return result.data[0] if result.data else {}
        except Exception as e:
            # If insert fails due to race condition, try to get again
            if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                result = self.client.table("investors").select("*").eq("normalized_name", normalized_name).execute()
                if result.data:
                    return result.data[0]
            raise

    def create_funding_round(
        self,
        document_id: UUID,
        company_id: Optional[UUID],
        amount_usd: Optional[float],
        amount_original: Optional[float],
        currency: Optional[str],
        round_type: Optional[str],
        round_date: Optional[str],
        lead_investor_id: Optional[UUID],
        investor_ids: Optional[List[UUID]],
        metadata: Optional[dict] = None,
    ) -> dict:
        """Create a funding round record."""
        result = (
            self.client.table("funding_rounds")
            .insert(
                {
                    "document_id": str(document_id),
                    "company_id": str(company_id) if company_id else None,
                    "amount_usd": amount_usd,
                    "amount_original": amount_original,
                    "currency": currency,
                    "round_type": round_type,
                    "round_date": round_date,
                    "lead_investor_id": str(lead_investor_id) if lead_investor_id else None,
                    "investor_ids": [str(i) for i in investor_ids] if investor_ids else [],
                    "metadata": metadata or {},
                }
            )
            .execute()
        )
        return result.data[0] if result.data else {}

    def create_document_features(
        self,
        document_id: UUID,
        company_ids: Optional[List[UUID]],
        investor_ids: Optional[List[UUID]],
        funding_round_ids: Optional[List[UUID]],
        sectors: Optional[List[str]],
        keywords: Optional[List[str]],
        metadata: Optional[dict] = None,
    ) -> dict:
        """Create or update document features."""
        # Check if exists
        result = self.client.table("document_features").select("*").eq("document_id", str(document_id)).execute()
        
        data = {
            "document_id": str(document_id),
            "company_ids": [str(c) for c in company_ids] if company_ids else [],
            "investor_ids": [str(i) for i in investor_ids] if investor_ids else [],
            "funding_round_ids": [str(f) for f in funding_round_ids] if funding_round_ids else [],
            "sectors": sectors or [],
            "keywords": keywords or [],
            "metadata": metadata or {},
        }

        if result.data:
            # Update existing
            result = self.client.table("document_features").update(data).eq("document_id", str(document_id)).execute()
        else:
            # Create new
            result = self.client.table("document_features").insert(data).execute()

        return result.data[0] if result.data else {}


class LLMServiceClient:
    """HTTP client for LLM service embedding generation."""

    def __init__(self):
        """Initialize LLM service client."""
        self.base_url = settings.llm_service_url
        self.timeout = settings.llm_service_timeout
        self.batch_size = settings.embedding_batch_size
        self.max_retries = settings.embedding_max_retries
        self.retry_delay = settings.embedding_retry_delay

    async def generate_embedding(
        self,
        text: str,
        user_id: str = "ingestion-service",
        retry_count: int = 0,
    ) -> List[float]:
        """Generate embedding for a single text."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.post(
                    f"{self.base_url}/embeddings",
                    json={"text": text, "provider": "openai"},
                    headers={"X-User-ID": user_id},
                )
                response.raise_for_status()
                data = response.json()
                return data["embedding"]
            except Exception as e:
                if retry_count < self.max_retries:
                    await asyncio.sleep(self.retry_delay * (2 ** retry_count))
                    return await self.generate_embedding(text, user_id, retry_count + 1)
                raise ValueError(f"Failed to generate embedding after {self.max_retries} retries: {str(e)}") from e

    async def generate_embeddings_batch(
        self,
        texts: List[str],
        user_id: str = "ingestion-service",
    ) -> List[List[float]]:
        """Generate embeddings for multiple texts in batch."""
        if not texts:
            return []

        all_embeddings = []
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i : i + self.batch_size]
            tasks = [self.generate_embedding(text, user_id) for text in batch]
            batch_embeddings = await asyncio.gather(*tasks, return_exceptions=True)

            embeddings = []
            for emb in batch_embeddings:
                if isinstance(emb, Exception):
                    print(f"Error generating embedding: {emb}")
                    embeddings.append([0.0] * 1536)
                else:
                    embeddings.append(emb)

            all_embeddings.extend(embeddings)

            if i + self.batch_size < len(texts):
                await asyncio.sleep(0.1)

        return all_embeddings

