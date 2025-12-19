"""Processing worker - chunking and embedding generation."""
import asyncio
import logging
import time
from typing import List, Dict, Any
from uuid import UUID
from app.shared.clients import SupabaseClient, LLMServiceClient
from app.shared.pipeline import Chunker
from app.config import settings

logger = logging.getLogger(__name__)


class ProcessingWorker:
    """Worker for processing documents (chunking and embedding)."""

    def __init__(self):
        """Initialize processing worker."""
        self.supabase_client = SupabaseClient()
        self.llm_client = LLMServiceClient()
        self.chunker = Chunker()
        self.batch_size = settings.batch_size

    async def run(self) -> Dict[str, Any]:
        """
        Run processing worker.

        Returns:
            Dictionary with processing results
        """
        start_time = time.time()
        logger.info("Starting processing worker...")

        # Get documents with status='pending'
        documents = self.supabase_client.list_documents_by_status("pending", limit=self.batch_size)
        logger.info(f"Found {len(documents)} documents to process")

        if not documents:
            logger.info("No documents to process")
            return {
                "status": "completed",
                "documents_processed": 0,
                "chunks_created": 0,
                "errors": 0,
                "duration_seconds": time.time() - start_time,
            }

        documents_processed = 0
        chunks_created = 0
        errors = 0

        for doc in documents:
            try:
                result = await self._process_document(doc)
                documents_processed += 1
                chunks_created += result.get("chunks_created", 0)
            except Exception as e:
                errors += 1
                logger.error(f"Error processing document {doc['id']}: {e}", exc_info=True)
                # Update document status to error
                self.supabase_client.update_document_status(
                    UUID(doc["id"]),
                    "error",
                    error_message=str(e),
                )

        duration = time.time() - start_time
        logger.info(
            f"Processing complete: {documents_processed} documents, {chunks_created} chunks, "
            f"{errors} errors, duration: {duration:.2f}s"
        )

        return {
            "status": "completed",
            "documents_processed": documents_processed,
            "chunks_created": chunks_created,
            "errors": errors,
            "duration_seconds": duration,
        }

    async def _process_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single document."""
        document_id = UUID(document["id"])
        metadata = document.get("metadata", {})
        raw_content = metadata.get("raw_content", "")

        if not raw_content:
            raise ValueError("No raw_content in document metadata")

        logger.debug(f"Processing document: {document_id}")

        # Update status to processing
        self.supabase_client.update_document_status(document_id, "processing")

        try:
            # Chunk content
            chunks = self.chunker.chunk_document(raw_content, metadata)

            if not chunks:
                raise ValueError("No chunks generated from content")

            logger.debug(f"Generated {len(chunks)} chunks for document {document_id}")

            # Generate embeddings
            chunk_texts = [chunk["content"] for chunk in chunks]
            embeddings = await self.llm_client.generate_embeddings_batch(
                chunk_texts,
                user_id=settings.system_user_id,
            )

            if len(embeddings) != len(chunks):
                raise ValueError(f"Embedding count mismatch: {len(embeddings)} != {len(chunks)}")

            # Prepare chunks for storage
            chunks_to_store = []
            for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                chunks_to_store.append({
                    "document_id": document_id,
                    "chunk_index": idx,
                    "content": chunk["content"],
                    "embedding": embedding,
                    "token_count": chunk["token_count"],
                    "metadata": chunk["metadata"],
                })

            # Store chunks
            self.supabase_client.insert_chunks(chunks_to_store)

            # Update document status to ready
            self.supabase_client.update_document_status(
                document_id,
                "ready",
                chunk_count=len(chunks),
            )

            logger.debug(f"Document {document_id} processed successfully: {len(chunks)} chunks")

            return {
                "chunks_created": len(chunks),
            }

        except Exception as e:
            logger.error(f"Error processing document {document_id}: {e}")
            self.supabase_client.update_document_status(
                document_id,
                "error",
                error_message=str(e),
            )
            raise

