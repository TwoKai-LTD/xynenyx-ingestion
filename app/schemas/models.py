"""Pydantic models for ingestion service."""
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime


class FeedResponse(BaseModel):
    """Feed response model."""
    id: str
    name: str
    url: str
    update_frequency: str
    status: str
    last_ingested_at: Optional[datetime] = None
    article_count: int = 0


class DocumentResponse(BaseModel):
    """Document response model."""
    id: str
    user_id: str
    name: str
    status: str
    created_at: datetime
    features_extracted: bool = False

