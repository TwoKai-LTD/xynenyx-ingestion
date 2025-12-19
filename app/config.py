"""Configuration settings for ingestion service."""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings."""

    # Supabase
    supabase_url: str
    supabase_service_key: str

    # LLM Service
    llm_service_url: str = "http://localhost:8003"
    llm_service_timeout: int = 60

    # Worker Configuration
    worker_mode: str = "ingestion"  # ingestion, processing, features
    batch_size: int = 10
    log_level: str = "info"

    # System User ID
    system_user_id: str = "system-ingestion"

    # RSS Ingestion Settings
    rss_request_timeout: int = 30
    rss_max_retries: int = 3

    # HTML Extraction Settings
    html_request_timeout: int = 30
    html_max_retries: int = 3
    html_user_agent: str = "Mozilla/5.0 (compatible; XynenyxBot/1.0)"

    # Chunking Settings
    chunk_size: int = 512
    chunk_overlap: int = 50

    # Embedding Settings
    embedding_batch_size: int = 10
    embedding_max_retries: int = 3
    embedding_retry_delay: float = 1.0

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()

