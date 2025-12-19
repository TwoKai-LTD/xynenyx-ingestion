# Repository Description

## Short Description (for GitHub)
```
Production-grade data ingestion service for Xynenyx. Modular RSS feed ingestion, document processing, and feature extraction workers deployed as Railway cron jobs.
```

## Full Description

**Xynenyx Ingestion Service** - A production-grade data ingestion pipeline for the Xynenyx platform. This service provides modular, independently-scalable workers for:

- **RSS Feed Ingestion**: Hourly ingestion of startup/VC news feeds (TechCrunch, Crunchbase, etc.)
- **Document Processing**: Chunking and embedding generation for vector search
- **Feature Extraction**: Structured entity extraction (companies, investors, funding rounds) into a feature store

### Key Features

- **Modular Architecture**: Three independent workers (ingestion, processing, features) that communicate via database state machine
- **Stateless Workers**: Perfect for Railway cron jobs - no state management required
- **Database-Driven State Machine**: Simple, persistent, queryable workflow (`pending` → `processing` → `ready` → `features_extracted`)
- **Feature Store**: Structured tables for fast entity-based queries and relationship tracking
- **Production-Ready**: Structured logging, error handling, retry logic, idempotent operations
- **Railway-Optimized**: Pre-configured cron jobs with `railway.json`

### Architecture

Workers run independently as Railway cron jobs:
- **Ingestion Worker**: Runs hourly, creates documents with `status='pending'`
- **Processing Worker**: Runs every 15 minutes, chunks and embeds documents, sets `status='ready'`
- **Features Worker**: Runs every 15 minutes, extracts features, stores in feature tables

### Tech Stack

- Python 3.11+
- FastAPI (for future API endpoints)
- Supabase (database)
- LlamaIndex (chunking)
- BeautifulSoup (HTML extraction)
- Feedparser (RSS parsing)
- Railway (deployment)

