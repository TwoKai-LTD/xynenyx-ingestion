# Xynenyx Ingestion Service

Production-grade data ingestion service for Xynenyx with modular architecture: separate workers for RSS ingestion, document processing (chunking/embeddings), and feature extraction.

## Architecture

The service consists of three independent workers that run as separate Railway cron jobs:

1. **Ingestion Worker** - Runs hourly, ingests RSS feeds, creates documents with `status='pending'`
2. **Processing Worker** - Runs every 15 minutes, chunks documents and generates embeddings, updates `status='ready'`
3. **Features Worker** - Runs every 15 minutes, extracts features and stores in feature store tables

### State Machine

```
Document Status Flow:
pending → processing → ready → (features_extracted flag)

Ingestion Worker:
  - Finds active feeds
  - Creates documents with status='pending'
  
Processing Worker:
  - Finds documents with status='pending'
  - Sets status='processing'
  - Chunks + embeds
  - Sets status='ready'
  
Features Worker:
  - Finds documents with status='ready' AND features_extracted=false
  - Extracts features
  - Stores in feature tables
  - Sets features_extracted=true
```

## Local Development

### Prerequisites

- Python 3.11+
- Supabase credentials
- LLM service running (for embeddings)

### Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp env.example .env
# Edit .env with your credentials

# Run a worker
python -m app.main --mode=ingestion
python -m app.main --mode=processing
python -m app.main --mode=features
```

## Railway Deployment

The service is configured for Railway cron jobs via `railway.json`:

- **Ingestion**: Runs hourly (`0 * * * *`)
- **Processing**: Runs every 15 minutes (`*/15 * * * *`)
- **Features**: Runs every 15 minutes (`*/15 * * * *`)

### Environment Variables

Set these in Railway:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_KEY`
- `LLM_SERVICE_URL`
- `WORKER_MODE` (ingestion/processing/features)
- `BATCH_SIZE` (default: 10)
- `LOG_LEVEL` (default: info)
- `SYSTEM_USER_ID` (default: system-ingestion)

## Usage

### Manual Feed Seeding

Before running workers, seed initial feeds:

```bash
cd xynenyx-infra/scripts
python seed-feeds.py
```

### Running Workers

Each worker can be run independently:

```bash
# Ingestion worker
python -m app.main --mode=ingestion

# Processing worker
python -m app.main --mode=processing

# Features worker
python -m app.main --mode=features
```

## Monitoring

Workers log structured JSON logs with:
- Timestamp
- Log level
- Logger name
- Message

Monitor via Railway logs or your logging service.

## Error Handling

- Failed documents are marked with `status='error'`
- Workers can be safely re-run (idempotent)
- Retry logic for embedding generation
- Feed errors are tracked in feed status

## Feature Store

The features worker populates:
- `companies` - Normalized company names
- `investors` - Investor names
- `funding_rounds` - Funding round details
- `document_features` - Document-feature relationships

This enables fast entity-based queries and relationship tracking.

