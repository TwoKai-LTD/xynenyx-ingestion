# Railway Deployment Guide

## Prerequisites

### 1. Database Migrations ✅

**COMPLETED** - All migrations have been applied:
- ✅ **009_feeds** - Created feeds table
- ✅ **011_feeds_anonymous** - Updated feeds table for anonymous users
- ✅ **012_feature_store** - Created feature store tables (companies, investors, funding_rounds, document_features)

### 2. Seed Initial Feeds

After migrations are applied, seed the initial RSS feeds:

```bash
cd xynenyx-infra/scripts
python seed-feeds.py
```

This will populate feeds like TechCrunch, Crunchbase, etc.

## Railway Setup

### Step 1: Connect Repository

1. Go to Railway dashboard
2. Create a new project
3. Connect the `xynenyx-ingestion` repository
4. Railway will detect the `railway.json` configuration

### Step 2: Set Environment Variables

Set these environment variables in Railway (for each service/cron job):

**Required:**
- `SUPABASE_URL` - Your Supabase project URL
- `SUPABASE_SERVICE_KEY` - Your Supabase service role key
- `LLM_SERVICE_URL` - URL of your LLM service (e.g., `https://your-llm-service.railway.app`)

**Optional (with defaults):**
- `WORKER_MODE` - Set automatically by cron jobs (ingestion/processing/features)
- `BATCH_SIZE` - Default: `10`
- `LOG_LEVEL` - Default: `info`
- `SYSTEM_USER_ID` - Default: `system-ingestion`

### Step 3: Configure Cron Jobs

Railway will automatically create three cron jobs based on `railway.json`:

1. **ingestion-worker** - Runs hourly (`0 * * * *`)
2. **processing-worker** - Runs every 15 minutes (`*/15 * * * *`)
3. **features-worker** - Runs every 15 minutes (`*/15 * * * *`)

Each cron job will:
- Build the Docker image
- Run the worker with the appropriate `--mode` flag
- Log output to Railway logs

### Step 4: Verify Deployment

1. Check Railway logs for each cron job
2. Verify workers are running successfully
3. Check Supabase to see documents being created:
   ```sql
   SELECT COUNT(*), status FROM documents GROUP BY status;
   ```

## Monitoring

- **Railway Logs**: View structured JSON logs for each cron job
- **Supabase**: Monitor document creation and processing
- **Feed Status**: Check `feeds` table for `last_ingested_at` timestamps

## Troubleshooting

### Workers Not Running

- Check environment variables are set correctly
- Verify database migrations are applied
- Check Railway logs for errors

### No Documents Being Created

- Verify feeds are seeded (`SELECT * FROM feeds WHERE status = 'active'`)
- Check ingestion worker logs for RSS parsing errors
- Verify `LLM_SERVICE_URL` is accessible from Railway

### Processing Errors

- Check LLM service is running and accessible
- Verify embedding generation is working
- Check document `error_message` field in Supabase

## Manual Testing

You can test workers locally before deploying:

```bash
# Set environment variables
export SUPABASE_URL=...
export SUPABASE_SERVICE_KEY=...
export LLM_SERVICE_URL=...

# Run workers
python -m app.main --mode=ingestion
python -m app.main --mode=processing
python -m app.main --mode=features
```

