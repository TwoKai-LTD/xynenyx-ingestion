# Railway Quick Start

## ✅ Ready to Deploy!

All prerequisites are complete:
- ✅ Database migrations applied
- ✅ Code ready
- ✅ Railway configuration ready

## Step-by-Step Deployment

### 1. Connect Repository to Railway

1. Go to [Railway Dashboard](https://railway.app)
2. Click "New Project"
3. Select "Deploy from GitHub repo"
4. Choose the `xynenyx-ingestion` repository
5. Railway will automatically detect `railway.json` and create 3 cron jobs

### 2. Set Environment Variables

For each service (you can set them at the project level to share):

**Required:**
```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-role-key
LLM_SERVICE_URL=https://your-llm-service.railway.app
```

**Optional (defaults provided):**
```
BATCH_SIZE=10
LOG_LEVEL=info
SYSTEM_USER_ID=system-ingestion
```

**Note:** `WORKER_MODE` is set automatically by each cron job, don't set it manually.

### 3. Seed Initial Feeds

After deployment, run the feed seeding script locally (or via Railway CLI):

```bash
cd xynenyx-infra/scripts
export SUPABASE_URL=...
export SUPABASE_SERVICE_KEY=...
python seed-feeds.py
```

This will populate feeds like:
- TechCrunch
- Crunchbase News
- VentureBeat
- Product Hunt
- Hacker News
- Y Combinator Blog

### 4. Verify Deployment

1. **Check Railway Logs:**
   - Go to each cron job in Railway
   - View logs to see workers running
   - Look for structured JSON logs

2. **Check Supabase:**
   ```sql
   -- Check feeds are active
   SELECT * FROM feeds WHERE status = 'active';
   
   -- Check documents being created
   SELECT COUNT(*), status FROM documents GROUP BY status;
   
   -- Check feature store
   SELECT COUNT(*) FROM companies;
   SELECT COUNT(*) FROM investors;
   ```

3. **Monitor First Run:**
   - Ingestion worker runs hourly (next run at :00)
   - Processing worker runs every 15 minutes
   - Features worker runs every 15 minutes

## Expected Behavior

### First Hour
- **00:00** - Ingestion worker runs, creates documents with `status='pending'`
- **00:15** - Processing worker runs, processes pending documents
- **00:15** - Features worker runs, extracts features from ready documents
- **00:30** - Processing/Features workers continue processing

### Ongoing
- Documents flow: `pending` → `processing` → `ready` → `features_extracted=true`
- Feature store populates with companies, investors, funding rounds
- All workers run independently and can scale separately

## Troubleshooting

### No Documents Created
- ✅ Check feeds are seeded: `SELECT * FROM feeds`
- ✅ Check ingestion worker logs for RSS parsing errors
- ✅ Verify `LLM_SERVICE_URL` is accessible

### Processing Stuck
- ✅ Check LLM service is running
- ✅ Check document `error_message` field
- ✅ Verify embeddings endpoint is working

### Features Not Extracting
- ✅ Check documents have `status='ready'` and `features_extracted=false`
- ✅ Check features worker logs for extraction errors

## Next Steps

Once deployed and verified:
1. Monitor logs for first few runs
2. Adjust `BATCH_SIZE` if needed (based on processing time)
3. Add more feeds via `seed-feeds.py` or directly in Supabase
4. Monitor feature store growth

