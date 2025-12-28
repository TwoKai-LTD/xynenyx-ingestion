# Data Cleanup Script

This script cleans up bad funding data that was created before the fixes were applied.

## What It Does

1. **Fixes Funding Amounts**: Multiplies amounts by 1,000,000 if they're clearly in millions (e.g., $8 -> $8,000,000)
2. **Deletes Invalid Funding Rounds**: Removes rounds with amounts that are too small and don't match expected patterns
3. **Deletes Bad Companies**: Removes companies with false positive names (e.g., "was caught in", "funding rounds fell")
4. **Marks Documents for Re-processing**: Sets `features_extracted=false` so the features worker will re-process them
5. **Optionally Re-runs Feature Extraction**: Can automatically re-run feature extraction after cleanup

## Usage

### Dry Run (Recommended First)

See what would be changed without making any changes:

```bash
cd xynenyx-ingestion
python scripts/cleanup_bad_data.py --dry-run
```

### Execute Cleanup

Actually perform the cleanup:

```bash
python scripts/cleanup_bad_data.py --execute
```

### Options

- `--dry-run` (default): Only show what would be done
- `--execute`: Actually perform the cleanup
- `--no-fix-amounts`: Skip fixing funding amounts
- `--no-delete-companies`: Skip deleting bad companies
- `--reprocess`: Re-run feature extraction after cleanup

### Examples

```bash
# Dry run to see what would be fixed
python scripts/cleanup_bad_data.py

# Fix amounts and delete bad companies, but don't re-process
python scripts/cleanup_bad_data.py --execute

# Full cleanup with re-processing
python scripts/cleanup_bad_data.py --execute --reprocess

# Only fix amounts, don't delete companies
python scripts/cleanup_bad_data.py --execute --no-delete-companies
```

## What Gets Fixed

### Funding Amounts
- Amounts < $10,000 that match the pattern of being in millions are multiplied by 1,000,000
- Example: `amount_usd: 8.0` -> `amount_usd: 8000000.0` (for "$8 million")

### Funding Rounds Deleted
- Rounds with amounts < $10,000 that don't match expected patterns
- Rounds with no `amount_original` value

### Companies Deleted
- Companies matching bad patterns: "was caught in", "funding rounds fell", "opportunities", etc.
- Companies with names < 3 characters
- Companies that are just common words: "the", "this", "that", etc.

## After Cleanup

After running the cleanup:

1. The features worker will automatically re-process documents (if `--reprocess` was used)
2. Or wait for the next features worker run (every 15 minutes on Railway)
3. New funding rounds will be created with correct amounts, dates, and company associations

## Safety

- The script is **safe by default** - it runs in dry-run mode unless you use `--execute`
- It backs up data by setting `company_id` to NULL in funding_rounds before deleting companies
- All operations are logged for audit purposes

