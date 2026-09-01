-- One-time backfill for a labeling bug in services/api/app/routers/analytics.py
-- (_apply_consideration_imputation_to_rows): when brand_consideration could not be
-- estimated at all, the code left brand_consideration_source = 'observed' instead of
-- 'none' (the equivalent satisfaction/csat branches already used 'none' correctly).
-- Fixed in code alongside this migration. This backfill corrects the rows already
-- written to Postgres before the fix, so the rate-model training corpus
-- (see 024_rate_model_training_stats_rpc.sql, which filters on *_source = 'observed')
-- isn't trained on mislabeled nulls.

update public.journey_metrics
set brand_consideration_source = 'none'
where brand_consideration is null
  and brand_consideration_source = 'observed';
