alter table if exists public.journey_metrics
  add column if not exists brand_satisfaction_imputed double precision,
  add column if not exists brand_satisfaction_source text,
  add column if not exists brand_satisfaction_impute_level text,
  add column if not exists brand_satisfaction_impute_version text;

alter table if exists public.journey_metrics
  drop constraint if exists ck_journey_satisfaction_source;

alter table if exists public.journey_metrics
  add constraint ck_journey_satisfaction_source
  check (
    brand_satisfaction_source is null
    or brand_satisfaction_source in ('observed', 'imputed', 'none')
  );

alter table if exists public.journey_metrics
  drop constraint if exists ck_journey_satisfaction_impute_level;

alter table if exists public.journey_metrics
  add constraint ck_journey_satisfaction_impute_level
  check (
    brand_satisfaction_impute_level is null
    or brand_satisfaction_impute_level in ('category', 'subsector', 'sector', 'global', 'none')
  );
