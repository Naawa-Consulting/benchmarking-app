alter table if exists public.journey_metrics
  add column if not exists csat_imputed double precision,
  add column if not exists csat_source text,
  add column if not exists csat_impute_level text,
  add column if not exists csat_impute_version text;

alter table if exists public.journey_metrics
  drop constraint if exists ck_journey_csat_source;

alter table if exists public.journey_metrics
  add constraint ck_journey_csat_source
  check (
    csat_source is null
    or csat_source in ('observed', 'imputed', 'none')
  );

alter table if exists public.journey_metrics
  drop constraint if exists ck_journey_csat_impute_level;

alter table if exists public.journey_metrics
  add constraint ck_journey_csat_impute_level
  check (
    csat_impute_level is null
    or csat_impute_level in ('category', 'subsector', 'sector', 'global', 'none')
  );
