-- Add Brand Consideration imputation traceability fields for journey snapshot rows.

alter table if exists public.journey_metrics
  add column if not exists brand_consideration_imputed double precision,
  add column if not exists brand_consideration_source text,
  add column if not exists brand_consideration_impute_level text,
  add column if not exists brand_consideration_impute_version text;

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'journey_metrics'
      and column_name = 'brand_consideration_source'
  ) and not exists (
    select 1
    from pg_constraint
    where conname = 'ck_journey_consideration_source'
  ) then
    execute $sql$
      alter table public.journey_metrics
      add constraint ck_journey_consideration_source
      check (brand_consideration_source in ('observed', 'imputed', 'none') or brand_consideration_source is null)
    $sql$;
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'journey_metrics'
      and column_name = 'brand_consideration_impute_level'
  ) and not exists (
    select 1
    from pg_constraint
    where conname = 'ck_journey_consideration_impute_level'
  ) then
    execute $sql$
      alter table public.journey_metrics
      add constraint ck_journey_consideration_impute_level
      check (
        brand_consideration_impute_level in ('category', 'subsector', 'sector', 'global', 'none')
        or brand_consideration_impute_level is null
      )
    $sql$;
  end if;
end
$$;
