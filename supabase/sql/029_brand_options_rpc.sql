-- 029 — bbs_brand_options: lightweight brand list for the global Scope Bar.
--
-- Why: ScopeProvider populated the Brand dropdown by running a FULL touchpoints
-- aggregation (`bbs_touchpoints_table_multi` with limit_mode="all") and discarding
-- everything except `row.brand` — 5.01 MB of JSONB per filter change, measured. When
-- touchpoint_metrics had no rows for the selection (true for 21 of 51 studies today,
-- including every LMB wave) it then chained a second full scan of
-- `bbs_journey_table_multi` (another 2.74 MB). See BITACORA.md 2026-09-01 (cont. 9).
--
-- This returns the distinct (brand × taxonomy) tuples only — 354 rows / 82.7 kB worst
-- case, ~61x smaller than the touchpoints scan alone — and unions both metric tables so
-- the journey fallback is no longer needed. Taxonomy columns are returned because the
-- Next layer resolves the market lens and applies the market-view selection filter
-- itself (same post-hoc filtering that journey/table_multi/route.ts already does), so
-- this function deliberately filters only on study_ids and years.

create or replace function public.bbs_brand_options(
  query jsonb default '{}'::jsonb,
  payload jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
stable
as $function$
declare
  v_study_ids text[] := case
    when jsonb_typeof(payload->'study_ids') = 'array'
      then array(select jsonb_array_elements_text(payload->'study_ids'))
    else public.bbs_csv_to_array(coalesce(query->>'study_ids', query->>'studies'))
  end;
  v_years int[] := case
    when jsonb_typeof(payload->'years') = 'array'
      then array(select (jsonb_array_elements_text(payload->'years'))::int)
    else array(select x::int from unnest(public.bbs_csv_to_array(query->>'years')) x where x ~ '^[0-9]{4}$')
  end;
begin
  return (
    with j as (
      select distinct
        jm.brand, jm.sector, jm.subsector, jm.category,
        jm.market_sector, jm.market_subsector, jm.market_category
      from public.journey_metrics jm
      where (coalesce(array_length(v_study_ids,1),0)=0 or jm.study_id = any(v_study_ids))
        and (coalesce(array_length(v_years,1),0)=0
             or coalesce(jm.year, public.bbs_year_from_study(jm.study_id)) = any(v_years))
        and jm.brand is not null and btrim(jm.brand) <> ''
    ),
    t as (
      select distinct
        tm.brand, tm.sector, tm.subsector, tm.category,
        tm.market_sector, tm.market_subsector, tm.market_category
      from public.touchpoint_metrics tm
      where (coalesce(array_length(v_study_ids,1),0)=0 or tm.study_id = any(v_study_ids))
        and (coalesce(array_length(v_years,1),0)=0
             or coalesce(tm.year, public.bbs_year_from_study(tm.study_id)) = any(v_years))
        and tm.brand is not null and btrim(tm.brand) <> ''
    ),
    u as (
      select * from j
      union
      select * from t
    )
    select jsonb_build_object(
      'items', coalesce((select jsonb_agg(to_jsonb(u) order by u.brand) from u), '[]'::jsonb),
      'meta', jsonb_build_object('source','supabase')
    )
  );
end;
$function$;
