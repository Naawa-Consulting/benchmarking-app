-- 030 — bbs_journey_table_multi: actually honor `response_mode`.
--
-- Why: the function built `rows` + `selection_rows` + `global_rows` in EVERY mode, and
-- `global_rows` is `base` (the whole table, with no sector/subsector/category filter).
-- Measured before this change: all three response_modes returned an identical
-- 2,741,710 bytes. The Journey page fires three calls per scope change
-- (benchmark_global / benchmark_selection / full), so its progressive-loading strategy
-- was paying ~8.2 MB and ~450 ms of DB time to receive the same payload three times.
--
-- What each caller actually reads (verified across apps/web/src):
--   benchmark_global    -> global_rows   (journey/page.tsx:888)
--   benchmark_selection -> rows          (journey/page.tsx:965)
--   full                -> rows          (journey/page.tsx:1039, ScopeProvider, Agent,
--                                         JourneyDataValidationTables)
-- Nothing reads `selection_rows`; it is kept in the response shape as an empty array so
-- the contract does not break.
--
-- The unused branches are gated inside the CTEs rather than with a CASE over the
-- aggregates, so Postgres never materializes the set the caller did not ask for.
-- Membership and ordering of the field each mode does read are unchanged.

create or replace function public.bbs_journey_table_multi(
  query jsonb default '{}'::jsonb,
  payload jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
stable
as $function$
declare
  v_response_mode text := lower(coalesce(query->>'response_mode', 'full'));
  v_sector text := nullif(coalesce(payload->>'sector', query->>'sector'), '');
  v_subsector text := nullif(coalesce(payload->>'subsector', query->>'subsector'), '');
  v_category text := nullif(coalesce(payload->>'category', query->>'category'), '');
  v_brands text[] := case
    when jsonb_typeof(payload->'brands') = 'array'
      then array(select jsonb_array_elements_text(payload->'brands'))
    else public.bbs_csv_to_array(query->>'brands')
  end;
  v_study_ids text[] := case
    when jsonb_typeof(payload->'study_ids') = 'array'
      then array(select jsonb_array_elements_text(payload->'study_ids'))
    else public.bbs_csv_to_array(query->>'study_ids')
  end;
  v_years int[] := case
    when jsonb_typeof(payload->'years') = 'array'
      then array(select (jsonb_array_elements_text(payload->'years'))::int)
    else array(select x::int from unnest(public.bbs_csv_to_array(query->>'years')) x where x ~ '^[0-9]{4}$')
  end;
  v_limit_mode text := lower(coalesce(query->>'limit_mode', 'all'));
  v_sort_by text := lower(coalesce(query->>'sort_by', 'brand_awareness'));
  v_sort_dir text := lower(coalesce(query->>'sort_dir', 'desc'));
begin
  return (
    with base as (
      select jm.*, coalesce(jm.year, public.bbs_year_from_study(jm.study_id)) as y
      from public.journey_metrics jm
      where (coalesce(array_length(v_study_ids,1),0)=0 or jm.study_id = any(v_study_ids))
        and (coalesce(array_length(v_years,1),0)=0 or coalesce(jm.year, public.bbs_year_from_study(jm.study_id)) = any(v_years))
    ),
    selection_rows as (
      select *
      from base
      where (v_sector is null or sector = v_sector)
        and (v_subsector is null or subsector = v_subsector)
        and (v_category is null or category = v_category)
    ),
    full_rows as (
      select *
      from selection_rows
      where (coalesce(array_length(v_brands,1),0)=0 or brand = any(v_brands))
    ),
    -- Only materialized when the caller asked for the global benchmark.
    global_out as (
      select * from base where v_response_mode = 'benchmark_global'
    ),
    -- Empty in benchmark_global mode: that caller reads global_rows, not rows.
    chosen as (
      select * from (
        select * from selection_rows where v_response_mode = 'benchmark_selection'
        union all
        select * from full_rows where v_response_mode not in ('benchmark_global','benchmark_selection')
      ) t
      order by
        case when v_sort_by='brand_awareness' and v_sort_dir='asc' then brand_awareness end asc nulls last,
        case when v_sort_by='brand_awareness' and v_sort_dir='desc' then brand_awareness end desc nulls last,
        case when v_sort_by='brand_consideration' and v_sort_dir='asc' then brand_consideration end asc nulls last,
        case when v_sort_by='brand_consideration' and v_sort_dir='desc' then brand_consideration end desc nulls last,
        case when v_sort_by='brand_purchase' and v_sort_dir='asc' then brand_purchase end asc nulls last,
        case when v_sort_by='brand_purchase' and v_sort_dir='desc' then brand_purchase end desc nulls last,
        brand asc
      limit case when v_limit_mode='top10' then 10 when v_limit_mode='top25' then 25 else 1000000 end
    )
    select jsonb_build_object(
      'rows', coalesce((select jsonb_agg(to_jsonb(chosen)) from chosen), '[]'::jsonb),
      'selection_rows', '[]'::jsonb,
      'global_rows', coalesce((select jsonb_agg(to_jsonb(global_out)) from global_out), '[]'::jsonb),
      'meta', jsonb_build_object('source','supabase','warning',null,'response_mode',v_response_mode)
    )
  );
end;
$function$;
