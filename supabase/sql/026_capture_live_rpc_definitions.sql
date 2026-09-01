-- Governance capture, not a change: these 4 functions already exist and run in
-- production (project oitnbifxxkxvjfjnpdvp) but their DDL was never checked into this
-- repo — the only trace of the gap was a stub for bbs_journey_table_multi in
-- 001_bbs_rpc_contract.sql (returns [] / "not implemented") and a similar undocumented
-- gap for bbs_touchpoints_table_multi/bbs_tracking_series/bbs_network. Recovered verbatim
-- via `select pg_get_functiondef(oid) from pg_proc where proname = '...'` on 2026-08-31
-- while investigating the benchmark calculation architecture (see BITACORA.md). Applying
-- this file again is a harmless no-op (`create or replace` of the exact live definition).
--
-- Known, pre-existing divergence worth flagging (not fixed here, out of scope for this
-- migration): bbs_tracking_series averages brand_awareness/brand_consideration/etc. with
-- a plain avg(), unweighted by base_n_population/aggregation_weight_n — the legacy Python
-- path in services/api/app/routers/analytics.py (_weighted_metric_average) DOES weight by
-- population. Also, bbs_network is a materially simpler reimplementation than the legacy
-- network.py: it has no secondary brand/touchpoint links, no percentile-based node size
-- scaling, and no conditional consideration|touchpoint probability.

create or replace function public.bbs_journey_table_multi(query jsonb default '{}'::jsonb, payload jsonb default '{}'::jsonb)
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
    global_rows as (
      select * from base
    ),
    chosen as (
      select * from (
        select * from global_rows where v_response_mode = 'benchmark_global'
        union all
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
      'selection_rows', coalesce((select jsonb_agg(to_jsonb(selection_rows)) from selection_rows), '[]'::jsonb),
      'global_rows', coalesce((select jsonb_agg(to_jsonb(global_rows)) from global_rows), '[]'::jsonb),
      'meta', jsonb_build_object('source','supabase','warning',null,'response_mode',v_response_mode)
    )
  );
end;
$function$;

create or replace function public.bbs_touchpoints_table_multi(query jsonb default '{}'::jsonb, payload jsonb default '{}'::jsonb)
returns jsonb
language plpgsql
stable
as $function$
declare
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
  v_sort_by text := lower(coalesce(query->>'sort_by', 'recall'));
  v_sort_dir text := lower(coalesce(query->>'sort_dir', 'desc'));
begin
  return (
    with filtered as (
      select
        tm.*,
        coalesce(tm.year, public.bbs_year_from_study(tm.study_id)) as y
      from public.touchpoint_metrics tm
      where (v_sector is null or tm.sector = v_sector)
        and (v_subsector is null or tm.subsector = v_subsector)
        and (v_category is null or tm.category = v_category)
        and (coalesce(array_length(v_brands,1),0)=0 or tm.brand = any(v_brands))
        and (coalesce(array_length(v_study_ids,1),0)=0 or tm.study_id = any(v_study_ids))
        and (coalesce(array_length(v_years,1),0)=0 or coalesce(tm.year, public.bbs_year_from_study(tm.study_id)) = any(v_years))
    ),
    ordered as (
      select * from filtered
      order by
        case when v_sort_by='recall' and v_sort_dir='asc' then recall end asc nulls last,
        case when v_sort_by='recall' and v_sort_dir='desc' then recall end desc nulls last,
        case when v_sort_by='consideration' and v_sort_dir='asc' then consideration end asc nulls last,
        case when v_sort_by='consideration' and v_sort_dir='desc' then consideration end desc nulls last,
        case when v_sort_by='purchase' and v_sort_dir='asc' then purchase end asc nulls last,
        case when v_sort_by='purchase' and v_sort_dir='desc' then purchase end desc nulls last,
        touchpoint asc, brand asc
    ),
    limited as (
      select * from ordered
      limit case when v_limit_mode='top10' then 10 when v_limit_mode='top25' then 25 else 1000000 end
    )
    select jsonb_build_object(
      'rows', coalesce(jsonb_agg(to_jsonb(limited)), '[]'::jsonb),
      'meta', jsonb_build_object('source','supabase','warning',null)
    )
    from limited
  );
end;
$function$;

create or replace function public.bbs_network(query jsonb default '{}'::jsonb, payload jsonb default '{}'::jsonb)
returns jsonb
language plpgsql
stable
as $function$
declare
  v_metric text := lower(coalesce(query->>'metric_mode','recall'));
  v_taxonomy_view text := case
    when lower(coalesce(payload->>'taxonomy_view', query->>'taxonomy_view', 'market')) = 'standard' then 'standard'
    else 'market'
  end;
  v_sector text := nullif(coalesce(payload->>'sector', query->>'sector'), '');
  v_subsector text := nullif(coalesce(payload->>'subsector', query->>'subsector'), '');
  v_category text := nullif(coalesce(payload->>'category', query->>'category'), '');
  v_brands text[] := case
    when jsonb_typeof(payload->'brands') = 'array' then array(select jsonb_array_elements_text(payload->'brands'))
    else public.bbs_csv_to_array(query->>'brands')
  end;
  v_study_ids text[] := case
    when jsonb_typeof(payload->'study_ids') = 'array' then array(select jsonb_array_elements_text(payload->'study_ids'))
    else public.bbs_csv_to_array(coalesce(query->>'study_ids', query->>'studies'))
  end;
  v_years int[] := case
    when jsonb_typeof(payload->'years') = 'array'
      then array(select (jsonb_array_elements_text(payload->'years'))::int)
    else array(select x::int from unnest(public.bbs_csv_to_array(query->>'years')) x where x ~ '^[0-9]{4}$')
  end;
begin
  return (
    with tm_base as (
      select
        tm.*,
        coalesce(tm.year, public.bbs_year_from_study(tm.study_id)) as y,
        case when v_taxonomy_view='market' then coalesce(nullif(trim(tm.market_sector), ''), public.bbs_market_sector(tm.sector, tm.subsector, tm.category)) else coalesce(tm.sector, 'Unassigned') end as tx_sector,
        case when v_taxonomy_view='market' then coalesce(nullif(trim(tm.market_subsector), ''), public.bbs_market_subsector(tm.sector, tm.subsector, tm.category)) else coalesce(tm.subsector, 'Unassigned') end as tx_subsector,
        case when v_taxonomy_view='market' then coalesce(nullif(trim(tm.market_category), ''), public.bbs_market_category(tm.sector, tm.subsector, tm.category)) else coalesce(tm.category, 'Unassigned') end as tx_category
      from public.touchpoint_metrics tm
      where (coalesce(array_length(v_brands,1),0)=0 or tm.brand = any(v_brands))
        and (coalesce(array_length(v_study_ids,1),0)=0 or tm.study_id = any(v_study_ids))
        and (coalesce(array_length(v_years,1),0)=0 or coalesce(tm.year, public.bbs_year_from_study(tm.study_id)) = any(v_years))
    ),
    tmf as (
      select * from tm_base
      where (v_sector is null or tx_sector = v_sector)
        and (v_subsector is null or tx_subsector = v_subsector)
        and (v_category is null or tx_category = v_category)
    ),
    jm_base as (
      select
        jm.*,
        coalesce(jm.year, public.bbs_year_from_study(jm.study_id)) as y,
        case when v_taxonomy_view='market' then coalesce(nullif(trim(jm.market_sector), ''), public.bbs_market_sector(jm.sector, jm.subsector, jm.category)) else coalesce(jm.sector, 'Unassigned') end as tx_sector,
        case when v_taxonomy_view='market' then coalesce(nullif(trim(jm.market_subsector), ''), public.bbs_market_subsector(jm.sector, jm.subsector, jm.category)) else coalesce(jm.subsector, 'Unassigned') end as tx_subsector,
        case when v_taxonomy_view='market' then coalesce(nullif(trim(jm.market_category), ''), public.bbs_market_category(jm.sector, jm.subsector, jm.category)) else coalesce(jm.category, 'Unassigned') end as tx_category
      from public.journey_metrics jm
      where (coalesce(array_length(v_brands,1),0)=0 or jm.brand = any(v_brands))
        and (coalesce(array_length(v_study_ids,1),0)=0 or jm.study_id = any(v_study_ids))
        and (coalesce(array_length(v_years,1),0)=0 or coalesce(jm.year, public.bbs_year_from_study(jm.study_id)) = any(v_years))
    ),
    jmf as (
      select * from jm_base
      where (v_sector is null or tx_sector = v_sector)
        and (v_subsector is null or tx_subsector = v_subsector)
        and (v_category is null or tx_category = v_category)
    ),
    links as (
      select
        touchpoint as source,
        brand as target,
        avg(recall)/100.0 as w_recall_raw,
        avg(consideration)/100.0 as w_consideration_raw,
        avg(purchase)/100.0 as w_purchase_raw,
        count(*)::double precision as n_base
      from tmf
      group by touchpoint, brand
    ),
    brand_nodes as (
      select
        brand as id,
        'brand'::text as type,
        brand as label,
        18::double precision as size,
        'brand'::text as "group",
        max(tx_sector) as sector,
        max(tx_subsector) as subsector,
        max(tx_category) as category,
        jsonb_build_object('kpi_awareness', avg(brand_awareness)) as colormeta
      from jmf
      group by brand
    ),
    tp_nodes as (
      select
        touchpoint as id,
        'touchpoint'::text as type,
        touchpoint as label,
        14::double precision as size,
        'touchpoint'::text as "group",
        null::text as sector,
        null::text as subsector,
        null::text as category,
        null::jsonb as colormeta
      from tmf
      group by touchpoint
    ),
    nodes as (
      select * from brand_nodes
      union all
      select * from tp_nodes
    )
    select jsonb_build_object(
      'ok', true,
      'metric', v_metric,
      'filters', coalesce(query,'{}'::jsonb),
      'nodes', coalesce((select jsonb_agg(jsonb_build_object(
        'id',id,'type',type,'label',label,'size',size,'group',"group",
        'sector',sector,'subsector',subsector,'category',category,'colorMeta',colormeta
      )) from nodes), '[]'::jsonb),
      'links', coalesce((select jsonb_agg(jsonb_build_object(
        'source',source,'target',target,'type','primary_tp_brand',
        'weight', case when v_metric='consideration' then coalesce(w_consideration_raw,0)
                       when v_metric='purchase' then coalesce(w_purchase_raw,0)
                       else coalesce(w_recall_raw,0) end,
        'w_recall_raw',w_recall_raw,
        'w_consideration_raw',w_consideration_raw,
        'w_purchase_raw',w_purchase_raw,
        'n_base',n_base
      )) from links), '[]'::jsonb),
      'meta', jsonb_build_object('source','supabase','warning',null,'cache_hit',false)
    )
  );
end;
$function$;

create or replace function public.bbs_tracking_series(query jsonb default '{}'::jsonb, payload jsonb default '{}'::jsonb)
returns jsonb
language plpgsql
stable
as $function$
declare
  v_taxonomy_view text := case
    when lower(coalesce(payload->>'taxonomy_view', query->>'taxonomy_view', 'market')) = 'standard' then 'standard'
    else 'market'
  end;
  v_sector text := nullif(coalesce(payload->>'sector', query->>'sector'), '');
  v_subsector text := nullif(coalesce(payload->>'subsector', query->>'subsector'), '');
  v_category text := nullif(coalesce(payload->>'category', query->>'category'), '');
  v_brands text[] := case
    when jsonb_typeof(payload->'brands') = 'array' then array(select jsonb_array_elements_text(payload->'brands'))
    else public.bbs_csv_to_array(query->>'brands')
  end;
  v_study_ids text[] := case
    when jsonb_typeof(payload->'study_ids') = 'array' then array(select jsonb_array_elements_text(payload->'study_ids'))
    else public.bbs_csv_to_array(query->>'study_ids')
  end;
  v_years int[] := case
    when jsonb_typeof(payload->'years') = 'array'
      then array(select (jsonb_array_elements_text(payload->'years'))::int)
    else array(select x::int from unnest(public.bbs_csv_to_array(query->>'years')) x where x ~ '^[0-9]{4}$')
  end;
  v_breakdown text;
begin
  if v_sector is null then
    v_breakdown := 'sector';
  elsif v_subsector is null then
    v_breakdown := 'subsector';
  elsif v_category is null then
    v_breakdown := 'category';
  else
    v_breakdown := 'brand';
  end if;

  return (
    with jb as (
      select
        jm.study_id,
        jm.brand,
        coalesce(jm.year, public.bbs_year_from_study(jm.study_id))::text as period_key,
        case when v_taxonomy_view='market' then coalesce(nullif(trim(jm.market_sector), ''), public.bbs_market_sector(jm.sector, jm.subsector, jm.category)) else coalesce(jm.sector, 'Unassigned') end as tx_sector,
        case when v_taxonomy_view='market' then coalesce(nullif(trim(jm.market_subsector), ''), public.bbs_market_subsector(jm.sector, jm.subsector, jm.category)) else coalesce(jm.subsector, 'Unassigned') end as tx_subsector,
        case when v_taxonomy_view='market' then coalesce(nullif(trim(jm.market_category), ''), public.bbs_market_category(jm.sector, jm.subsector, jm.category)) else coalesce(jm.category, 'Unassigned') end as tx_category,
        jm.brand_awareness, jm.ad_awareness, jm.brand_consideration, jm.brand_purchase, jm.brand_satisfaction, jm.brand_recommendation, jm.csat, jm.nps
      from public.journey_metrics jm
      where (coalesce(array_length(v_study_ids,1),0)=0 or jm.study_id = any(v_study_ids))
        and (coalesce(array_length(v_years,1),0)=0 or coalesce(jm.year, public.bbs_year_from_study(jm.study_id)) = any(v_years))
    ),
    jb_filtered as (
      select *,
        case
          when v_breakdown='sector' then tx_sector
          when v_breakdown='subsector' then tx_subsector
          when v_breakdown='category' then tx_category
          else coalesce(brand, 'Unassigned')
        end as entity_name
      from jb
      where (v_sector is null or tx_sector = v_sector)
        and (v_subsector is null or tx_subsector = v_subsector)
        and (v_category is null or tx_category = v_category)
        and (v_breakdown <> 'brand' or coalesce(array_length(v_brands,1),0)=0 or brand = any(v_brands))
    ),
    tb as (
      select
        tm.study_id,
        tm.touchpoint,
        coalesce(tm.year, public.bbs_year_from_study(tm.study_id))::text as period_key,
        case when v_taxonomy_view='market' then coalesce(nullif(trim(tm.market_sector), ''), public.bbs_market_sector(tm.sector, tm.subsector, tm.category)) else coalesce(tm.sector, 'Unassigned') end as tx_sector,
        case when v_taxonomy_view='market' then coalesce(nullif(trim(tm.market_subsector), ''), public.bbs_market_subsector(tm.sector, tm.subsector, tm.category)) else coalesce(tm.subsector, 'Unassigned') end as tx_subsector,
        case when v_taxonomy_view='market' then coalesce(nullif(trim(tm.market_category), ''), public.bbs_market_category(tm.sector, tm.subsector, tm.category)) else coalesce(tm.category, 'Unassigned') end as tx_category,
        tm.brand, tm.recall, tm.consideration, tm.purchase
      from public.touchpoint_metrics tm
      where (coalesce(array_length(v_study_ids,1),0)=0 or tm.study_id = any(v_study_ids))
        and (coalesce(array_length(v_years,1),0)=0 or coalesce(tm.year, public.bbs_year_from_study(tm.study_id)) = any(v_years))
    ),
    tb_filtered as (
      select * from tb
      where (v_sector is null or tx_sector = v_sector)
        and (v_subsector is null or tx_subsector = v_subsector)
        and (v_category is null or tx_category = v_category)
        and (v_breakdown <> 'brand' or coalesce(array_length(v_brands,1),0)=0 or brand = any(v_brands))
    ),
    periods as (
      select period_key, min(period_key::int) as ord
      from (
        select period_key from jb_filtered
        union all
        select period_key from tb_filtered
      ) p
      group by period_key
      order by ord
    ),
    delta_columns as (
      select
        'd_' || lag(period_key) over(order by ord) || '_' || period_key as key,
        lag(period_key) over(order by ord) as from_key,
        period_key as to_key,
        lag(period_key) over(order by ord) || ' -> ' || period_key as label
      from periods
    ),
    delta_clean as (
      select * from delta_columns where from_key is not null
    ),
    bp as (
      select
        entity_name,
        period_key,
        avg(brand_awareness) as brand_awareness,
        avg(ad_awareness) as ad_awareness,
        avg(brand_consideration) as brand_consideration,
        avg(brand_purchase) as brand_purchase,
        avg(brand_satisfaction) as brand_satisfaction,
        avg(brand_recommendation) as brand_recommendation,
        avg(csat) as csat,
        avg(nps) as nps
      from jb_filtered
      group by entity_name, period_key
    ),
    bm as (
      select
        entity_name,
        jsonb_build_object(
          'brand_awareness', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, brand_awareness) from bp b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((select jsonb_object_agg(dc.key, (bto.brand_awareness - bfrom.brand_awareness)) from delta_clean dc left join bp bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key left join bp bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key), '{}'::jsonb)
          ),
          'ad_awareness', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, ad_awareness) from bp b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((select jsonb_object_agg(dc.key, (bto.ad_awareness - bfrom.ad_awareness)) from delta_clean dc left join bp bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key left join bp bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key), '{}'::jsonb)
          ),
          'brand_consideration', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, brand_consideration) from bp b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((select jsonb_object_agg(dc.key, (bto.brand_consideration - bfrom.brand_consideration)) from delta_clean dc left join bp bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key left join bp bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key), '{}'::jsonb)
          ),
          'brand_purchase', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, brand_purchase) from bp b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((select jsonb_object_agg(dc.key, (bto.brand_purchase - bfrom.brand_purchase)) from delta_clean dc left join bp bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key left join bp bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key), '{}'::jsonb)
          ),
          'brand_satisfaction', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, brand_satisfaction) from bp b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((select jsonb_object_agg(dc.key, (bto.brand_satisfaction - bfrom.brand_satisfaction)) from delta_clean dc left join bp bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key left join bp bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key), '{}'::jsonb)
          ),
          'brand_recommendation', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, brand_recommendation) from bp b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((select jsonb_object_agg(dc.key, (bto.brand_recommendation - bfrom.brand_recommendation)) from delta_clean dc left join bp bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key left join bp bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key), '{}'::jsonb)
          ),
          'csat', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, csat) from bp b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((select jsonb_object_agg(dc.key, (bto.csat - bfrom.csat)) from delta_clean dc left join bp bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key left join bp bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key), '{}'::jsonb)
          ),
          'nps', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, nps) from bp b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((select jsonb_object_agg(dc.key, (bto.nps - bfrom.nps)) from delta_clean dc left join bp bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key left join bp bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key), '{}'::jsonb)
          )
        ) as metrics
      from bp b1
      group by entity_name
    ),
    tp as (
      select touchpoint, period_key, avg(recall) as recall, avg(consideration) as consideration, avg(purchase) as purchase
      from tb_filtered
      group by touchpoint, period_key
    ),
    tm as (
      select
        touchpoint,
        jsonb_build_object(
          'recall', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, recall) from tp t2 where t2.touchpoint=t1.touchpoint), '{}'::jsonb),
            'deltas', coalesce((select jsonb_object_agg(dc.key, (tto.recall - tfrom.recall)) from delta_clean dc left join tp tfrom on tfrom.touchpoint=t1.touchpoint and tfrom.period_key=dc.from_key left join tp tto on tto.touchpoint=t1.touchpoint and tto.period_key=dc.to_key), '{}'::jsonb)
          ),
          'consideration', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, consideration) from tp t2 where t2.touchpoint=t1.touchpoint), '{}'::jsonb),
            'deltas', coalesce((select jsonb_object_agg(dc.key, (tto.consideration - tfrom.consideration)) from delta_clean dc left join tp tfrom on tfrom.touchpoint=t1.touchpoint and tfrom.period_key=dc.from_key left join tp tto on tto.touchpoint=t1.touchpoint and tto.period_key=dc.to_key), '{}'::jsonb)
          ),
          'purchase', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, purchase) from tp t2 where t2.touchpoint=t1.touchpoint), '{}'::jsonb),
            'deltas', coalesce((select jsonb_object_agg(dc.key, (tto.purchase - tfrom.purchase)) from delta_clean dc left join tp tfrom on tfrom.touchpoint=t1.touchpoint and tfrom.period_key=dc.from_key left join tp tto on tto.touchpoint=t1.touchpoint and tto.period_key=dc.to_key), '{}'::jsonb)
          )
        ) as metrics
      from tp t1
      group by touchpoint
    )
    select jsonb_build_object(
      'ok', true,
      'resolved_granularity', 'year',
      'resolved_breakdown', v_breakdown,
      'entity_label',
        case
          when v_breakdown='sector' then case when v_taxonomy_view='market' then 'Macrosector' else 'Sector' end
          when v_breakdown='subsector' then case when v_taxonomy_view='market' then 'Segmento' else 'Subsector' end
          when v_breakdown='category' then case when v_taxonomy_view='market' then 'Categoría comercial' else 'Category' end
          else 'Brand'
        end,
      'periods', coalesce((select jsonb_agg(jsonb_build_object('key', period_key, 'label', period_key, 'order', (period_key::int * 10)) order by ord) from periods), '[]'::jsonb),
      'delta_columns', coalesce((select jsonb_agg(jsonb_build_object('key', key, 'from', from_key, 'to', to_key, 'label', label)) from delta_clean), '[]'::jsonb),
      'entity_rows', coalesce((select jsonb_agg(jsonb_build_object('entity', entity_name, 'metrics', metrics) order by entity_name) from bm), '[]'::jsonb),
      'secondary_rows', coalesce((select jsonb_agg(jsonb_build_object('entity', touchpoint, 'metrics', metrics) order by touchpoint) from tm), '[]'::jsonb),
      'brand_rows', '[]'::jsonb,
      'touchpoint_rows', '[]'::jsonb,
      'metric_meta_brand', jsonb_build_object(
        'brand_awareness', jsonb_build_object('label','Brand Awareness','unit','%'),
        'ad_awareness', jsonb_build_object('label','Ad Awareness','unit','%'),
        'brand_consideration', jsonb_build_object('label','Brand Consideration','unit','%'),
        'brand_purchase', jsonb_build_object('label','Brand Purchase','unit','%'),
        'brand_satisfaction', jsonb_build_object('label','Brand Satisfaction','unit','%'),
        'brand_recommendation', jsonb_build_object('label','Brand Recommendation','unit','%'),
        'csat', jsonb_build_object('label','CSAT','unit','%'),
        'nps', jsonb_build_object('label','NPS','unit','%')
      ),
      'metric_meta_touchpoint', jsonb_build_object(
        'recall', jsonb_build_object('label','Recall','unit','%'),
        'consideration', jsonb_build_object('label','Consideration','unit','%'),
        'purchase', jsonb_build_object('label','Purchase','unit','%')
      ),
      'meta', jsonb_build_object('source', 'supabase', 'warning', null)
    )
  );
end;
$function$;
