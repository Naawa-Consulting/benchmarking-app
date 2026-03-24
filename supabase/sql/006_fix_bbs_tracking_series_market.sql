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
  v_breakdown text;
  v_granularity text;
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

  with base_years as (
    select distinct coalesce(jm.year, public.bbs_year_from_study(jm.study_id)) as y
    from public.journey_metrics jm
    where (v_sector is null or
      (case when v_taxonomy_view='market' then coalesce(jm.market_sector, jm.sector, 'Unassigned') else coalesce(jm.sector, 'Unassigned') end) = v_sector)
      and (v_subsector is null or
      (case when v_taxonomy_view='market' then coalesce(jm.market_subsector, jm.subsector, 'Unassigned') else coalesce(jm.subsector, 'Unassigned') end) = v_subsector)
      and (v_category is null or
      (case when v_taxonomy_view='market' then coalesce(jm.market_category, jm.category, 'Unassigned') else coalesce(jm.category, 'Unassigned') end) = v_category)
      and (coalesce(array_length(v_study_ids,1),0)=0 or jm.study_id = any(v_study_ids))
      and (coalesce(array_length(v_years,1),0)=0 or coalesce(jm.year, public.bbs_year_from_study(jm.study_id)) = any(v_years))
  )
  select case when count(*) <= 1 then 'quarter' else 'year' end
  into v_granularity
  from base_years;

  return (
    with journey_base as (
      select
        jm.*,
        coalesce(jm.year, public.bbs_year_from_study(jm.study_id)) as y,
        case
          when v_taxonomy_view='market' then coalesce(jm.market_sector, jm.sector, 'Unassigned')
          else coalesce(jm.sector, 'Unassigned')
        end as tx_sector,
        case
          when v_taxonomy_view='market' then coalesce(jm.market_subsector, jm.subsector, 'Unassigned')
          else coalesce(jm.subsector, 'Unassigned')
        end as tx_subsector,
        case
          when v_taxonomy_view='market' then coalesce(jm.market_category, jm.category, 'Unassigned')
          else coalesce(jm.category, 'Unassigned')
        end as tx_category,
        case
          when v_breakdown='sector' then
            case when v_taxonomy_view='market' then coalesce(jm.market_sector, jm.sector, 'Unassigned') else coalesce(jm.sector, 'Unassigned') end
          when v_breakdown='subsector' then
            case when v_taxonomy_view='market' then coalesce(jm.market_subsector, jm.subsector, 'Unassigned') else coalesce(jm.subsector, 'Unassigned') end
          when v_breakdown='category' then
            case when v_taxonomy_view='market' then coalesce(jm.market_category, jm.category, 'Unassigned') else coalesce(jm.category, 'Unassigned') end
          else coalesce(jm.brand, 'Unassigned')
        end as entity_name,
        case
          when v_granularity='year'
            then coalesce(jm.year, public.bbs_year_from_study(jm.study_id))::text
          else
            coalesce(
              substring(jm.study_id from '(Q[1-4])'),
              coalesce(jm.year, public.bbs_year_from_study(jm.study_id))::text
            )
        end as period_key,
        case
          when v_granularity='year' then coalesce(jm.year, public.bbs_year_from_study(jm.study_id)) * 10
          else
            coalesce(jm.year, public.bbs_year_from_study(jm.study_id)) * 10 +
            case substring(jm.study_id from '(Q[1-4])')
              when 'Q1' then 1
              when 'Q2' then 2
              when 'Q3' then 3
              when 'Q4' then 4
              else 0
            end
        end as period_order
      from public.journey_metrics jm
      where (v_sector is null or
          (case when v_taxonomy_view='market' then coalesce(jm.market_sector, jm.sector, 'Unassigned') else coalesce(jm.sector, 'Unassigned') end) = v_sector)
        and (v_subsector is null or
          (case when v_taxonomy_view='market' then coalesce(jm.market_subsector, jm.subsector, 'Unassigned') else coalesce(jm.subsector, 'Unassigned') end) = v_subsector)
        and (v_category is null or
          (case when v_taxonomy_view='market' then coalesce(jm.market_category, jm.category, 'Unassigned') else coalesce(jm.category, 'Unassigned') end) = v_category)
        and (coalesce(array_length(v_study_ids,1),0)=0 or jm.study_id = any(v_study_ids))
        and (coalesce(array_length(v_years,1),0)=0 or coalesce(jm.year, public.bbs_year_from_study(jm.study_id)) = any(v_years))
        and (
          v_breakdown <> 'brand'
          or coalesce(array_length(v_brands,1),0)=0
          or jm.brand = any(v_brands)
        )
    ),
    touchpoint_base as (
      select
        tm.*,
        coalesce(tm.year, public.bbs_year_from_study(tm.study_id)) as y,
        case
          when v_breakdown='sector' then
            case when v_taxonomy_view='market' then coalesce(tm.market_sector, tm.sector, 'Unassigned') else coalesce(tm.sector, 'Unassigned') end
          when v_breakdown='subsector' then
            case when v_taxonomy_view='market' then coalesce(tm.market_subsector, tm.subsector, 'Unassigned') else coalesce(tm.subsector, 'Unassigned') end
          when v_breakdown='category' then
            case when v_taxonomy_view='market' then coalesce(tm.market_category, tm.category, 'Unassigned') else coalesce(tm.category, 'Unassigned') end
          else coalesce(tm.brand, 'Unassigned')
        end as entity_name,
        case
          when v_granularity='year'
            then coalesce(tm.year, public.bbs_year_from_study(tm.study_id))::text
          else
            coalesce(
              substring(tm.study_id from '(Q[1-4])'),
              coalesce(tm.year, public.bbs_year_from_study(tm.study_id))::text
            )
        end as period_key,
        case
          when v_granularity='year' then coalesce(tm.year, public.bbs_year_from_study(tm.study_id)) * 10
          else
            coalesce(tm.year, public.bbs_year_from_study(tm.study_id)) * 10 +
            case substring(tm.study_id from '(Q[1-4])')
              when 'Q1' then 1
              when 'Q2' then 2
              when 'Q3' then 3
              when 'Q4' then 4
              else 0
            end
        end as period_order
      from public.touchpoint_metrics tm
      where (v_sector is null or
          (case when v_taxonomy_view='market' then coalesce(tm.market_sector, tm.sector, 'Unassigned') else coalesce(tm.sector, 'Unassigned') end) = v_sector)
        and (v_subsector is null or
          (case when v_taxonomy_view='market' then coalesce(tm.market_subsector, tm.subsector, 'Unassigned') else coalesce(tm.subsector, 'Unassigned') end) = v_subsector)
        and (v_category is null or
          (case when v_taxonomy_view='market' then coalesce(tm.market_category, tm.category, 'Unassigned') else coalesce(tm.category, 'Unassigned') end) = v_category)
        and (coalesce(array_length(v_study_ids,1),0)=0 or tm.study_id = any(v_study_ids))
        and (coalesce(array_length(v_years,1),0)=0 or coalesce(tm.year, public.bbs_year_from_study(tm.study_id)) = any(v_years))
        and (
          v_breakdown <> 'brand'
          or coalesce(array_length(v_brands,1),0)=0
          or tm.brand = any(v_brands)
        )
    ),
    periods as (
      select period_key as key, period_key as label, min(period_order) as ord
      from journey_base
      group by period_key
      union
      select period_key as key, period_key as label, min(period_order) as ord
      from touchpoint_base
      group by period_key
    ),
    periods_dedup as (
      select key, label, min(ord) as ord
      from periods
      group by key, label
    ),
    periods_ordered as (
      select key, label, ord
      from periods_dedup
      order by ord, key
    ),
    delta_columns as (
      select
        'd_' || lag(key) over (order by ord, key) || '_' || key as key,
        lag(key) over (order by ord, key) as from_key,
        key as to_key,
        lag(label) over (order by ord, key) || ' -> ' || label as label
      from periods_ordered
    ),
    delta_columns_clean as (
      select * from delta_columns where from_key is not null
    ),
    brand_period_agg as (
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
      from journey_base
      group by entity_name, period_key
    ),
    brand_metrics_json as (
      select
        entity_name,
        jsonb_build_object(
          'brand_awareness', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, brand_awareness) from brand_period_agg b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((
              select jsonb_object_agg(dc.key, (bto.brand_awareness - bfrom.brand_awareness))
              from delta_columns_clean dc
              left join brand_period_agg bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key
              left join brand_period_agg bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key
            ), '{}'::jsonb)
          ),
          'ad_awareness', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, ad_awareness) from brand_period_agg b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((
              select jsonb_object_agg(dc.key, (bto.ad_awareness - bfrom.ad_awareness))
              from delta_columns_clean dc
              left join brand_period_agg bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key
              left join brand_period_agg bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key
            ), '{}'::jsonb)
          ),
          'brand_consideration', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, brand_consideration) from brand_period_agg b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((
              select jsonb_object_agg(dc.key, (bto.brand_consideration - bfrom.brand_consideration))
              from delta_columns_clean dc
              left join brand_period_agg bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key
              left join brand_period_agg bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key
            ), '{}'::jsonb)
          ),
          'brand_purchase', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, brand_purchase) from brand_period_agg b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((
              select jsonb_object_agg(dc.key, (bto.brand_purchase - bfrom.brand_purchase))
              from delta_columns_clean dc
              left join brand_period_agg bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key
              left join brand_period_agg bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key
            ), '{}'::jsonb)
          ),
          'brand_satisfaction', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, brand_satisfaction) from brand_period_agg b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((
              select jsonb_object_agg(dc.key, (bto.brand_satisfaction - bfrom.brand_satisfaction))
              from delta_columns_clean dc
              left join brand_period_agg bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key
              left join brand_period_agg bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key
            ), '{}'::jsonb)
          ),
          'brand_recommendation', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, brand_recommendation) from brand_period_agg b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((
              select jsonb_object_agg(dc.key, (bto.brand_recommendation - bfrom.brand_recommendation))
              from delta_columns_clean dc
              left join brand_period_agg bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key
              left join brand_period_agg bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key
            ), '{}'::jsonb)
          ),
          'csat', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, csat) from brand_period_agg b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((
              select jsonb_object_agg(dc.key, (bto.csat - bfrom.csat))
              from delta_columns_clean dc
              left join brand_period_agg bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key
              left join brand_period_agg bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key
            ), '{}'::jsonb)
          ),
          'nps', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, nps) from brand_period_agg b2 where b2.entity_name=b1.entity_name), '{}'::jsonb),
            'deltas', coalesce((
              select jsonb_object_agg(dc.key, (bto.nps - bfrom.nps))
              from delta_columns_clean dc
              left join brand_period_agg bfrom on bfrom.entity_name=b1.entity_name and bfrom.period_key=dc.from_key
              left join brand_period_agg bto on bto.entity_name=b1.entity_name and bto.period_key=dc.to_key
            ), '{}'::jsonb)
          )
        ) as metrics
      from brand_period_agg b1
      group by entity_name
    ),
    tp_period_agg as (
      select
        touchpoint,
        period_key,
        avg(recall) as recall,
        avg(consideration) as consideration,
        avg(purchase) as purchase
      from touchpoint_base
      group by touchpoint, period_key
    ),
    tp_metrics_json as (
      select
        touchpoint,
        jsonb_build_object(
          'recall', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, recall) from tp_period_agg t2 where t2.touchpoint=t1.touchpoint), '{}'::jsonb),
            'deltas', coalesce((
              select jsonb_object_agg(dc.key, (tto.recall - tfrom.recall))
              from delta_columns_clean dc
              left join tp_period_agg tfrom on tfrom.touchpoint=t1.touchpoint and tfrom.period_key=dc.from_key
              left join tp_period_agg tto on tto.touchpoint=t1.touchpoint and tto.period_key=dc.to_key
            ), '{}'::jsonb)
          ),
          'consideration', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, consideration) from tp_period_agg t2 where t2.touchpoint=t1.touchpoint), '{}'::jsonb),
            'deltas', coalesce((
              select jsonb_object_agg(dc.key, (tto.consideration - tfrom.consideration))
              from delta_columns_clean dc
              left join tp_period_agg tfrom on tfrom.touchpoint=t1.touchpoint and tfrom.period_key=dc.from_key
              left join tp_period_agg tto on tto.touchpoint=t1.touchpoint and tto.period_key=dc.to_key
            ), '{}'::jsonb)
          ),
          'purchase', jsonb_build_object(
            'values', coalesce((select jsonb_object_agg(period_key, purchase) from tp_period_agg t2 where t2.touchpoint=t1.touchpoint), '{}'::jsonb),
            'deltas', coalesce((
              select jsonb_object_agg(dc.key, (tto.purchase - tfrom.purchase))
              from delta_columns_clean dc
              left join tp_period_agg tfrom on tfrom.touchpoint=t1.touchpoint and tfrom.period_key=dc.from_key
              left join tp_period_agg tto on tto.touchpoint=t1.touchpoint and tto.period_key=dc.to_key
            ), '{}'::jsonb)
          )
        ) as metrics
      from tp_period_agg t1
      group by touchpoint
    ),
    entity_rows_json as (
      select coalesce(
        jsonb_agg(
          jsonb_build_object(
            'entity', entity_name,
            'metrics', metrics
          )
          order by entity_name
        ),
        '[]'::jsonb
      ) as v
      from brand_metrics_json
    ),
    secondary_rows_json as (
      select coalesce(
        jsonb_agg(
          jsonb_build_object(
            'entity', touchpoint,
            'metrics', metrics
          )
          order by touchpoint
        ),
        '[]'::jsonb
      ) as v
      from tp_metrics_json
    ),
    periods_json as (
      select coalesce(
        jsonb_agg(
          jsonb_build_object('key', key, 'label', label, 'order', ord)
          order by ord, key
        ),
        '[]'::jsonb
      ) as v
      from periods_ordered
    ),
    deltas_json as (
      select coalesce(
        jsonb_agg(
          jsonb_build_object('key', key, 'from', from_key, 'to', to_key, 'label', label)
          order by key
        ),
        '[]'::jsonb
      ) as v
      from delta_columns_clean
    )
    select jsonb_build_object(
      'ok', true,
      'resolved_granularity', v_granularity,
      'resolved_breakdown', v_breakdown,
      'entity_label',
        case
          when v_breakdown='sector' then case when v_taxonomy_view='market' then 'Macrosector' else 'Sector' end
          when v_breakdown='subsector' then case when v_taxonomy_view='market' then 'Segmento' else 'Subsector' end
          when v_breakdown='category' then case when v_taxonomy_view='market' then 'Categoría comercial' else 'Category' end
          else 'Brand'
        end,
      'periods', (select v from periods_json),
      'delta_columns', (select v from deltas_json),
      'entity_rows', (select v from entity_rows_json),
      'secondary_rows', (select v from secondary_rows_json),
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
      'meta', jsonb_build_object(
        'source','supabase',
        'warning', null
      )
    )
  );
end;
$function$;

