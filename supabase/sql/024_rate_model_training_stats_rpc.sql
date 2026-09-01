-- Replaces the corpus-wide parquet scan that services/api/app/routers/analytics.py's
-- _build_consideration_rate_model / _build_satisfaction_rate_model / _build_csat_gap_model
-- perform on every Push (and every journey table computation) with a single SQL query
-- over the already-persisted public.journey_metrics. The per-row estimator
-- (_resolve_*_for_level / _estimate_*) stays in Python unchanged — this function only
-- replaces the training/aggregation step.
--
-- Business constants (min/invalid/max year, excluded market categories) are passed in as
-- parameters rather than hardcoded here, so Python (analytics.py) stays the single source
-- of truth for them.
--
-- Bucketing mirrors the 4-level loop in the Python builders exactly:
--   ("category", market_sector, market_subsector, market_category)
--   ("subsector", market_sector, market_subsector)
--   ("sector", market_sector)
--   ("global",)
-- A bucket for consideration/satisfaction exists for every row that passes the
-- year/excluded-category filter, regardless of whether that row has usable numeric data
-- (matching _get_bucket(key) being called unconditionally in the Python loop). A bucket
-- for csat only exists where a row has both brand_satisfaction and csat present (matching
-- the `continue` guard in _build_csat_gap_model before entering the per-level loop).
--
-- Winsorized median (see _winsorized_median in analytics.py: clip to the 5th/95th
-- percentile, then take the median of the clipped values) has no single-pass Postgres
-- aggregate, so each ratio is computed in two passes: percentile_cont(0.05)/(0.95) per
-- bucket, then a second percentile_cont(0.5) over each value clipped to that range.
--
-- NOTE ON NULLS: sector/global buckets deliberately have NULL lvl_subsector/lvl_category.
-- All joins between per-bucket CTEs use IS NOT DISTINCT FROM, never USING/`=`, or those
-- buckets would silently drop or duplicate rows (NULL = NULL is false in plain equality).

create or replace function public.bbs_rate_model_training_stats(
  p_min_year int default 1900,
  p_invalid_year int default 20,
  p_max_year int default null,
  p_excluded_market_categories text[] default array['specialty stores', 'speciality stores']
)
returns jsonb
language sql
stable
as $$
  with base as (
    select
      coalesce(nullif(trim(market_sector), ''), 'Unassigned') as market_sector,
      coalesce(nullif(trim(market_subsector), ''), 'Unassigned') as market_subsector,
      case
        when lower(regexp_replace(coalesce(market_category, ''), '\s+', ' ', 'g')) = 'speciality stores'
          then 'Specialty Stores'
        else coalesce(nullif(trim(market_category), ''), 'Unassigned')
      end as market_category,
      brand_awareness,
      brand_purchase,
      brand_recommendation,
      -- IS DISTINCT FROM 'imputed' (not `= 'observed'`): a handful of studies were pushed
      -- before the *_source columns existed for a given metric and have source = NULL with
      -- a real value still present (confirmed live: 20240925_senosiain_brand_tracking_ddbb_w2
      -- has csat_source null for all 28 rows). Those are genuinely observed, never imputed —
      -- excluding them on a strict '= observed' filter would silently shrink the training
      -- corpus for no good reason.
      case when brand_consideration_source is distinct from 'imputed' then brand_consideration end as consideration,
      case when brand_satisfaction_source is distinct from 'imputed' then brand_satisfaction end as satisfaction,
      case when csat_source is distinct from 'imputed' then csat end as csat
    from public.journey_metrics
    where year is not null
      and year <> p_invalid_year
      and year between p_min_year and coalesce(p_max_year, extract(year from now())::int + 1)
      and lower(regexp_replace(coalesce(market_category, ''), '\s+', ' ', 'g')) not in (
        select lower(x) from unnest(p_excluded_market_categories) as x
      )
  ),
  levels as (
    select
      b.*,
      l.level,
      case when l.level in ('category', 'subsector', 'sector') then b.market_sector else null end as lvl_sector,
      case when l.level in ('category', 'subsector') then b.market_subsector else null end as lvl_subsector,
      case when l.level = 'category' then b.market_category else null end as lvl_category
    from base b
    cross join (values ('category'), ('subsector'), ('sector'), ('global')) as l(level)
  ),

  -- ===== CONSIDERATION (c_over_a, p_over_c) =====
  ca_raw as (
    select level, lvl_sector, lvl_subsector, lvl_category,
           consideration / nullif(brand_awareness, 0) as value
    from levels
    where brand_awareness is not null and brand_awareness > 0 and consideration is not null
  ),
  ca_bounds as (
    select level, lvl_sector, lvl_subsector, lvl_category,
           percentile_cont(0.05) within group (order by value) as p5,
           percentile_cont(0.95) within group (order by value) as p95
    from ca_raw group by level, lvl_sector, lvl_subsector, lvl_category
  ),
  ca_stats as (
    select r.level, r.lvl_sector, r.lvl_subsector, r.lvl_category,
           count(*) as n_ca,
           percentile_cont(0.5) within group (order by least(greatest(r.value, b.p5), b.p95)) as r_ca
    from ca_raw r
    join ca_bounds b on r.level is not distinct from b.level
      and r.lvl_sector is not distinct from b.lvl_sector
      and r.lvl_subsector is not distinct from b.lvl_subsector
      and r.lvl_category is not distinct from b.lvl_category
    group by r.level, r.lvl_sector, r.lvl_subsector, r.lvl_category
  ),
  pc_raw as (
    select level, lvl_sector, lvl_subsector, lvl_category,
           brand_purchase / nullif(consideration, 0) as value
    from levels
    where consideration is not null and consideration > 0 and brand_purchase is not null
  ),
  pc_bounds as (
    select level, lvl_sector, lvl_subsector, lvl_category,
           percentile_cont(0.05) within group (order by value) as p5,
           percentile_cont(0.95) within group (order by value) as p95
    from pc_raw group by level, lvl_sector, lvl_subsector, lvl_category
  ),
  pc_stats as (
    select r.level, r.lvl_sector, r.lvl_subsector, r.lvl_category,
           count(*) as n_pc,
           percentile_cont(0.5) within group (order by least(greatest(r.value, b.p5), b.p95)) as r_pc
    from pc_raw r
    join pc_bounds b on r.level is not distinct from b.level
      and r.lvl_sector is not distinct from b.lvl_sector
      and r.lvl_subsector is not distinct from b.lvl_subsector
      and r.lvl_category is not distinct from b.lvl_category
    group by r.level, r.lvl_sector, r.lvl_subsector, r.lvl_category
  ),
  consideration_buckets_all as (
    select level, lvl_sector, lvl_subsector, lvl_category,
      count(*) filter (where consideration is not null and brand_purchase is not null) as comparisons_total,
      count(*) filter (where consideration is not null and brand_purchase is not null and brand_purchase > consideration) as comparisons_p_gt_c
    from levels group by level, lvl_sector, lvl_subsector, lvl_category
  ),
  consideration_buckets as (
    select cc.level, cc.lvl_sector, cc.lvl_subsector, cc.lvl_category,
      coalesce(ca.n_ca, 0) as n_ca, ca.r_ca,
      coalesce(pc.n_pc, 0) as n_pc, pc.r_pc,
      cc.comparisons_total, cc.comparisons_p_gt_c
    from consideration_buckets_all cc
    left join ca_stats ca on cc.level is not distinct from ca.level
      and cc.lvl_sector is not distinct from ca.lvl_sector
      and cc.lvl_subsector is not distinct from ca.lvl_subsector
      and cc.lvl_category is not distinct from ca.lvl_category
    left join pc_stats pc on cc.level is not distinct from pc.level
      and cc.lvl_sector is not distinct from pc.lvl_sector
      and cc.lvl_subsector is not distinct from pc.lvl_subsector
      and cc.lvl_category is not distinct from pc.lvl_category
  ),

  -- ===== SATISFACTION (s_over_p, s_over_r) =====
  sp_raw as (
    select level, lvl_sector, lvl_subsector, lvl_category,
           satisfaction / nullif(brand_purchase, 0) as value
    from levels
    where brand_purchase is not null and brand_purchase > 0 and satisfaction is not null
  ),
  sp_bounds as (
    select level, lvl_sector, lvl_subsector, lvl_category,
           percentile_cont(0.05) within group (order by value) as p5,
           percentile_cont(0.95) within group (order by value) as p95
    from sp_raw group by level, lvl_sector, lvl_subsector, lvl_category
  ),
  sp_stats as (
    select r.level, r.lvl_sector, r.lvl_subsector, r.lvl_category,
           count(*) as n_sp,
           percentile_cont(0.5) within group (order by least(greatest(r.value, b.p5), b.p95)) as r_sp
    from sp_raw r
    join sp_bounds b on r.level is not distinct from b.level
      and r.lvl_sector is not distinct from b.lvl_sector
      and r.lvl_subsector is not distinct from b.lvl_subsector
      and r.lvl_category is not distinct from b.lvl_category
    group by r.level, r.lvl_sector, r.lvl_subsector, r.lvl_category
  ),
  sr_raw as (
    select level, lvl_sector, lvl_subsector, lvl_category,
           satisfaction / nullif(brand_recommendation, 0) as value
    from levels
    where brand_recommendation is not null and brand_recommendation > 0 and satisfaction is not null
  ),
  sr_bounds as (
    select level, lvl_sector, lvl_subsector, lvl_category,
           percentile_cont(0.05) within group (order by value) as p5,
           percentile_cont(0.95) within group (order by value) as p95
    from sr_raw group by level, lvl_sector, lvl_subsector, lvl_category
  ),
  sr_stats as (
    select r.level, r.lvl_sector, r.lvl_subsector, r.lvl_category,
           count(*) as n_sr,
           percentile_cont(0.5) within group (order by least(greatest(r.value, b.p5), b.p95)) as r_sr
    from sr_raw r
    join sr_bounds b on r.level is not distinct from b.level
      and r.lvl_sector is not distinct from b.lvl_sector
      and r.lvl_subsector is not distinct from b.lvl_subsector
      and r.lvl_category is not distinct from b.lvl_category
    group by r.level, r.lvl_sector, r.lvl_subsector, r.lvl_category
  ),
  satisfaction_buckets_all as (
    select level, lvl_sector, lvl_subsector, lvl_category,
      count(*) filter (where brand_recommendation is not null and satisfaction is not null) as comparisons_total,
      count(*) filter (where brand_recommendation is not null and satisfaction is not null and brand_recommendation > satisfaction) as comparisons_r_gt_s
    from levels group by level, lvl_sector, lvl_subsector, lvl_category
  ),
  satisfaction_buckets as (
    select cc.level, cc.lvl_sector, cc.lvl_subsector, cc.lvl_category,
      coalesce(sp.n_sp, 0) as n_sp, sp.r_sp,
      coalesce(sr.n_sr, 0) as n_sr, sr.r_sr,
      cc.comparisons_total, cc.comparisons_r_gt_s
    from satisfaction_buckets_all cc
    left join sp_stats sp on cc.level is not distinct from sp.level
      and cc.lvl_sector is not distinct from sp.lvl_sector
      and cc.lvl_subsector is not distinct from sp.lvl_subsector
      and cc.lvl_category is not distinct from sp.lvl_category
    left join sr_stats sr on cc.level is not distinct from sr.level
      and cc.lvl_sector is not distinct from sr.lvl_sector
      and cc.lvl_subsector is not distinct from sr.lvl_subsector
      and cc.lvl_category is not distinct from sr.lvl_category
  ),

  -- ===== CSAT (sat_minus_csat, csat_minus_rec) =====
  -- Unlike consideration/satisfaction, a row only enters bucketing at all when both
  -- brand_satisfaction and csat are present (mirrors the `continue` guard in
  -- _build_csat_gap_model before its per-level loop) — so the "universal" bucket set here
  -- is scoped to csat_levels, not all of `levels`.
  csat_levels as (
    select * from levels where satisfaction is not null and csat is not null
  ),
  sc_raw as (
    select level, lvl_sector, lvl_subsector, lvl_category, (satisfaction - csat) as value
    from csat_levels
  ),
  sc_bounds as (
    select level, lvl_sector, lvl_subsector, lvl_category,
           percentile_cont(0.05) within group (order by value) as p5,
           percentile_cont(0.95) within group (order by value) as p95
    from sc_raw group by level, lvl_sector, lvl_subsector, lvl_category
  ),
  sc_stats as (
    select r.level, r.lvl_sector, r.lvl_subsector, r.lvl_category,
           count(*) as n_delta_sc,
           percentile_cont(0.5) within group (order by least(greatest(r.value, b.p5), b.p95)) as delta_sc
    from sc_raw r
    join sc_bounds b on r.level is not distinct from b.level
      and r.lvl_sector is not distinct from b.lvl_sector
      and r.lvl_subsector is not distinct from b.lvl_subsector
      and r.lvl_category is not distinct from b.lvl_category
    group by r.level, r.lvl_sector, r.lvl_subsector, r.lvl_category
  ),
  cr_raw as (
    select level, lvl_sector, lvl_subsector, lvl_category, (csat - brand_recommendation) as value
    from csat_levels
    where brand_recommendation is not null
  ),
  cr_bounds as (
    select level, lvl_sector, lvl_subsector, lvl_category,
           percentile_cont(0.05) within group (order by value) as p5,
           percentile_cont(0.95) within group (order by value) as p95
    from cr_raw group by level, lvl_sector, lvl_subsector, lvl_category
  ),
  cr_stats as (
    select r.level, r.lvl_sector, r.lvl_subsector, r.lvl_category,
           count(*) as n_delta_cr,
           percentile_cont(0.5) within group (order by least(greatest(r.value, b.p5), b.p95)) as delta_cr
    from cr_raw r
    join cr_bounds b on r.level is not distinct from b.level
      and r.lvl_sector is not distinct from b.lvl_sector
      and r.lvl_subsector is not distinct from b.lvl_subsector
      and r.lvl_category is not distinct from b.lvl_category
    group by r.level, r.lvl_sector, r.lvl_subsector, r.lvl_category
  ),
  csat_buckets_all as (
    select level, lvl_sector, lvl_subsector, lvl_category,
      count(*) as comparisons_total,
      count(*) filter (where csat > satisfaction) as comparisons_csat_gt_sat
    from csat_levels group by level, lvl_sector, lvl_subsector, lvl_category
  ),
  csat_buckets as (
    select cc.level, cc.lvl_sector, cc.lvl_subsector, cc.lvl_category,
      coalesce(sc.n_delta_sc, 0) as n_delta_sc, sc.delta_sc,
      coalesce(cr.n_delta_cr, 0) as n_delta_cr, cr.delta_cr,
      cc.comparisons_total, cc.comparisons_csat_gt_sat
    from csat_buckets_all cc
    left join sc_stats sc on cc.level is not distinct from sc.level
      and cc.lvl_sector is not distinct from sc.lvl_sector
      and cc.lvl_subsector is not distinct from sc.lvl_subsector
      and cc.lvl_category is not distinct from sc.lvl_category
    left join cr_stats cr on cc.level is not distinct from cr.level
      and cc.lvl_sector is not distinct from cr.lvl_sector
      and cc.lvl_subsector is not distinct from cr.lvl_subsector
      and cc.lvl_category is not distinct from cr.lvl_category
  )

  select jsonb_build_object(
    'consideration', (
      select coalesce(jsonb_agg(jsonb_build_object(
        'level', level, 'market_sector', lvl_sector, 'market_subsector', lvl_subsector, 'market_category', lvl_category,
        'n_ca', n_ca, 'r_ca', r_ca, 'n_pc', n_pc, 'r_pc', r_pc,
        'comparisons_total', comparisons_total, 'comparisons_p_gt_c', comparisons_p_gt_c
      )), '[]'::jsonb)
      from consideration_buckets
    ),
    'satisfaction', (
      select coalesce(jsonb_agg(jsonb_build_object(
        'level', level, 'market_sector', lvl_sector, 'market_subsector', lvl_subsector, 'market_category', lvl_category,
        'n_sp', n_sp, 'r_sp', r_sp, 'n_sr', n_sr, 'r_sr', r_sr,
        'comparisons_total', comparisons_total, 'comparisons_r_gt_s', comparisons_r_gt_s
      )), '[]'::jsonb)
      from satisfaction_buckets
    ),
    'csat', (
      select coalesce(jsonb_agg(jsonb_build_object(
        'level', level, 'market_sector', lvl_sector, 'market_subsector', lvl_subsector, 'market_category', lvl_category,
        'n_delta_sc', n_delta_sc, 'delta_sc', delta_sc, 'n_delta_cr', n_delta_cr, 'delta_cr', delta_cr,
        'comparisons_total', comparisons_total, 'comparisons_csat_gt_sat', comparisons_csat_gt_sat
      )), '[]'::jsonb)
      from csat_buckets
    )
  );
$$;
