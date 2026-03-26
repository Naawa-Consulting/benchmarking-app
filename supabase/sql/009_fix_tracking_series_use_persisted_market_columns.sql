do $$
declare
  fn_def text;
  updated text;
begin
  select pg_get_functiondef(p.oid)
    into fn_def
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'public'
    and p.proname = 'bbs_tracking_series'
  limit 1;

  if fn_def is null then
    raise exception 'Function public.bbs_tracking_series not found';
  end if;

  updated := fn_def;

  updated := replace(
    updated,
    'case when v_taxonomy_view=''market'' then public.bbs_market_sector(jm.sector, jm.subsector, jm.category) else coalesce(jm.sector, ''Unassigned'') end as tx_sector,',
    'case when v_taxonomy_view=''market'' then coalesce(nullif(trim(jm.market_sector), ''''), public.bbs_market_sector(jm.sector, jm.subsector, jm.category)) else coalesce(jm.sector, ''Unassigned'') end as tx_sector,'
  );

  updated := replace(
    updated,
    'case when v_taxonomy_view=''market'' then public.bbs_market_subsector(jm.sector, jm.subsector, jm.category) else coalesce(jm.subsector, ''Unassigned'') end as tx_subsector,',
    'case when v_taxonomy_view=''market'' then coalesce(nullif(trim(jm.market_subsector), ''''), public.bbs_market_subsector(jm.sector, jm.subsector, jm.category)) else coalesce(jm.subsector, ''Unassigned'') end as tx_subsector,'
  );

  updated := replace(
    updated,
    'case when v_taxonomy_view=''market'' then public.bbs_market_category(jm.sector, jm.subsector, jm.category) else coalesce(jm.category, ''Unassigned'') end as tx_category,',
    'case when v_taxonomy_view=''market'' then coalesce(nullif(trim(jm.market_category), ''''), public.bbs_market_category(jm.sector, jm.subsector, jm.category)) else coalesce(jm.category, ''Unassigned'') end as tx_category,'
  );

  updated := replace(
    updated,
    'case when v_taxonomy_view=''market'' then public.bbs_market_sector(tm.sector, tm.subsector, tm.category) else coalesce(tm.sector, ''Unassigned'') end as tx_sector,',
    'case when v_taxonomy_view=''market'' then coalesce(nullif(trim(tm.market_sector), ''''), public.bbs_market_sector(tm.sector, tm.subsector, tm.category)) else coalesce(tm.sector, ''Unassigned'') end as tx_sector,'
  );

  updated := replace(
    updated,
    'case when v_taxonomy_view=''market'' then public.bbs_market_subsector(tm.sector, tm.subsector, tm.category) else coalesce(tm.subsector, ''Unassigned'') end as tx_subsector,',
    'case when v_taxonomy_view=''market'' then coalesce(nullif(trim(tm.market_subsector), ''''), public.bbs_market_subsector(tm.sector, tm.subsector, tm.category)) else coalesce(tm.subsector, ''Unassigned'') end as tx_subsector,'
  );

  updated := replace(
    updated,
    'case when v_taxonomy_view=''market'' then public.bbs_market_category(tm.sector, tm.subsector, tm.category) else coalesce(tm.category, ''Unassigned'') end as tx_category,',
    'case when v_taxonomy_view=''market'' then coalesce(nullif(trim(tm.market_category), ''''), public.bbs_market_category(tm.sector, tm.subsector, tm.category)) else coalesce(tm.category, ''Unassigned'') end as tx_category,'
  );

  if position('jm.market_sector' in updated) = 0 then
    raise exception 'Patch was not applied to journey_metrics expressions';
  end if;

  if position('tm.market_sector' in updated) = 0 then
    raise exception 'Patch was not applied to touchpoint_metrics expressions';
  end if;

  execute updated;
end;
$$;
