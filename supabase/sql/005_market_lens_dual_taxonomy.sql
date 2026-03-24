-- Fase Taxonomía Doble: Market Lens + Estándar (compatibilidad incluida).

create table if not exists public.taxonomy_market_lens (
  market_sector text not null,
  market_subsector text not null,
  market_category text not null,
  created_at timestamptz not null default now()
);

create unique index if not exists ux_taxonomy_market_lens_key
on public.taxonomy_market_lens (market_sector, market_subsector, market_category);

alter table if exists public.study_catalog
  add column if not exists market_sector text,
  add column if not exists market_subsector text,
  add column if not exists market_category text,
  add column if not exists market_source text check (market_source in ('rule', 'manual')),
  add column if not exists market_updated_at timestamptz;

alter table if exists public.journey_metrics
  add column if not exists market_sector text,
  add column if not exists market_subsector text,
  add column if not exists market_category text;

alter table if exists public.touchpoint_metrics
  add column if not exists market_sector text,
  add column if not exists market_subsector text,
  add column if not exists market_category text;

update public.study_catalog
set
  market_sector = coalesce(nullif(trim(market_sector), ''), nullif(trim(sector), ''), 'Unassigned'),
  market_subsector = coalesce(nullif(trim(market_subsector), ''), nullif(trim(subsector), ''), 'Unassigned'),
  market_category = coalesce(nullif(trim(market_category), ''), nullif(trim(category), ''), 'Unassigned'),
  market_source = coalesce(market_source, 'rule'),
  market_updated_at = coalesce(market_updated_at, now());

update public.journey_metrics jm
set
  market_sector = coalesce(jm.market_sector, sc.market_sector, jm.sector, 'Unassigned'),
  market_subsector = coalesce(jm.market_subsector, sc.market_subsector, jm.subsector, 'Unassigned'),
  market_category = coalesce(jm.market_category, sc.market_category, jm.category, 'Unassigned')
from public.study_catalog sc
where sc.study_id = jm.study_id;

update public.touchpoint_metrics tm
set
  market_sector = coalesce(tm.market_sector, sc.market_sector, tm.sector, 'Unassigned'),
  market_subsector = coalesce(tm.market_subsector, sc.market_subsector, tm.subsector, 'Unassigned'),
  market_category = coalesce(tm.market_category, sc.market_category, tm.category, 'Unassigned')
from public.study_catalog sc
where sc.study_id = tm.study_id;

insert into public.taxonomy_market_lens (market_sector, market_subsector, market_category)
select distinct
  coalesce(nullif(trim(market_sector), ''), 'Unassigned'),
  coalesce(nullif(trim(market_subsector), ''), 'Unassigned'),
  coalesce(nullif(trim(market_category), ''), 'Unassigned')
from public.study_catalog
where coalesce(nullif(trim(market_sector), ''), '') <> ''
  and coalesce(nullif(trim(market_subsector), ''), '') <> ''
  and coalesce(nullif(trim(market_category), ''), '') <> ''
on conflict (market_sector, market_subsector, market_category) do nothing;

create index if not exists idx_study_catalog_market_triplet
on public.study_catalog (market_sector, market_subsector, market_category);

create index if not exists idx_journey_metrics_market_triplet
on public.journey_metrics (market_sector, market_subsector, market_category);

create index if not exists idx_touchpoint_metrics_market_triplet
on public.touchpoint_metrics (market_sector, market_subsector, market_category);

do $$
declare
  c record;
begin
  for c in
    select conname
    from pg_constraint
    where conrelid = 'public.user_access_scopes'::regclass
      and contype = 'c'
      and conname ilike '%scope_type%'
  loop
    execute format('alter table public.user_access_scopes drop constraint %I', c.conname);
  end loop;
end $$;

alter table if exists public.user_access_scopes
  add constraint user_access_scopes_scope_type_check
  check (
    scope_type in (
      'sector', 'subsector', 'category',
      'market_sector', 'market_subsector', 'market_category'
    )
  );

create or replace function public.bbs_can_read_scope(
  p_market_sector text,
  p_market_subsector text,
  p_market_category text
)
returns boolean
language plpgsql
stable
as $$
declare
  v_uid uuid;
  v_role text;
begin
  if current_setting('role', true) = 'service_role' then
    return true;
  end if;

  v_uid := auth.uid();
  if v_uid is null then
    return false;
  end if;

  select lower(role) into v_role
  from public.user_roles
  where user_id = v_uid
  limit 1;

  if v_role in ('owner', 'admin', 'analyst') then
    return true;
  end if;

  if coalesce(v_role, 'viewer') <> 'viewer' then
    return false;
  end if;

  return exists (
    select 1
    from public.user_access_scopes uas
    where uas.user_id = v_uid
      and (
        (uas.scope_type in ('market_category', 'category') and lower(uas.scope_key) = lower(coalesce(p_market_category, '')))
        or (uas.scope_type in ('market_subsector', 'subsector') and lower(uas.scope_key) = lower(coalesce(p_market_subsector, '')))
        or (uas.scope_type in ('market_sector', 'sector') and lower(uas.scope_key) = lower(coalesce(p_market_sector, '')))
      )
  );
end;
$$;

drop policy if exists study_catalog_bbs_read on public.study_catalog;
create policy study_catalog_bbs_read
on public.study_catalog
for select
to authenticated
using (public.bbs_can_read_scope(market_sector, market_subsector, market_category));

drop policy if exists journey_metrics_bbs_read on public.journey_metrics;
create policy journey_metrics_bbs_read
on public.journey_metrics
for select
to authenticated
using (public.bbs_can_read_scope(market_sector, market_subsector, market_category));

drop policy if exists touchpoint_metrics_bbs_read on public.touchpoint_metrics;
create policy touchpoint_metrics_bbs_read
on public.touchpoint_metrics
for select
to authenticated
using (public.bbs_can_read_scope(market_sector, market_subsector, market_category));
