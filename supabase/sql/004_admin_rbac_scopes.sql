-- Fase 4: Admin user management + fine-grained viewer scopes.

create table if not exists public.user_permissions (
  id bigserial primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  permission text not null,
  created_at timestamptz not null default now()
);

create unique index if not exists user_permissions_user_permission_uidx
on public.user_permissions(user_id, permission);

create table if not exists public.user_access_scopes (
  id bigserial primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  scope_type text not null check (scope_type in ('sector', 'subsector', 'category')),
  scope_key text not null,
  created_at timestamptz not null default now()
);

create unique index if not exists user_access_scopes_user_type_key_uidx
on public.user_access_scopes(user_id, scope_type, scope_key);

create index if not exists user_access_scopes_user_idx
on public.user_access_scopes(user_id);

create or replace function public.bbs_can_read_scope(
  p_sector text,
  p_subsector text,
  p_category text
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
        (uas.scope_type = 'category' and lower(uas.scope_key) = lower(coalesce(p_category, '')))
        or (uas.scope_type = 'subsector' and lower(uas.scope_key) = lower(coalesce(p_subsector, '')))
        or (uas.scope_type = 'sector' and lower(uas.scope_key) = lower(coalesce(p_sector, '')))
      )
  );
end;
$$;

alter table if exists public.study_catalog enable row level security;
alter table if exists public.journey_metrics enable row level security;
alter table if exists public.touchpoint_metrics enable row level security;

drop policy if exists study_catalog_bbs_read on public.study_catalog;
create policy study_catalog_bbs_read
on public.study_catalog
for select
to authenticated
using (public.bbs_can_read_scope(sector, subsector, category));

drop policy if exists journey_metrics_bbs_read on public.journey_metrics;
create policy journey_metrics_bbs_read
on public.journey_metrics
for select
to authenticated
using (public.bbs_can_read_scope(sector, subsector, category));

drop policy if exists touchpoint_metrics_bbs_read on public.touchpoint_metrics;
create policy touchpoint_metrics_bbs_read
on public.touchpoint_metrics
for select
to authenticated
using (public.bbs_can_read_scope(sector, subsector, category));
