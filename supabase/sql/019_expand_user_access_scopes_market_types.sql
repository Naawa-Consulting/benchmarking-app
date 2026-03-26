do $$
declare
  constraint_name text;
begin
  select c.conname
    into constraint_name
  from pg_constraint c
  join pg_class t on t.oid = c.conrelid
  join pg_namespace n on n.oid = t.relnamespace
  where n.nspname = 'public'
    and t.relname = 'user_access_scopes'
    and c.contype = 'c'
    and pg_get_constraintdef(c.oid) ilike '%scope_type%';

  if constraint_name is not null then
    execute format('alter table public.user_access_scopes drop constraint %I', constraint_name);
  end if;
end $$;

alter table if exists public.user_access_scopes
  add constraint user_access_scopes_scope_type_check
  check (
    scope_type in (
      'sector',
      'subsector',
      'category',
      'market_sector',
      'market_subsector',
      'market_category'
    )
  );
