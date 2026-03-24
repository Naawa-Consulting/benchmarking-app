-- Tables required by export_supabase_seed.py and filter RPC functions.

create table if not exists public.study_catalog (
  study_id text primary key,
  study_name text,
  sector text,
  subsector text,
  category text,
  has_demographics boolean default true,
  has_date boolean default true,
  created_at timestamptz default now()
);

create table if not exists public.taxonomy (
  sector text not null,
  subsector text not null,
  category text not null,
  created_at timestamptz default now()
);

create unique index if not exists ux_taxonomy_key
on public.taxonomy (sector, subsector, category);

create table if not exists public.demographic_options (
  gender text,
  nse text,
  state text,
  age_min double precision,
  age_max double precision,
  created_at timestamptz default now()
);

create unique index if not exists ux_demographic_options_key
on public.demographic_options (gender, nse, state, age_min, age_max);
