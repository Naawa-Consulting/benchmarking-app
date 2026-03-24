-- Fase 1 (Data module): operational tables for uploads and ingestion jobs.

create table if not exists public.uploaded_files (
  id uuid primary key default gen_random_uuid(),
  bucket text not null,
  storage_path text not null,
  filename text not null,
  content_type text,
  size_bytes bigint not null default 0,
  checksum text,
  uploaded_by uuid references auth.users(id) on delete set null,
  status text not null default 'uploaded' check (status in ('uploaded', 'processed', 'deleted', 'error')),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists uploaded_files_bucket_path_uidx on public.uploaded_files(bucket, storage_path);
create index if not exists uploaded_files_created_idx on public.uploaded_files(created_at desc);

create table if not exists public.ingestion_jobs (
  id uuid primary key default gen_random_uuid(),
  file_id uuid references public.uploaded_files(id) on delete set null,
  requested_by uuid references auth.users(id) on delete set null,
  status text not null default 'pending' check (status in ('pending', 'running', 'success', 'error', 'cancelled')),
  operation text not null default 'upload',
  started_at timestamptz,
  finished_at timestamptz,
  error_message text,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists ingestion_jobs_created_idx on public.ingestion_jobs(created_at desc);
create index if not exists ingestion_jobs_status_idx on public.ingestion_jobs(status);

create table if not exists public.ingestion_job_logs (
  id bigserial primary key,
  job_id uuid not null references public.ingestion_jobs(id) on delete cascade,
  level text not null default 'info' check (level in ('debug', 'info', 'warn', 'error')),
  message text not null,
  context jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists ingestion_job_logs_job_created_idx on public.ingestion_job_logs(job_id, created_at desc);

create table if not exists public.data_versions (
  id uuid primary key default gen_random_uuid(),
  source_job_id uuid references public.ingestion_jobs(id) on delete set null,
  label text not null,
  status text not null default 'draft' check (status in ('draft', 'published', 'archived')),
  notes text,
  created_by uuid references auth.users(id) on delete set null,
  published_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists data_versions_created_idx on public.data_versions(created_at desc);
create index if not exists data_versions_status_idx on public.data_versions(status);

create or replace function public.set_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end $$;

drop trigger if exists trg_uploaded_files_updated_at on public.uploaded_files;
create trigger trg_uploaded_files_updated_at
before update on public.uploaded_files
for each row execute function public.set_updated_at();

drop trigger if exists trg_ingestion_jobs_updated_at on public.ingestion_jobs;
create trigger trg_ingestion_jobs_updated_at
before update on public.ingestion_jobs
for each row execute function public.set_updated_at();

drop trigger if exists trg_data_versions_updated_at on public.data_versions;
create trigger trg_data_versions_updated_at
before update on public.data_versions
for each row execute function public.set_updated_at();

alter table public.uploaded_files enable row level security;
alter table public.ingestion_jobs enable row level security;
alter table public.ingestion_job_logs enable row level security;
alter table public.data_versions enable row level security;

drop policy if exists uploaded_files_read_auth on public.uploaded_files;
create policy uploaded_files_read_auth on public.uploaded_files
for select to authenticated using (true);

drop policy if exists ingestion_jobs_read_auth on public.ingestion_jobs;
create policy ingestion_jobs_read_auth on public.ingestion_jobs
for select to authenticated using (true);

drop policy if exists ingestion_job_logs_read_auth on public.ingestion_job_logs;
create policy ingestion_job_logs_read_auth on public.ingestion_job_logs
for select to authenticated using (true);

drop policy if exists data_versions_read_auth on public.data_versions;
create policy data_versions_read_auth on public.data_versions
for select to authenticated using (true);
