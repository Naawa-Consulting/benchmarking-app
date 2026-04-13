-- Agent v1 chat persistence

create extension if not exists pgcrypto;

create table if not exists public.agent_conversations (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users (id) on delete cascade,
  title text not null default 'New chat',
  archived boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  last_message_at timestamptz not null default now()
);

create table if not exists public.agent_messages (
  id uuid primary key default gen_random_uuid(),
  conversation_id uuid not null references public.agent_conversations (id) on delete cascade,
  role text not null check (role in ('user', 'assistant', 'system')),
  content text not null,
  chart_spec jsonb null,
  meta jsonb null,
  created_at timestamptz not null default now()
);

create index if not exists idx_agent_conversations_user_last
  on public.agent_conversations (user_id, last_message_at desc);

create index if not exists idx_agent_messages_conversation_created
  on public.agent_messages (conversation_id, created_at asc);

alter table public.agent_conversations enable row level security;
alter table public.agent_messages enable row level security;

drop policy if exists agent_conversations_owner_select on public.agent_conversations;
create policy agent_conversations_owner_select
  on public.agent_conversations
  for select
  to authenticated
  using (auth.uid() = user_id);

drop policy if exists agent_conversations_owner_insert on public.agent_conversations;
create policy agent_conversations_owner_insert
  on public.agent_conversations
  for insert
  to authenticated
  with check (auth.uid() = user_id);

drop policy if exists agent_conversations_owner_update on public.agent_conversations;
create policy agent_conversations_owner_update
  on public.agent_conversations
  for update
  to authenticated
  using (auth.uid() = user_id)
  with check (auth.uid() = user_id);

drop policy if exists agent_conversations_owner_delete on public.agent_conversations;
create policy agent_conversations_owner_delete
  on public.agent_conversations
  for delete
  to authenticated
  using (auth.uid() = user_id);

drop policy if exists agent_messages_owner_select on public.agent_messages;
create policy agent_messages_owner_select
  on public.agent_messages
  for select
  to authenticated
  using (
    exists (
      select 1
      from public.agent_conversations c
      where c.id = agent_messages.conversation_id
        and c.user_id = auth.uid()
    )
  );

drop policy if exists agent_messages_owner_insert on public.agent_messages;
create policy agent_messages_owner_insert
  on public.agent_messages
  for insert
  to authenticated
  with check (
    exists (
      select 1
      from public.agent_conversations c
      where c.id = agent_messages.conversation_id
        and c.user_id = auth.uid()
    )
  );

drop policy if exists agent_messages_owner_update on public.agent_messages;
create policy agent_messages_owner_update
  on public.agent_messages
  for update
  to authenticated
  using (
    exists (
      select 1
      from public.agent_conversations c
      where c.id = agent_messages.conversation_id
        and c.user_id = auth.uid()
    )
  )
  with check (
    exists (
      select 1
      from public.agent_conversations c
      where c.id = agent_messages.conversation_id
        and c.user_id = auth.uid()
    )
  );

drop policy if exists agent_messages_owner_delete on public.agent_messages;
create policy agent_messages_owner_delete
  on public.agent_messages
  for delete
  to authenticated
  using (
    exists (
      select 1
      from public.agent_conversations c
      where c.id = agent_messages.conversation_id
        and c.user_id = auth.uid()
    )
  );
