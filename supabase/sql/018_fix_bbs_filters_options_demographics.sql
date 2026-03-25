-- Fix demographics options RPC to return real values from snapshot table.
-- Supports normalized gender buckets used in frontend demo filters.

create or replace function public.bbs_filters_options_demographics(
  query jsonb default '{}'::jsonb,
  payload jsonb default '{}'::jsonb
)
returns jsonb
language sql
stable
as $$
  with normalized as (
    select
      case
        when gender is null or btrim(gender) = '' then null
        when lower(translate(gender, 'ÁÉÍÓÚáéíóú', 'AEIOUaeiou')) ~ '(prefer not|prefiere no|declina|no responde|refus)' then 'Prefer not to say'
        when lower(translate(gender, 'ÁÉÍÓÚáéíóú', 'AEIOUaeiou')) ~ '(non[- ]?binary|no binari|genderqueer)' then 'Non-binary'
        when lower(translate(gender, 'ÁÉÍÓÚáéíóú', 'AEIOUaeiou')) ~ '(^|[^a-z])(male|mascul|hombre|varon|m)([^a-z]|$)' then 'Male'
        when lower(translate(gender, 'ÁÉÍÓÚáéíóú', 'AEIOUaeiou')) ~ '(^|[^a-z])(female|femen|mujer|f)([^a-z]|$)' then 'Female'
        else 'Unknown'
      end as gender_std,
      nullif(btrim(nse), '') as nse_norm,
      nullif(btrim(state), '') as state_norm,
      age_min,
      age_max
    from public.demographic_options
  ),
  agg as (
    select
      array_remove(array_agg(distinct gender_std), null) as genders,
      array_remove(array_agg(distinct nse_norm), null) as nses,
      array_remove(array_agg(distinct state_norm), null) as states,
      min(age_min) as age_min,
      max(age_max) as age_max
    from normalized
  )
  select jsonb_build_object(
    'gender',
      to_jsonb(
        array(
          select g
          from unnest(array['Male','Female','Non-binary','Prefer not to say','Unknown']) as g
          where g = any(coalesce((select genders from agg), array[]::text[]))
        )
      ),
    'nse', to_jsonb(coalesce((select nses from agg), array[]::text[])),
    'state', to_jsonb(coalesce((select states from agg), array[]::text[])),
    'age', jsonb_build_object(
      'min', (select age_min from agg),
      'max', (select age_max from agg)
    )
  );
$$;
