-- Adds Calzado/Calzado deportivo (athletic footwear brands, e.g. Nike, Adidas — sector
-- Industria, not a retail category like Zapaterías/Comercio) to the Market Lens fallback
-- functions, same pattern as 013_market_lens_beverages_beer.sql. Mirrors the standard
-- taxonomy addition in warehouse/taxonomy/sector_subsector_category_v1.json (Industria ->
-- Calzado -> Calzado deportivo) and the new rules in market_lens_rules_v1.json (shares
-- market_subsector "Fashion" with "Tiendas de ropa", distinct market_category
-- "Athletic Footwear").

create or replace function public.bbs_market_subsector(p_sector text, p_subsector text, p_category text)
returns text
language sql
immutable
as $$
  select case
    when public.bbs_norm_taxon(p_subsector) in (
      'materiales para la construccion',
      'materiales para la construcción',
      'materiales para la construcciã£â³n'
    )
      or public.bbs_norm_taxon(p_category) in (
        'quimicos y soluciones para la construccion',
        'químicos y soluciones para la construcción',
        'quimicos y soluciones para la construcciã£â³n'
      ) then 'Home Improvement'
    when public.bbs_norm_taxon(p_category) = 'tiendas de descuento' then 'Mass Retail'
    when public.bbs_norm_taxon(p_category) in ('tiendas de ropa', 'calzado deportivo')
      or public.bbs_norm_taxon(p_subsector) = 'calzado' then 'Fashion'
    when public.bbs_norm_taxon(p_category) = 'farmacias' then 'Pharma Retail'
    when public.bbs_norm_taxon(p_category) in ('hospitales','clinicas','clínicas','laboratorios') then 'Healthcare Services'
    when public.bbs_norm_taxon(p_category) in ('cervezas', 'bebidas alcoholicas', 'bebidas alcohólicas', 'bebidas no alcoholicas', 'bebidas no alcohólicas')
      or public.bbs_norm_taxon(p_subsector) = 'bebidas' then 'Beverage'
    when public.bbs_norm_taxon(p_subsector) = 'e-commerce' then 'Digital Commerce'
    when public.bbs_norm_taxon(p_subsector) = 'autoservicios' then 'Mass Retail'
    when public.bbs_norm_taxon(p_subsector) = 'comercio especializado' then 'Specialty Retail'
    when public.bbs_norm_taxon(p_sector) = 'comercio' then 'Retail'
    when public.bbs_norm_taxon(p_sector) = 'servicios' then 'Consumer Services'
    when public.bbs_norm_taxon(p_sector) = 'industria' then 'Manufacturing'
    when public.bbs_norm_taxon(p_sector) = 'servicios financieros' then 'Banking & Insurance'
    when coalesce(nullif(trim(p_subsector),''), '') = '' then coalesce(nullif(trim(p_sector),''), 'Unassigned')
    else coalesce(nullif(trim(p_subsector),''), 'Unassigned')
  end;
$$;

create or replace function public.bbs_market_category(p_sector text, p_subsector text, p_category text)
returns text
language sql
immutable
as $$
  select case
    when public.bbs_norm_taxon(p_subsector) in (
      'materiales para la construccion',
      'materiales para la construcción',
      'materiales para la construcciã£â³n'
    )
      or public.bbs_norm_taxon(p_category) in (
        'quimicos y soluciones para la construccion',
        'químicos y soluciones para la construcción',
        'quimicos y soluciones para la construcciã£â³n'
      ) then 'Materials & Supplies'
    when public.bbs_norm_taxon(p_category) = 'tiendas de descuento' then 'Discount Retail'
    when public.bbs_norm_taxon(p_category) = 'tiendas de ropa' then 'Apparel Stores'
    when public.bbs_norm_taxon(p_category) = 'calzado deportivo' then 'Athletic Footwear'
    when public.bbs_norm_taxon(p_subsector) = 'calzado' then 'Footwear'
    when public.bbs_norm_taxon(p_category) = 'farmacias' then 'Pharmacies'
    when public.bbs_norm_taxon(p_category) = 'laboratorios' then 'Labs'
    when public.bbs_norm_taxon(p_category) = 'hospitales' then 'Hospitals'
    when public.bbs_norm_taxon(p_category) in ('clinicas','clínicas') then 'Clinics'
    when public.bbs_norm_taxon(p_category) = 'cervezas' then 'Beer'
    when public.bbs_norm_taxon(p_category) in ('bebidas alcoholicas', 'bebidas alcohólicas') then 'Alcoholic Drinks'
    when public.bbs_norm_taxon(p_category) in ('bebidas no alcoholicas', 'bebidas no alcohólicas') then 'Non-Alcoholic Drinks'
    when public.bbs_norm_taxon(p_subsector) = 'bebidas' then 'Beverage'
    when public.bbs_norm_taxon(p_subsector) = 'e-commerce' then 'E-commerce'
    when public.bbs_norm_taxon(p_subsector) = 'autoservicios' then 'Mass Retail'
    when public.bbs_norm_taxon(p_subsector) = 'comercio especializado' then 'Specialty Stores'
    when public.bbs_norm_taxon(p_sector) = 'comercio' then 'General Retail'
    when public.bbs_norm_taxon(p_sector) = 'servicios' then 'General Services'
    when public.bbs_norm_taxon(p_sector) = 'industria' then 'General Industry'
    when public.bbs_norm_taxon(p_sector) = 'servicios financieros' then 'Financial Products'
    when coalesce(nullif(trim(p_category),''), '') = '' then coalesce(nullif(trim(p_subsector),''), coalesce(nullif(trim(p_sector),''), 'Unassigned'))
    else coalesce(nullif(trim(p_category),''), 'Unassigned')
  end;
$$;
