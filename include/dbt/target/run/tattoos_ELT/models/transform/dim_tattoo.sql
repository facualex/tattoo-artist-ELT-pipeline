
  
    

  create  table "tattoo_raw_data"."public"."dim_tattoo__dbt_tmp"
  
  
    as
  
  (
    SELECT 
    md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nombre as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tatuaje as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as tattoo_id,
    INITCAP(tatuaje) as tatuaje,
    INITCAP(estilo) as estilo,
    INITCAP(zona) as zona,
    NULLIF(NULLIF(REPLACE(tiempo, ' min', ''), ''), 'min')::INTEGER AS tiempo,
    "necesita repaso?" AS necesita_repaso,
    "repaso hecho?" AS repaso_hecho,
    INITCAP(diseño) as diseño,
    pagado
FROM
    tattoo_raw_data
  );
  