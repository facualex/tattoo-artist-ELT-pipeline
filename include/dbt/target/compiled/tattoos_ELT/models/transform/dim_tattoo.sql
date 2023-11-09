SELECT 
    md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nombre as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tatuaje as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as tattoo_id,
    tatuaje,
    estilo,
    zona,
    tiempo,
    "necesita repaso?" AS necesita_repaso,
    "repaso hecho?" AS repaso_hecho,
    diseño,
    pagado
FROM
    tattoo_raw_data