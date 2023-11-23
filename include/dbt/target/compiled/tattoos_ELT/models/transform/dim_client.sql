SELECT DISTINCT
    md5(cast(coalesce(cast(nombre as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as client_id,
    INITCAP(nombre) as nombre
FROM
    tattoo_raw_data