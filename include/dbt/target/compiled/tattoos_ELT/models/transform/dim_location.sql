SELECT DISTINCT 
    md5(cast(coalesce(cast(lugar as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as location_id,
    INITCAP(lugar) as lugar
FROM
    tattoo_raw_data