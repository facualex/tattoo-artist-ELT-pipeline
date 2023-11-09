SELECT 
    md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(lugar as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as location_id,
    lugar
FROM
    tattoo_raw_data