SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['nombre']) }} as client_id,
    nombre 
FROM
    tattoo_raw_data