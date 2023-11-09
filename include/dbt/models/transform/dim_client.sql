SELECT 
    {{ dbt_utils.generate_surrogate_key(['id', 'nombre']) }} as client_id,
    nombre 
FROM
    tattoo_raw_data