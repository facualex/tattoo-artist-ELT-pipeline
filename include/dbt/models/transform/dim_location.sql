SELECT 
    {{ dbt_utils.generate_surrogate_key(['id', 'lugar']) }} as location_id,
    lugar
FROM
    tattoo_raw_data