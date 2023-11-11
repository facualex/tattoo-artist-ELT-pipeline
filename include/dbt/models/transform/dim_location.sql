SELECT DISTINCT 
    {{ dbt_utils.generate_surrogate_key(['lugar']) }} as location_id,
    lugar
FROM
    tattoo_raw_data