SELECT DISTINCT 
    {{ dbt_utils.generate_surrogate_key(['lugar']) }} as location_id,
    INITCAP(lugar) as lugar
FROM
    tattoo_raw_data