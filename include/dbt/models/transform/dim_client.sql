SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['nombre']) }} as client_id,
    INITCAP(nombre) as nombre
FROM
    tattoo_raw_data