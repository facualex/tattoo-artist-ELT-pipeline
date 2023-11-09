SELECT 
    md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nombre as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tatuaje as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fecha as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as time_id,
    TO_DATE(fecha, 'DD-MM-YYYY') as fecha_tattoo,
    TO_DATE("fecha pago", 'DD-MM-YYYY') AS fecha_pago,
    TO_CHAR(TO_DATE(fecha, 'DD-MM-YYYY'), 'MM')::INTEGER as mes_tattoo,
    TO_CHAR(TO_DATE(fecha, 'DD-MM-YYYY'), 'YYYY') as año_tattoo,
    TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'MM')::INTEGER as mes_pago,
    TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'YYYY') as año_pago
FROM
    tattoo_raw_data