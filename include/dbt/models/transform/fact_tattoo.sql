WITH fct_tattoo_cte AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id', 'nombre', 'tatuaje', 'lugar', 'fecha']) }} as fact_id,
        {{ dbt_utils.generate_surrogate_key(['id', 'nombre']) }} as client_id,
        {{ dbt_utils.generate_surrogate_key(['id', 'lugar']) }} as location_id,
        {{ dbt_utils.generate_surrogate_key(['id', 'nombre', 'tatuaje']) }} as tattoo_id,
        {{ dbt_utils.generate_surrogate_key(['id', 'nombre', 'tatuaje', 'fecha']) }} as time_id,
        precio
    FROM tattoo_raw_data
)

SELECT
    -- Assuming the existence of suitable fields in your source data for appointment_id, cantidad_tatuajes, and monto_pago
    fact_id,
    dc.client_id,
    dt.tattoo_id,
    dl.location_id,
    dtime.time_id,
    CAST(REPLACE(SUBSTRING(precio FROM 2), '.', '') AS INTEGER) AS precio
FROM
    fct_tattoo_cte AS ft
JOIN
    dim_client dc ON ft.client_id = dc.client_id
JOIN
    dim_location dl ON ft.location_id = dl.location_id
JOIN
    dim_tattoo dt ON ft.tattoo_id = dt.tattoo_id
JOIN
    dim_time dtime ON ft.time_id = dtime.time_id