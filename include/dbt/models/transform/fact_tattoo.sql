WITH fct_tattoo_cte AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id', 'nombre', 'tatuaje', 'lugar', 'fecha']) }} as fact_id,
        {{ dbt_utils.generate_surrogate_key(['nombre']) }} as client_id,
        {{ dbt_utils.generate_surrogate_key(['lugar']) }} as location_id,
        {{ dbt_utils.generate_surrogate_key(['id', 'nombre', 'tatuaje']) }} as tattoo_id,
        {{ dbt_utils.generate_surrogate_key(['fecha']) }} as time_id,
        REPLACE(SUBSTRING(precio FROM 2), '.', '')::INTEGER AS precio
    FROM tattoo_raw_data
)

SELECT DISTINCT
    fact_id,
    dc.client_id,
    dt.tattoo_id,
    dl.location_id,
    dtime.time_id,
    precio,
    (SELECT SUM(precio) FROM fct_tattoo_cte WHERE client_id = dc.client_id GROUP BY client_id) AS total_expenditure,
    (SELECT COUNT(fact_id) FROM fct_tattoo_cte WHERE client_id = dc.client_id GROUP BY client_id) AS total_tattoos,
    (SELECT SUM(precio) FROM fct_tattoo_cte WHERE client_id = dc.client_id GROUP BY client_id) / (SELECT COUNT(fact_id) FROM fct_tattoo_cte WHERE client_id = dc.client_id GROUP BY client_id) AS average_tattoo_expenditure
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