
  
    

  create  table "tattoo_raw_data"."public"."fact_tattoo__dbt_tmp"
  
  
    as
  
  (
    WITH fct_tattoo_cte AS (
    SELECT
        md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nombre as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tatuaje as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(lugar as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fecha as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as fact_id,
        md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nombre as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as client_id,
        md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(lugar as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as location_id,
        md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nombre as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tatuaje as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as tattoo_id,
        md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nombre as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tatuaje as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fecha as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as time_id,
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
  );
  