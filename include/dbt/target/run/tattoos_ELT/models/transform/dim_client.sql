
  
    

  create  table "tattoo_raw_data"."public"."dim_client__dbt_tmp"
  
  
    as
  
  (
    SELECT 
    md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nombre as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as client_id,
    nombre 
FROM
    tattoo_raw_data
  );
  