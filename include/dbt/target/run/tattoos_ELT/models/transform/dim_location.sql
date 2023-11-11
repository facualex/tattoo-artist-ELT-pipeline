
  
    

  create  table "tattoo_raw_data"."public"."dim_location__dbt_tmp"
  
  
    as
  
  (
    SELECT DISTINCT 
    md5(cast(coalesce(cast(lugar as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as location_id,
    lugar
FROM
    tattoo_raw_data
  );
  