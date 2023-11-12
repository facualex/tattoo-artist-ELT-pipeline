
  
    

  create  table "tattoo_raw_data"."public"."dim_time__dbt_tmp"
  
  
    as
  
  (
    WITH MonthlyEarnings AS (
    SELECT
        TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'MM')::INTEGER as mes_pago,
        TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'YYYY') as año_pago,
        SUM(REPLACE(SUBSTRING(precio FROM 2), '.', '')::INTEGER) AS total_earnings
    FROM
        tattoo_raw_data
    WHERE
        pagado = TRUE
    GROUP BY
        TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'MM')::INTEGER,
        TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'YYYY')
)

SELECT DISTINCT 
    -- Generate a surrogate key based on the 'fecha' column
    md5(cast(coalesce(cast(fecha as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as time_id,
    
    -- Convert 'fecha' column to a date format
    TO_DATE(fecha, 'DD-MM-YYYY') as fecha_tattoo,
    
    -- Convert 'fecha pago' column to a date format
    TO_DATE("fecha pago", 'DD-MM-YYYY') AS fecha_pago,
    
    -- Extract the month from 'fecha' and convert it to an integer
    TO_CHAR(TO_DATE(fecha, 'DD-MM-YYYY'), 'MM')::INTEGER as mes_tattoo,
    
    -- Extract the year from 'fecha' 
    TO_CHAR(TO_DATE(fecha, 'DD-MM-YYYY'), 'YYYY') as año_tattoo,
    
    -- Extract the month from 'fecha pago' and convert it to an integer
    TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'MM')::INTEGER as mes_pago,
    
    -- Extract the year from 'fecha pago'
    TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'YYYY') as año_pago,
    
    -- Retrieve the total earnings from the MonthlyEarnings table
    ME.total_earnings as month_earnings

FROM (
    SELECT
        -- Original columns
        fecha,
        "fecha pago",
        -- Assign a unique row number within each group of identical dates
        ROW_NUMBER() OVER (PARTITION BY fecha ORDER BY "fecha pago") AS row_num
    FROM
        tattoo_raw_data
) AS RankedData

JOIN
    MonthlyEarnings ME ON TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'MM')::INTEGER = ME.mes_pago
                      AND TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'YYYY') = ME.año_pago
WHERE
    -- Filter out rows with row_num greater than 1, indicating duplicates
    row_num = 1
  );
  