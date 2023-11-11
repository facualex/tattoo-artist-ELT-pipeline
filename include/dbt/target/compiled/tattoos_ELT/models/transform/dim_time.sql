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
    md5(cast(coalesce(cast(fecha as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as time_id,
    TO_DATE(fecha, 'DD-MM-YYYY') as fecha_tattoo,
    TO_DATE("fecha pago", 'DD-MM-YYYY') AS fecha_pago,
    TO_CHAR(TO_DATE(fecha, 'DD-MM-YYYY'), 'MM')::INTEGER as mes_tattoo,
    TO_CHAR(TO_DATE(fecha, 'DD-MM-YYYY'), 'YYYY') as año_tattoo,
    TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'MM')::INTEGER as mes_pago,
    TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'YYYY') as año_pago,
    ME.total_earnings as month_earnings
FROM
    tattoo_raw_data
JOIN
    MonthlyEarnings ME ON TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'MM')::INTEGER = ME.mes_pago
                      AND TO_CHAR(TO_DATE("fecha pago", 'DD-MM-YYYY'), 'YYYY') = ME.año_pago