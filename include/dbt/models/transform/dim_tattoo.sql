SELECT 
    {{ dbt_utils.generate_surrogate_key(['id', 'nombre', 'tatuaje']) }} as tattoo_id,
    INITCAP(tatuaje) as tatuaje,
    INITCAP(estilo) as estilo,
    INITCAP(zona) as zona,
    NULLIF(NULLIF(REPLACE(tiempo, ' min', ''), ''), 'min')::INTEGER AS tiempo,
    "necesita repaso?" AS necesita_repaso,
    "repaso hecho?" AS repaso_hecho,
    INITCAP(diseño) as diseño,
    pagado
FROM
    tattoo_raw_data