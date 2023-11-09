SELECT 
    {{ dbt_utils.generate_surrogate_key(['id', 'nombre', 'tatuaje']) }} as tattoo_id,
    tatuaje,
    estilo,
    zona,
    tiempo,
    "necesita repaso?" AS necesita_repaso,
    "repaso hecho?" AS repaso_hecho,
    diseño,
    pagado
FROM
    tattoo_raw_data