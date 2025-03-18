INSERT INTO "raw_telegram"."telegram_staging" 
SELECT usuario,
fecha,
ruta,
correo,
cast(date_format(fecha, '%Y%m') AS INT) as periodo
FROM "raw_telegram"."telegram" 
where correo !='';