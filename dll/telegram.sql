CREATE EXTERNAL TABLE IF NOT EXISTS `raw_telegram`.`personajes_marvel` (
`ID` varchar(100),
    `Nombre` varchar(100),
    `Descripcion` varchar(100)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://raw-my-sql-input/mysql-telegram/personajes/'
TBLPROPERTIES ('classification' = 'csv', 'skip.header.line.count'='1');
SELECT * FROM "raw_telegram"."personajes_marvel"  WHERE NULLIF(TRIM(descripcion), '') IS NOT NULL LIMIT 10;

CREATE EXTERNAL TABLE IF NOT EXISTS `STAGING`.`telegram` (
`usuario` varchar(100),
    `fecha` TIMESTAMP,
    `ruta` varchar(100),
    `correo` varchar(100) )
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://raw-my-sql-input/mysql-telegram/DATA_LOAD/'
TBLPROPERTIES ('classification' = 'parquet');
SELECT * FROM "STAGING"."telegram" limit 10;


DROP TABLE `STAGING`.`telegram_staging`;
CREATE EXTERNAL TABLE IF NOT EXISTS  `STAGING`.`telegram_staging` (
    `usuario` varchar(100),
    `fecha` date,
    `ruta` varchar(100),
    `correo` varchar(100)
)
PARTITIONED BY (
    `periodo`  int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://staging-telegram-01/telegram_staging/';

SELECT * FROM "STAGING"."telegram_staging" limit 10;
DROP TABLE IF  EXISTS `STAGING`.`telegram_staging`;
DROP TABLE IF  EXISTS  `STAGING`.`telegram`;
DROP TABLE IF  EXISTS  `raw_telegram`.`personajes_marvel`;