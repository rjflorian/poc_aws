#!/bin/bash

nombre_bucket="raw-my-sql-input/mysql-telegram/"
carpeta_local1="parquet_files/"

subir_carpeta_s3() {
  local carpeta_local="$1"
  local nombre_bucket="$2"

  if [ ! -d "$carpeta_local" ]; then
    echo "Error: La carpeta $carpeta_local no existe."
    return 1
  fi

  aws s3 cp "$carpeta_local" "s3://$nombre_bucket" --recursive
  if [ $? -eq 0 ]; then
    echo "Carpeta $carpeta_local subida a S3 correctamente."
    return 0
  else
    echo "Error: No se pudo subir la carpeta $carpeta_local a S3."
    return 1
  fi
}

subir_carpeta_s3 "$carpeta_local1" "$nombre_bucket"


echo "Proceso completado."
