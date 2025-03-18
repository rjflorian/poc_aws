#!/bin/bash

nombre_bucket="raw-my-sql-input/objetos_athenas/"
carpeta_local="sql/" 

if [ ! -d "$carpeta_local" ]; then
    echo "Error: La carpeta $carpeta_local no existe."
    exit 1
fi

aws s3 cp "$carpeta_local" "s3://$nombre_bucket" --recursive
if [ $? -eq 0 ]; then
    echo "Carpeta $carpeta_local subida a S3 correctamente."
else
    echo "Error: No se pudo subir la carpeta $carpeta_local a S3."
fi