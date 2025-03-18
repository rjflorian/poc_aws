import pandas as pd
from sqlalchemy import create_engine
import logging
import os
import threading
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def export_table_to_parquet(usuario, contraseña, nombre_base_datos, tabla, carpeta_destino):
    try:
        engine = create_engine(f'mysql+mysqlconnector://{usuario}:{contraseña}@localhost/{nombre_base_datos}')

        tabla_carpeta = os.path.join(carpeta_destino, tabla)
        if not os.path.exists(tabla_carpeta):
            os.makedirs(tabla_carpeta)

        consulta = f"select * FROM {tabla}"
        df = pd.read_sql(consulta, engine)

        logging.info(f"DataFrame para la tabla {tabla}:\n{df}")

        parquet_file = os.path.join(tabla_carpeta, f'{tabla}.parquet')

        logging.info(f"Intentando convertir a Parquet: {parquet_file}")

        df.to_parquet(parquet_file, index=False)

        logging.info(f"Archivo Parquet creado: {parquet_file}")
        logging.info(f"Tabla {tabla} exportada a Parquet correctamente.")

    except Exception as e:
        logging.error(f"Error al procesar la tabla {tabla}: {e}")

def export_batch(usuario, contraseña, nombre_base_datos, tablas, carpeta_destino):
    threads = []
    for tabla in tablas:
        thread = threading.Thread(target=export_table_to_parquet, args=(usuario, contraseña, nombre_base_datos, tabla, carpeta_destino))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def export_mysql_to_parquet_folders_batched(usuario, contraseña, nombre_base_datos, tablas, batch_size=1, carpeta_destino='parquet_files'):
    if not os.path.exists(carpeta_destino):
        os.makedirs(carpeta_destino)

    for i in range(0, len(tablas), batch_size):
        batch = tablas[i:i + batch_size]
        logging.info(f"Iniciando lote: {batch}")
        export_batch(usuario, contraseña, nombre_base_datos, batch, carpeta_destino)
        logging.info(f"Lote completado: {batch}")

usuario = ""
contraseña = ""
nombre_base_datos = "DATALOAD"
tablas = ["GRUPO", "DATA_LOAD"]
carpeta_destino = "parquet_files"
batch_size = 2
start_time = time.time()
export_mysql_to_parquet_folders_batched(usuario, contraseña, nombre_base_datos, tablas, batch_size=batch_size, carpeta_destino=carpeta_destino)
end_time = time.time()
elapsed_time = end_time - start_time
logging.info(f"Tiempo total de ejecución: {elapsed_time:.2f} segundos")