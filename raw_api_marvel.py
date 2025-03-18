import hashlib
import time
import requests
import csv
import threading
import os

def generar_hash(timestamp, clave_privada, clave_publica):
    hash_md5 = hashlib.md5()
    hash_md5.update(f'{timestamp}{clave_privada}{clave_publica}'.encode('utf-8'))
    return hash_md5.hexdigest()

def obtener_personajes(clave_publica, clave_privada, nombre_personaje=None, limite=20, desplazamiento=0):
    timestamp = str(time.time())
    hash_generado = generar_hash(timestamp, clave_privada, clave_publica)

    url = 'http://gateway.marvel.com/v1/public/characters'
    parametros = {
        'ts': timestamp,
        'apikey': clave_publica,
        'hash': hash_generado,
        'limit': limite,
        'offset': desplazamiento
    }

    if nombre_personaje:
        parametros['nameStartsWith'] = nombre_personaje

    respuesta = requests.get(url, params=parametros)

    if respuesta.status_code == 200:
        datos = respuesta.json()
        return datos['data']['results']
    else:
        print(f'Error: {respuesta.status_code}')
        return None

def guardar_personajes_csv(personajes, nombre_archivo):
    with open(nombre_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
        escritor_csv = csv.writer(archivo_csv)
        escritor_csv.writerow(['ID', 'Nombre', 'Descripción'])  

        for personaje in personajes:
            escritor_csv.writerow([personaje['id'], personaje['name'], personaje['description']])

def procesar_personajes(clave_publica, clave_privada, nombre_personaje=None, limite=20, desplazamiento=0, nombre_archivo='personajes.csv'):
    personajes = obtener_personajes(clave_publica, clave_privada, nombre_personaje, limite, desplazamiento)
    if personajes:
        guardar_personajes_csv(personajes, nombre_archivo)
        print(f'Personajes guardados en {nombre_archivo}')

def procesar_personajes_con_hilos(clave_publica, clave_privada, nombre_personaje=None, limite=20, offset_por_hilo=100, nombre_archivo_base='personajes'):
    """Procesa personajes con hilos."""
    if not os.path.exists('csv_files/personajes'):
        os.makedirs('csv_files/personajes')
    threads = []
    for i in range(0, 1000, offset_por_hilo): 
        nombre_archivo = f'csv_files/personajes/{nombre_archivo_base}_{i}.csv'
        thread = threading.Thread(target=procesar_personajes, args=(clave_publica, clave_privada, nombre_personaje, limite, i, nombre_archivo))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


clave_publica = ''
clave_privada = ''

procesar_personajes_con_hilos(clave_publica, clave_privada)