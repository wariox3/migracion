import psycopg2
import json
from decimal import Decimal
from decouple import config

conexionPS = psycopg2.connect(user=config('PG_USUARIO'),
                                password=config('PG_CLAVE'),
                                host=config('PG_SERVIDOR'),
                                port=config('PG_PUERTO'),
                                database=config('PG_BASEDATOS'))

cursorPg = conexionPS.cursor()
cursorPg.execute(f"SET search_path TO gamboa")

cursorPg.execute("SELECT id, nombre, codigo_postal, latitud, longitud, estado_id FROM gen_ciudad")
results = cursorPg.fetchall()

ciudades_json = []
for result in results:
    ciudad_json = {
        "model": "general.ciudad",
        "pk": result[0],
        "fields": {
            "nombre": result[1],
            "codigo_postal": result[2],
            "latitud": result[3],
            "longitud": result[4],
            "estado": result[5],
            "codigo": result[2],
        }
    }
    ciudades_json.append(ciudad_json)
    print(result[3])

# Escribir el JSON en un archivo
with open('/home/desarrollo/Escritorio/ciudades.json', 'w', encoding='utf-8') as file:
    json.dump(ciudades_json, file, indent=4, ensure_ascii=False)

cursorPg.close()
conexionPS.close()