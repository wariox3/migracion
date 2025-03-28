import boto3
from decouple import config

b2 = boto3.client(
    's3',
    endpoint_url="https://s3.us-east-005.backblazeb2.com",  # Ajusta la región si es necesario
    aws_access_key_id=config('B2_APPLICATION_KEY_ID'),
    aws_secret_access_key=config('B2_APPLICATION_KEY')
)

def obtener_tamano_directorios():
    # Diccionario para almacenar el peso de cada directorio raíz
    tamaños = {}

    # Obtener todos los objetos del bucket
    paginator = b2.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=config('B2_BUCKET_NAME')):
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                size = obj["Size"]

                # Extraer el directorio raíz (lo que está antes de la primera '/')
                root_dir = key.split("/")[0]

                # Sumar el tamaño de cada archivo al directorio correspondiente
                if root_dir in tamaños:
                    tamaños[root_dir] += size
                else:
                    tamaños[root_dir] = size

    return tamaños

# Obtener y mostrar los tamaños
tamaños = obtener_tamano_directorios()
for directorio, peso in tamaños.items():
    #print(f"{directorio}: {peso / (1024 * 1024):.2f} MB")
    print(f"{directorio}: {peso / (1024 ** 3):.2f} GB")