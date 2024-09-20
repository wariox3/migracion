import pandas as pd
import json

# Leer el archivo Excel
excel_file = '/home/desarrollo/Escritorio/administradoras.xlsx'  # Reemplaza esto con la ruta de tu archivo Excel
#df = pd.read_excel(excel_file)
df = pd.read_excel(excel_file, dtype={'codigo': str})
# Transformar los datos al formato requerido
cuentas = []

for index, row in df.iterrows():
    cuenta = {
        "model": "humano.HumEntidad",
        "pk": row['id'],
        "fields": {
            "codigo": row['codigo'],
            "numero_identificacion": row['numero_identificacion'],
            "nombre": row['nombre'],
            "nombre_extendido": row['nombre_extendido'],
            "salud": True if row['salud'] == 1 else False,
            "pension": True if row['pension'] == 1 else False,
            "caja": True if row['caja'] == 1 else False,
            "riesgo": True if row['riesgo'] == 1 else False,
            "sena": True if row['sena'] == 1 else False,
            "icbf": True if row['icbf'] == 1 else False
        }
    }
    cuentas.append(cuenta)

# Convertir la lista de cuentas a JSON
json_data = json.dumps(cuentas, indent=4, ensure_ascii=False)


# Guardar el JSON en un archivo
with open('/home/desarrollo/Escritorio/generar_entidad.json', 'w', encoding='utf-8') as json_file:
    json_file.write(json_data)

print("El archivo JSON se ha creado exitosamente.")