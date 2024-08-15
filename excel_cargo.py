import pandas as pd
import json

# Leer el archivo Excel
excel_file = '/home/desarrollo/Escritorio/generar_cargo.xlsx'  # Reemplaza esto con la ruta de tu archivo Excel
#df = pd.read_excel(excel_file)
df = pd.read_excel(excel_file, dtype={'codigo': str})
# Transformar los datos al formato requerido
cuentas = []

for index, row in df.iterrows():
    cuenta = {
        "model": "humano.HumCargo",
        "pk": row['id'],
        "fields": {
            "codigo": row['codigo'],
            "nombre": row['nombre'],
            "estado_inactivo": row['estado_inactivo']
        }
    }
    cuentas.append(cuenta)

# Convertir la lista de cuentas a JSON
json_data = json.dumps(cuentas, indent=4, ensure_ascii=False)


# Guardar el JSON en un archivo
with open('/home/desarrollo/Escritorio/generar_cargo.json', 'w', encoding='utf-8') as json_file:
    json_file.write(json_data)

print("El archivo JSON se ha creado exitosamente.")