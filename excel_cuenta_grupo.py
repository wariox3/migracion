import pandas as pd
import json

# Leer el archivo Excel
excel_file = '/home/desarrollo/Escritorio/cuenta_grupo.xlsx'  # Reemplaza esto con la ruta de tu archivo Excel
df = pd.read_excel(excel_file, dtype={'codigo': str})
cuentas = []

for index, row in df.iterrows():
    cuenta = {
        "model": "contabilidad.ConCuentaGrupo",
        "pk": row['id'],
        "fields": {
            "nombre": row['nombre']
        }
    }
    cuentas.append(cuenta)
json_data = json.dumps(cuentas, indent=4, ensure_ascii=False)
with open('/home/desarrollo/Escritorio/cuenta_grupo.json', 'w', encoding='utf-8') as json_file:
    json_file.write(json_data)
print("El archivo JSON se ha creado exitosamente.")