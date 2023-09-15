import psycopg2
from decouple import config

HOST = config('BD_HOST')
DATABASE = config('BD_DATABASE')
USER = config('BD_USER')
PASSWORD = config('BD_PASSWORD')

conexion = psycopg2.connect(
    database=DATABASE,
    user=USER,
    password=PASSWORD,
    host=HOST
)

# Crea un cursor para ejecutar comandos SQL
cursor = conexion.cursor()
try:
    # Obtiene una lista de todas las tablas en la base de datos
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tablas = cursor.fetchall()
    cantidad = 0
    # Itera a través de las tablas y busca claves foráneas
    for tabla in tablas:
        tabla_nombre = tabla[0]        
        tabla_simple = tabla_nombre[4:]     
        nombre_secuencia = (f"{tabla_nombre}_codigo_{tabla_simple}_pk_seq")        
        campo = f"codigo_{tabla_simple}_pk"
        nombre_secuencia2 = (f"{tabla_nombre}_id_seq")        
        campo2 = f"id"

        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.sequences WHERE sequence_name = '{nombre_secuencia}')")
        existe_secuencia = cursor.fetchone()[0]
        if existe_secuencia:
            cursor.execute(f"SELECT MAX({campo}) FROM {tabla_nombre}")
            max_valor = cursor.fetchone()[0]
            if max_valor is not None:                
                cursor.execute(f"SELECT setval('{nombre_secuencia}', {max_valor})")
            print(f"La secuencia '{nombre_secuencia}' existe en la base de datos.")
            cantidad += 1

        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.sequences WHERE sequence_name = '{nombre_secuencia2}')")
        existe_secuencia = cursor.fetchone()[0]
        if existe_secuencia:
            cursor.execute(f"SELECT MAX({campo2}) FROM {tabla_nombre}")
            max_valor = cursor.fetchone()[0]
            if max_valor is not None:                
                cursor.execute(f"SELECT setval('{nombre_secuencia2}', {max_valor})")            
            print(f"La secuencia '{nombre_secuencia2}' existe en la base de datos.")
            cantidad += 1
    print(f"Se encontraron {cantidad} secuencias")
    cursor.close()
    conexion.commit()
    conexion.close()
except Exception as e:
    print("Error al eliminar restricciones de clave externa:", e)
