import re
import psycopg2
from datetime import datetime
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
cursor = conexion.cursor()
log_pattern = re.compile(r'(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+ (?:[^"]|")*)" (\d+) (\d+) "([^"]*)" "([^"]*)"')

with open("/home/desarrollo/Escritorio/access.log.14", "r") as log_file:
    contador = 0
    for line in log_file:
        match = log_pattern.match(line)
        if match:
            ip, remote_user, local_user, local_date_time, request, status_code, response_size, referer, user_agent = match.groups()
            requestSeparado = request.split()
            requestMetodo = requestSeparado[0]
            requestUrl = requestSeparado[1]
            requestProtocolo = requestSeparado[2]
            #fecha_parseada = datetime.strptime(local_date_time, "%d/%b/%Y:%H:%M:%S %z")
            #print(fecha_parseada)
            sql = f"INSERT INTO apache_acceso (ip, remote_user, local_user, date_local, request_metod, request_url, request_protocol, status_code, response_size, referer, user_agent) VALUES ('{ip}','{remote_user}','{local_user}','{local_date_time}','{requestMetodo}','{requestUrl}','{requestProtocolo}',{status_code},{response_size},'{referer}','{user_agent}')"
            cursor.execute(sql)                        
            conexion.commit()
        contador += 1
        #if contador >= 5:
        #    break
        print(f"Linea {contador}")
print("Termino el proceso satisfactoriamente")        
conexion.close()
