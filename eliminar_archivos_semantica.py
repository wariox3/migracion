import mysql.connector
#import paramiko
from decouple import config
import os

empresa = "veloenvios"
directorio_raiz = f"/mnt/almacenamiento_{empresa}/{empresa}/masivo/TteGuia"
conexion = mysql.connector.connect(
    host=config('MYSQL_SERVIDOR'),
    port=config('MYSQL_PUERTO'),
    user=config('MYSQL_USUARIO'),
    password=config('MYSQL_CLAVE'),
    database=config('MYSQL_BASEDATOS'))
cursorMysql = conexion.cursor()

#host = config('SFTP_HOST')
#port = 22
#username = config('SFTP_USER')
#password = config('SFTP_PASSWORD')

#client = paramiko.SSHClient()
#client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

def limpiar_errores_identificacion():
    client.connect(host, port, username, password)
    sftp = client.open_sftp()   
    cursorMysql.execute(f"SELECT codigo_masivo_pk, directorio, archivo_destino \
                        FROM doc_masivo dm \
                        WHERE dm.codigo_masivo_tipo_fk  = 'TteGuia' AND dm.identificador IS NOT NULL AND dm.identificador NOT REGEXP '^[0-9]+$' LIMIT 1")
    registros = cursorMysql.fetchall()
    for registro in registros: 
        ruta = f"{directorio_raiz}/{registro[1]}/{registro[2]}"
        mensaje = ""
        try:
            sftp.stat(ruta)
            sftp.remove(ruta)
            mensaje = "Archivo eliminado"            
        except FileNotFoundError:
            mensaje = "No existe"
        try:
            cursorMysql.execute(f"DELETE FROM doc_masivo where codigo_masivo_pk={registro[0]}")
            mensaje = f"{mensaje} / Registro eliminado"
        except mysql.connector.Error as error:
            mensaje = f"{mensaje} / No se pudo eliminar el registro: {error}"            
        print(f"{registro[0]} {ruta} {mensaje}")    
    conexion.commit()

def limpiar_errores_singuia():
    client.connect(host, port, username, password)
    sftp = client.open_sftp()
    
    cursorMysql.execute(f"SELECT codigo_masivo_pk, directorio, archivo_destino, identificador \
                        FROM doc_masivo d left join tte_guia g on d.identificador = g.codigo_guia_pk \
                        WHERE d.codigo_masivo_tipo_fk = 'TteGuia' AND g.codigo_guia_pk IS NULL LIMIT 1")
    registros = cursorMysql.fetchall()
    for registro in registros: 
        ruta = f"{directorio_raiz}/{registro[1]}/{registro[2]}"
        mensaje = ""
        try:
            sftp.stat(ruta)
            sftp.remove(ruta)
            mensaje = "Archivo eliminado"            
        except FileNotFoundError:
            mensaje = "No existe"
        try:
            cursorMysql.execute(f"DELETE FROM doc_masivo where codigo_masivo_pk={registro[0]}")
            mensaje = f"{mensaje} / Registro eliminado"
        except mysql.connector.Error as error:
            mensaje = f"{mensaje} / No se pudo eliminar el registro: {error}"            
        print(f"{registro[0]} {ruta} {mensaje}")    
    conexion.commit()

def archivo_no_existe():    
    #client.connect(host, port, username, password)
    #sftp = client.open_sftp()
    pagina = 0
    registros_por_pagina = 100000
    correctos = 0
    invalidos = 0    
    while True:
        offset = pagina * registros_por_pagina
        cursorMysql.execute(f"SELECT d.codigo_masivo_pk, d.directorio, d.archivo_destino FROM \
                            doc_masivo d WHERE d.backup = false AND d.codigo_masivo_tipo_fk = 'TteGuia' ORDER BY d.codigo_masivo_pk LIMIT {registros_por_pagina} OFFSET {offset}")
        registros = cursorMysql.fetchall()
        if not registros or pagina == 50:        
            break   
             
        for registro in registros:         
            ruta = f"{directorio_raiz}/{registro[1]}/{registro[2]}"
            if os.path.exists(ruta):
                #print(f"{registro[0]} {ruta} archivo verificado")
                correctos = correctos + 1
            else:
                cursorMysql.execute(f"DELETE FROM doc_masivo where codigo_masivo_pk={registro[0]}")        
                invalidos = invalidos + 1
                print(f"{registro[0]} {ruta} no se encontro el archivo")
            '''try:
                sftp.stat(ruta)           
                print(f"{registro[0]} {ruta} archivo verificado")
            except FileNotFoundError:
                try:
                    #cursorMysql.execute(f"DELETE FROM doc_masivo where codigo_masivo_pk={registro[0]}")        
                    print(f"{registro[0]} {ruta} no se encontro el archivo")
                except mysql.connector.Error as error:
                    print(f"No se pudo eliminar el registro: {error}")'''
        pagina += 1   
    print(f"Correctos: {correctos} Invalidos: {invalidos}")         
    conexion.commit()        


def eliminar_anio(anio):    
    directorio_backup = f"/mnt/almacenamiento_{empresa}/{empresa}/backup/{anio}"
    #client.connect(host, port, username, password)
    #sftp = client.open_sftp()
    try:
        #sftp.stat(directorio_backup)
        os.listdir(directorio_backup)
        cursorMysql.execute(f"SELECT d.codigo_masivo_pk, d.directorio, d.archivo_destino, d.identificador \
                            FROM doc_masivo d LEFT JOIN tte_guia g ON d.identificador = g.codigo_guia_pk \
                            WHERE d.backup = false AND d.codigo_masivo_tipo_fk = 'TteGuia' AND (g.fecha_ingreso >= '{anio}-01-01 00:00' AND g.fecha_ingreso <= '{anio}-12-31 23:00') LIMIT 100000")
        registros = cursorMysql.fetchall()
        for registro in registros:         
            ruta = f"{directorio_raiz}/{registro[1]}/{registro[2]}"
            rutaDestino = f"{directorio_backup}/{registro[1]}/{registro[2]}"
            rutaDestinoRaiz = f"{directorio_backup}/{registro[1]}/"
            #try:
            #    sftp.stat(rutaDestinoRaiz)
            #except FileNotFoundError:
            #    sftp.mkdir(rutaDestinoRaiz)
            if not os.path.exists(rutaDestinoRaiz):            
                os.makedirs(rutaDestinoRaiz)
            mensaje = ""
            try:
                #sftp.stat(ruta)
                #sftp.rename(ruta, rutaDestino)
                os.listdir(directorio_backup)
                os.rename(ruta, rutaDestino)
                mensaje = "Archivo movido a backup"            
            except FileNotFoundError:
                mensaje = "No existe"
            try:
                cursorMysql.execute(f"UPDATE doc_masivo SET backup = true where codigo_masivo_pk={registro[0]}")
                mensaje = f"{mensaje} / Registro actualizado"
            except mysql.connector.Error as error:
                mensaje = f"{mensaje} / No se pudo actualizar el registro: {error}"            
            print(f"{registro[0]} {ruta} {mensaje}")
        conexion.commit()        
    except FileNotFoundError:
        print(f"El directorio {directorio_backup} no existe")

#limpiar_errores_identificacion()
#limpiar_errores_singuia()
#SELECT YEAR(g.fecha_ingreso) AS año, COUNT(*) AS total_ingresos FROM doc_masivo d LEFT JOIN tte_guia g ON d.identificador = g.codigo_guia_pk WHERE d.backup = false AND d.codigo_masivo_tipo_fk = 'TteGuia'GROUP BY YEAR(g.fecha_ingreso)
#archivo_no_existe()
eliminar_anio(2021)

cursorMysql.close()
conexion.close()