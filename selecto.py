import mysql.connector
import psycopg2
from decouple import config

codigo_empresa = 16

conexion = mysql.connector.connect(
    host=config('MYSQL_SERVIDOR'),
    user=config('MYSQL_USUARIO'),
    password=config('MYSQL_CLAVE'),
    database=config('MYSQL_BASEDATOS'))

conexionPS = psycopg2.connect(user=config('PG_USUARIO'),
                                password=config('PG_CLAVE'),
                                host=config('PG_SERVIDOR'),
                                port=config('PG_PUERTO'),
                                database=config('PG_BASEDATOS'))

cursorPg = conexionPS.cursor()
cursorPg.execute(f"SET search_path TO {config('PG_ESQUEMA')}")
cursorMysql = conexion.cursor()
 
def entero_a_boolean(valor_entero):
    return valor_entero != 0

def identificacion(valor):
    respuesta = 6
    if valor == "CC":
        respuesta = 3
    if valor == "CE":
        respuesta = 5
    if valor == "NI":
        respuesta = 6
    if valor == "PE":
        respuesta = 7
    if valor == "RC":
        respuesta = 1
    if valor == "TDE":
        respuesta = 8
    if valor == "TE":
        respuesta = 4
    if valor == "TI":
        respuesta = 2
    return respuesta

def formatear_texto(texto):
    if texto is None:
        texto = "NULL"
    else:
        texto = texto.replace("'", "")
        texto = f"'{texto}'" 
    return texto

def importar_item():
    try: 
        cursorMysql.execute(f"SELECT codigo_item_pk, nombre, vr_precio, codigo, referencia, producto, servicio, afecta_inventario, codigo_impuesto_iva_venta_fk FROM item where codigo_empresa_fk = {codigo_empresa}")
        registros = cursorMysql.fetchall()
        for registro in registros:
            print(registro[0])
            producto = entero_a_boolean(registro[5])
            servicio = entero_a_boolean(registro[6])
            inventario = entero_a_boolean(registro[7])
            if registro[1] is None:
                nombre = "NULL"
            else:
                nombre = registro[1]
                nombre = nombre.replace("'", "")
                nombre = f"'{nombre}'"
            sql = f"INSERT INTO gen_item (id, nombre, costo, precio, codigo, referencia, producto, servicio, inventario, existencia, disponible) VALUES ({registro[0]}, {nombre}, 0, {registro[2]}, {registro[3]}, '{registro[4]}', {producto}, {servicio}, {inventario}, 0, 0)"
            cursorPg.execute(sql)
            if registro[8] == "I19":
                cursorPg.execute(f"INSERT INTO gen_item_impuesto (impuesto_id, item_id) VALUES (1,{registro[0]})")        
        conexionPS.commit()
    except psycopg2.Error as e:
        print(f"Error al insertar registros: {sql}", e)

def importar_tercero():
    try: 
        cursorMysql.execute(f"SELECT codigo_tercero_pk, numero_identificacion, nombre_corto, direccion, telefono, celular, email, \
                            codigo_regimen_fk, codigo_tipo_persona_fk, digito_verificacion, primer_nombre, segundo_nombre, primer_apellido, segundo_apellido, \
                            codigo_postal, barrio, codigo_ciuu, plazo_pago, codigo_identificacion_fk, ciudad.codigo_reddoc \
                            FROM tercero LEFT JOIN ciudad ON tercero.codigo_ciudad_fk = ciudad.codigo_ciudad_pk where codigo_empresa_fk = {codigo_empresa}")
        registros = cursorMysql.fetchall()
        for registro in registros:
            print(registro[0])
            if(registro[7] == "O"):
                regimen_id = 1
            else:
                regimen_id = 2

            if(registro[8] == "J"):
                tipo_persona_id = 1
            else:
                tipo_persona_id = 2
            
            plazo_pago_id = 1
            if registro[17] == 15:
                plazo_pago_id = 3
            if registro[17] == 30:
                plazo_pago_id = 4

            identificacion_id = identificacion(registro[18])

            #producto = entero_a_boolean(registro[5]) 
            if registro[2] is None:
                nombre_corto = "NULL"
            else:
                nombre_corto = registro[2]
                nombre_corto = nombre_corto.replace("'", "")
                nombre_corto = f"'{nombre_corto}'"

            if registro[4] is None:
                telefono = "NULL"
            else:
                telefono = registro[4]
                telefono = telefono.replace("'", "")
                telefono = f"'{telefono}'"
                

            if registro[5] is None:
                celular = "NULL"
            else:
                celular = f"'{registro[5]}'"

            if registro[15] is None:
                barrio = "NULL"
            else:
                barrio = f"'{registro[15]}'"

            if registro[16] is None:
                codigo_ciuu = "NULL"
            else:
                codigo_ciuu = f"'{registro[16]}'"

            if registro[10] is None:
                nombre1 = "NULL"
            else:
                nombre1 = f"'{registro[10]}'"

            if registro[11] is None:
                nombre2 = "NULL"
            else:
                nombre2 = f"'{registro[11]}'"

            if registro[12] is None:
                apellido1 = "NULL"
            else:
                apellido1 = f"'{registro[12]}'"

            if registro[13] is None:
                apellido2 = "NULL"
            else:
                apellido2 = f"'{registro[13]}'"

            sql = f"INSERT INTO gen_contacto (id, numero_identificacion, nombre_corto, direccion, telefono, celular, correo, ciudad_id, identificacion_id, \
                            regimen_id, tipo_persona_id, digito_verificacion, nombre1, nombre2, apellido1, apellido2, \
                            codigo_postal, barrio, codigo_ciuu, plazo_pago_id) \
                            VALUES ({registro[0]}, '{registro[1]}', {nombre_corto}, '{registro[3]}', {telefono}, {celular}, '{registro[6]}', {registro[19]}, {identificacion_id}, \
                            {regimen_id}, {tipo_persona_id}, {registro[9]}, {nombre1}, {nombre2}, {apellido1}, {apellido2}, \
                            '{registro[14]}', {barrio}, {codigo_ciuu}, {plazo_pago_id})"
            cursorPg.execute(sql)
    except psycopg2.Error as e:
        print(f"Error al insertar registros: {sql}", e)
    conexionPS.commit()

def importar_resolucion():
    try: 
        cursorMysql.execute(f"SELECT codigo_resolucion_pk, numero_desde, numero_hasta, fecha_desde, fecha_hasta, prefijo, numero, ambiente FROM resolucion where codigo_empresa_fk = {codigo_empresa}")
        registros = cursorMysql.fetchall()
        for registro in registros:
            print(registro[0])
            numero = round(registro[6])        
            sql = f"INSERT INTO gen_resolucion (id, consecutivo_desde, consecutivo_hasta, fecha_desde, fecha_hasta, prefijo, numero) \
                    VALUES ({registro[0]}, {registro[1]}, {registro[2]}, '{registro[3]}', '{registro[4]}', '{registro[5]}', '{numero}')"
            cursorPg.execute(sql)      
        conexionPS.commit()
    except psycopg2.Error as e:
        print(f"Error al insertar registros: {sql}", e)

def importar_movimiento():
    try: 
        cursorMysql.execute(f"SELECT codigo_movimiento_pk, codigo_movimiento_tipo_fk, vr_subtotal, vr_total_bruto, vr_total_neto, \
                            numero, fecha, fecha_vence, vr_base_iva, vr_iva, codigo_tercero_fk, codigo_resolucion_fk, comentario, cue, \
                            documento_soporte, codigo_movimiento_fk \
                            FROM movimiento where codigo_empresa_fk = {codigo_empresa} AND (codigo_movimiento_tipo_fk = 'FAC' OR codigo_movimiento_tipo_fk = 'NC')")
        registros = cursorMysql.fetchall()
        for registro in registros:
            print(f"{registro[0]} {registro[1]}")  
            documentoTipo = 1
            if registro[1] == 'FAC':
                documentoTipo = 1
            if registro[1] == 'NC':
                documentoTipo = 2   
            if registro[15] is None:
                documentoReferencia = "NULL"
            else:
                documentoReferencia = registro[15]
            soporte = formatear_texto(registro[14])
            comentario = formatear_texto(registro[12])                
            sql = f"INSERT INTO gen_documento (id, subtotal, total_bruto, total, descuento, estado_aprobado, estado_anulado, estado_electronico, estado_electronico_enviado, estado_electronico_notificado, \
                            documento_tipo_id, empresa_id, metodo_pago_id, cobrar, cobrar_afectado, cobrar_pendiente, numero, fecha, \
                            fecha_contable, fecha_vence, base_impuesto, impuesto, contacto_id, resolucion_id, comentario, cue, \
                            soporte, documento_referencia_id) \
                            VALUES ({registro[0]}, {registro[2]}, {registro[3]}, {registro[4]}, 0, false, false, false, false, false, \
                            {documentoTipo}, 1, 1, 0, 0, 0, {registro[5]}, '{registro[6]}', \
                            '{registro[6]}', '{registro[7]}', {registro[8]}, {registro[9]}, {registro[10]}, {registro[11]}, {comentario}, '{registro[13]}', \
                            {soporte}, {documentoReferencia})"            
            cursorPg.execute(sql)
            cursorMysql.execute(f"SELECT codigo_movimiento_detalle_pk, cantidad, vr_precio, vr_subtotal, vr_total, porcentaje_descuento, codigo_item_fk, \
                                porcentaje_iva, vr_base_iva, vr_iva \
                            FROM movimiento_detalle where codigo_movimiento_fk = {registro[0]}")
            registrosDetalles = cursorMysql.fetchall()
            for registroDetalle in registrosDetalles:
                print(f"Detalle: {registroDetalle[0]}")
                sql = f"INSERT INTO gen_documento_detalle (id, cantidad, precio, subtotal, total_bruto, total, \
                                descuento, porcentaje_descuento, documento_id, item_id) \
                                VALUES ({registroDetalle[0]}, {registroDetalle[1]}, {registroDetalle[2]}, {registroDetalle[3]}, {registroDetalle[4]}, {registroDetalle[4]}, \
                                0, {registroDetalle[5]}, {registro[0]}, {registroDetalle[6]})" 
                cursorPg.execute(sql)
                if registroDetalle[7] == 19:
                    sql = f"INSERT INTO gen_documento_impuesto (base, porcentaje, total, documento_detalle_id, impuesto_id) \
                        VALUES ({registroDetalle[8]}, {registroDetalle[7]}, {registroDetalle[9]}, {registroDetalle[0]}, 1)" 
                    cursorPg.execute(sql)
    except psycopg2.Error as e:
        print(f"Error al insertar registros: {sql}", e)
    conexionPS.commit()

#importar_item()  
#importar_tercero()
#importar_resolucion()    
#importar_movimiento()    

cursorMysql.close()
conexion.close()
cursorPg.close()
conexionPS.close()