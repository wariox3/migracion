import psycopg2

conexion = psycopg2.connect(
    host="localhost",
    database="bdenergy",
    user="mario",
    password="70143086"
)

campos_omitir = ["com_movimiento_anticipo_operacion",
"com_anticipo_operacion",
"com_anticipo_tipo_operacion",
"gen_movimiento_operacion_inventario",
"gen_movimiento_operacion_comercial",
"gen_movimiento_operacion_control",
"gen_movimiento_detalle_operacion_inventario",
"gen_movimiento_detalle_operacion_control",
"gen_movimiento_tipo_operacion_inventario",
"gen_movimiento_tipo_operacion_comercial",
"gen_movimiento_tipo_operacion_control",
"inv_informe_disponible_operacion_inventario",
"inv_informe_kardex_operacion_inventario",
"inv_informe_kardex_operacion_comercial",
"rhu_aporte_detalle_secuencia",
"rhu_aporte_detalle_tipo_cotizante",
"rhu_aporte_detalle_subtipo_cotizante",
"rhu_concepto_tipo_operacion",
"tte_factura_operacion_comercial",
"tte_factura_tipo_operacion_comercial",
"tur_pedido_clase_operacion",
"tur_soporte_contrato_incapacidad31",
"tur_soporte_contrato_licencia31",
"tur_soporte_contrato_vacacion31",
"tur_soporte_hora_incapacidad31",
"tur_soporte_hora_licencia31",
"tur_soporte_hora_vacacion31",
"ven_remision_operacion_inventario",
"ven_remision_tipo_operacion_inventario",
"ven_remision_detalle_operacion_inventario"
]
try:
    cursor = conexion.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tablas = cursor.fetchall()

    # Itera a través de las tablas y busca claves foráneas
    for tabla in tablas:
        tabla_nombre = tabla[0]
        modulo = tabla_nombre[:3]        
        #if modulo == "tte":
        print(f"Analizando tabla {tabla_nombre}")
        cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (tabla_nombre,))
        for fila_campos in cursor.fetchall():            
            nombre_campo, tipo_dato = fila_campos
            nombre_campo_completo = f"{tabla_nombre}_{nombre_campo}"
            if nombre_campo_completo not in campos_omitir:
                #print(f"Tabla {tabla_nombre} campo {nombre_campo} tipo {tipo_dato}")        
                if tipo_dato == 'smallint':
                    print(f"Cambiando campo {nombre_campo} tipo smallinst a boolean")
                    nueva_columna = f"{nombre_campo}_b"
                    cursor.execute(f"ALTER TABLE {tabla_nombre} ADD COLUMN {nueva_columna} BOOLEAN")
                    cursor.execute(f"UPDATE {tabla_nombre} SET {nueva_columna} = ({nombre_campo} = 1)")
                    cursor.execute(f"ALTER TABLE {tabla_nombre} DROP COLUMN {nombre_campo}")
                    cursor.execute(f"ALTER TABLE {tabla_nombre} RENAME COLUMN {nueva_columna} TO {nombre_campo}")                                                    
                    cursor.execute("SELECT pg_stat_reset()")
                    conexion.commit()            
except Exception as e:
    conexion.rollback()
    print("Ocurrio un error:", e)

finally:
    cursor.close()
    conexion.close()  
