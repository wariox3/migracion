import mysql.connector
from decouple import config

conexion = mysql.connector.connect(host=config('MYSQL_SERVIDOR'), user=config('MYSQL_USUARIO'), password=config('MYSQL_CLAVE'),database=config('MYSQL_BASEDATOS'))
conexion_destino = mysql.connector.connect(host=config('MYSQL_SERVIDOR'), user=config('MYSQL_USUARIO'), password=config('MYSQL_CLAVE'),database=config('MYSQL_BASEDATOS_DESTINO'))

cursor = conexion.cursor(dictionary=True)
cursor_destino = conexion_destino.cursor()
def migrar_pago():    
    cursor.execute(f"SELECT p.codigo_pago_pk, p.codigo_pago_tipo_fk, p.fecha_desde, p.fecha_hasta, p.fecha_desde_pago, p.fecha_hasta_pago, p.numero, \
                        p.vr_salario_empleado, p.dias_laborados, p.vr_devengado, p.vr_deducciones, p.vr_neto, p.vr_ingreso_base_cotizacion, p.vr_ingreso_base_prestacion, \
                        p.vr_salario_periodo, p.vr_auxilio_transporte, p.vr_adicional_valor, p.vr_adicional_valor_no_prestacional, \
                        p.codigo_empleado_semantica_fk, p.codigo_contrato_semantica_fk, \
                        cc.codigo_interface as codigo_grupo_fk \
                        FROM rhu_pago p \
                        LEFT JOIN rhu_centro_costo cc ON p.codigo_centro_costo_fk=cc.codigo_centro_costo_pk \
                        WHERE p.fecha_desde >= '2024-01-01' AND p.estado_pagado = 1 AND p.codigo_empleado_semantica_fk IS NOT NULL \
                        ORDER BY p.fecha_desde LIMIT 10000")
    sql = "INSERT INTO rhu_pago (codigo_pago_pk, fecha_desde, fecha_hasta, fecha_desde_contrato, fecha_hasta_contrato, fecha, codigo_grupo_fk, numero, \
                vr_salario_contrato, dias, vr_devengado, vr_deduccion, vr_neto, vr_ingreso_base_cotizacion, vr_ingreso_base_prestacion, \
                vr_salario, vr_auxilio_transporte, vr_devengado_prestacional, vr_devengado_no_prestacional, \
                codigo_pago_tipo_fk, codigo_empleado_fk, codigo_contrato_fk, \
                codigo_periodo_fk, codigo_empresa_fk, migracion, \
                estado_autorizado, estado_aprobado, estado_electronico) \
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                'QUI', 1, true, \
                true, true, true)"
    sql_detalle = "INSERT INTO rhu_pago_detalle (codigo_pago_detalle_pk, codigo_pago_fk, operacion, vr_pago, vr_pago_operado, \
            horas, vr_hora, porcentaje, vr_ingreso_base_cotizacion, vr_ingreso_base_prestacion, detalle, vr_devengado, vr_deduccion, \
            codigo_concepto_fk, \
            vr_ingreso_base_prestacion_vacacion, dias, codigo_empresa_fk) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 1)"
    registros = cursor.fetchall()
    try:
        for registro in registros:
            print(registro['codigo_pago_pk'])
            pago_tipo = 'NCA'
            if registro['codigo_pago_tipo_fk'] == 1:
                pago_tipo = 'NCA'
            if registro['codigo_pago_tipo_fk'] == 4:
                pago_tipo = 'VCA'
            if registro['codigo_pago_tipo_fk'] == 5:
                pago_tipo = 'LCA'                                
            valores = (
                registro['codigo_pago_pk'],
                registro['fecha_desde'],
                registro['fecha_hasta'],
                registro['fecha_desde_pago'],
                registro['fecha_hasta_pago'],
                registro['fecha_desde'],
                registro['codigo_grupo_fk'],
                registro['numero'],
                registro['vr_salario_empleado'], 
                registro['dias_laborados'],
                registro['vr_devengado'],
                registro['vr_deducciones'],
                registro['vr_neto'],
                registro['vr_ingreso_base_cotizacion'],
                registro['vr_ingreso_base_prestacion'],
                registro['vr_salario_periodo'],
                registro['vr_auxilio_transporte'],
                registro['vr_adicional_valor'],
                registro['vr_adicional_valor_no_prestacional'],
                pago_tipo,
                registro['codigo_empleado_semantica_fk'],
                registro['codigo_contrato_semantica_fk'],
            )
            cursor_destino.execute(sql, valores)
            cursor.execute(f"SELECT pd.codigo_pago_detalle_pk, pd.codigo_pago_fk, pd.operacion, pd.vr_pago, pd.vr_pago_operado, pd.numero_horas, pd.vr_hora, \
                           pd.porcentaje_aplicado, pd.vr_ingreso_base_cotizacion, pd.vr_ingreso_base_prestacion, pd.detalle, pd.codigo_pago_concepto_fk \
                    FROM rhu_pago_detalle pd \
                    WHERE pd.codigo_pago_fk={registro['codigo_pago_pk']}")
            detalles = cursor.fetchall()
            for detalle in detalles:
                print(detalle['codigo_pago_detalle_pk'])                
                devengado = 0
                deduccion = 0
                if detalle['operacion'] == 1:
                    devengado = detalle['vr_pago']
                if detalle['operacion'] == -1:
                    deduccion = detalle['vr_pago']
                concepto = detalle['codigo_pago_concepto_fk']
                if concepto == 22:
                    concepto = 6
                if concepto == 566:
                    concepto == 508
                if concepto == 27:
                    concepto = 517
                valores_detalle = (
                    detalle['codigo_pago_detalle_pk'],
                    detalle['codigo_pago_fk'],
                    detalle['operacion'],
                    detalle['vr_pago'],
                    detalle['vr_pago_operado'],
                    detalle['numero_horas'],
                    detalle['vr_hora'],
                    detalle['porcentaje_aplicado'],
                    detalle['vr_ingreso_base_cotizacion'],
                    detalle['vr_ingreso_base_prestacion'],
                    detalle['detalle'],
                    devengado,
                    deduccion,
                    concepto,
                )
                try:
                    cursor_destino.execute(sql_detalle, valores_detalle)
                except mysql.connector.Error as err:
                    print(f"Error: {err} en la consulta SQL: {sql_detalle} con valores: {valores_detalle}")
                    raise                
        conexion_destino.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        conexion_destino.rollback()   
    finally:
        cursor.close()
        conexion.close()
        cursor_destino.close()
        conexion_destino.close()             

migrar_pago()  