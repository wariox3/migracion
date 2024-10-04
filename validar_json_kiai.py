import json
import pandas as pd

with open('/home/desarrollo/Escritorio/factura49506.json', 'r') as archivo:
    datos = json.load(archivo)


validadorImpuesto = []
factura_venta = datos.get("FacturaVenta", {})
impuestos_factura = factura_venta.get("ImpuestosFactura", [])
for impuesto_factura in impuestos_factura:
    doei_total = impuesto_factura.get("DoeiTotal")
    doei_es_porcentual = impuesto_factura.get("DoeiEsPorcentual")
    impu_codigo = impuesto_factura.get("ImpuCodigo")
    print(f"DoeiTotal: {doei_total}")

    doei_total_calculado = 0
    detalles = impuesto_factura.get("Detalle", [])
    i = 1
    for detalle in detalles:
        dedi_base = detalle.get("DediBase")
        dedi_valor = float(detalle.get("DediValor"))
        validadorImpuesto.append({
            "index": i,
            "DediValor": dedi_valor,
        })
        doei_total_calculado += dedi_valor
        i += 1
print(f"DoeiTotal_calculado: {doei_total_calculado}")

validadorDetalle = []
i = 1
doei_total_impuestos_calculado = 0
detalles_factura = factura_venta.get("DetalleFactura", [])
for detalle_factura in detalles_factura:
    doei_item = detalle_factura.get("DoeiItem")
    doei_total_impuestos = float(detalle_factura.get("DoeiTotalImpuestos"))
    doei_total_impuestos_calculado += doei_total_impuestos
    validadorDetalle.append({
        "index": i,
        "DoeiTotalImpuestos": doei_total_impuestos,
    })
    i += 1
print(f"doei_total_impuestos_calculado: {doei_total_impuestos_calculado}")    

df_validador = pd.DataFrame(validadorImpuesto)
output_path = '/home/desarrollo/Escritorio/validador_impuesto.xlsx'
df_validador.to_excel(output_path, index=False)

df_validador = pd.DataFrame(validadorDetalle)
output_path = '/home/desarrollo/Escritorio/validador_detalle.xlsx'
df_validador.to_excel(output_path, index=False)