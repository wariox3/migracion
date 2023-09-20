import paramiko
import openpyxl
from decouple import config

# Configura la conexión SFTP
host = config('SFTP_HOST')
port = 22  # Puerto estándar para SFTP
username = config('SFTP_USER')
password = config('SFTP_PASSWORD')  # O puedes usar autenticación con clave en lugar de contraseña

# Crea una instancia de cliente SFTP
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    # Conéctate al servidor SFTP
    client.connect(host, port, username, password)

    # Abre una conexión SFTP
    sftp = client.open_sftp()
    # Cambia al directorio deseado en el servidor SFTP
    directorio_raiz = '/mnt/almacenamiento_energy/energy/masivo/TteGuia'
    sftp.chdir(directorio_raiz)

    # Lista los archivos en el directorio
    subdirectorios = sftp.listdir()
    for directorio in subdirectorios:
        print(directorio)

        # Crea un archivo Excel
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.title = "archivos"
        # Cambia al directorio deseado en el servidor SFTP
        subdirectorio = f"/mnt/almacenamiento_energy/energy/masivo/TteGuia/{directorio}"
        sftp.chdir(subdirectorio)

        # Obtiene una lista de atributos de archivos en el directorio
        archivos_subdirectorio = sftp.listdir_attr()

        # Escribe los nombres y tamaños de los archivos en KB en el archivo Excel
        sheet.cell(row=1, column=1, value="Nombre")
        sheet.cell(row=1, column=2, value="Tamaño")
        fila = 1
        for idx, archivo_attr in enumerate(archivos_subdirectorio, start=2):
            nombre_archivo = archivo_attr.filename
            tamaño_archivo_bytes = archivo_attr.st_size
            tamaño_archivo_mb = tamaño_archivo_bytes / (1024 * 1024)
            if tamaño_archivo_mb > 3:
                fila += 1
                sheet.cell(row=fila, column=1, value=nombre_archivo)
                sheet.cell(row=fila, column=2, value=tamaño_archivo_mb)
        # Guarda el archivo Excel
        nombre_archivo_excel = f"/home/desarrollo/Escritorio/analisis_ftp/listado_archivos{directorio}.xlsx"
        workbook.save(nombre_archivo_excel)

finally:
    # Cierra la conexión SFTP
    sftp.close()
    client.close()