import pandas as pd
from sqlalchemy import create_engine
import html2text

engine = create_engine('mysql://root:70143086@localhost/bdcarbono')
query = "SELECT codigo_documentacion_pk, contenido FROM documentacion"
df = pd.read_sql(query, engine)
converter = html2text.HTML2Text()
converter.ignore_links = False

for index, row in df.iterrows():
    markdown_content = converter.handle(row['contenido'])
    filename = f"/home/desarrollo/Escritorio/documentacion/{row['codigo_documentacion_pk']}.md"
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(markdown_content)
    print(f"Archivo {filename} guardado.")