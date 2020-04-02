import pandas as pd
import os
from urllib.parse import urlparse
from urllib.request import urlretrieve
from sqlalchemy import create_engine, types, schema
import logging

# inicializace
DOWNLOAD_DIR = 'data/download/'
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
logging.getLogger().setLevel(logging.INFO)

postgre_cnn = os.environ.get('POSTGRES_CONNECTION')
if not postgre_cnn or postgre_cnn.isspace():
    logging.error('Missing environment variable. Please set environment variable in following format: [POSTGRES_CONNECTION="postgresql://username:password@localhost:5432"]')
    exit()

def downloadFile(url):
    filename = os.path.split(urlparse(url).path)[-1].rstrip('\n')
    local_path = os.path.join(DOWNLOAD_DIR, filename)
    if not os.path.isfile(local_path):
        logging.info('Nemam %s (%s) lokalne, stahuju', filename, url)
        urlretrieve(url, local_path)
    logging.info('Soubor %s je stažen', filename)


# def runSqlQueries(eng, filename):
#     with eng.connect() as connection:
#         with open(filename) as file:
#             commands = file.readlines()
#             for sql in commands:
#                 try:
#                     connection.execute(sql)
#                 except:
#                     logging.info('Nejde spustit %s', sql)


# stáhne všechna potřebná zdrojová data
with open('downloadList.csv') as f:
    lines = f.readlines()
    for line in lines:
        downloadFile(line)

logging.info('Data jsou stažena')
# připojení k postgres
dbschema = 'cedr'
database = '/import'

engine = create_engine(postgre_cnn + database)
if not engine.dialect.has_schema(engine, dbschema):
    engine.execute(schema.CreateSchema(dbschema))

# drop views
# runSqlQueries(engine, 'drop_views.sql')

filesToProcess = os.listdir(DOWNLOAD_DIR)

# plnění databáze soubor po souboru
for fileToProcess in filesToProcess:
    logging.info('Zpracovávám %s', fileToProcess)
    df = pd.read_csv(DOWNLOAD_DIR + fileToProcess, compression='gzip')  # , dtype=str)
    logging.info('Csv načteno, nastavuji názvy sloupců a index')
    df.columns = [c.lower() for c in df.columns]  # nastavit malé názvy sloupců, protože postgre je nemá rádo
    indLabel = df.columns[0]
    df.set_index(df.columns[0], inplace=True)
    logging.info('Hotovo, ukládám do db')
    if fileToProcess.split('.')[0].lower() == 'dotace':
        df['projektidnetifikator'] = df['projektidnetifikator'].apply(lambda val: str(val).strip())
    df.to_sql(fileToProcess.split('.')[0].lower(),
              engine,
              schema=dbschema,
              if_exists='replace',
              chunksize=100,  # definuje, kolik kusů naráz se zapisuje
              index=True,
              index_label=indLabel,
              # dtype=types.String,
              method='multi')  # spojuje inserty do větších kup, takže by to mělo být rychlejší

# create views
# runSqlQueries(engine, 'create_views.sql')
