import pandas as pd
import unidecode
import re
from sqlalchemy import create_engine, types, schema
import logging
import json
import os


def normalize_column_name(name):
    strippedChars = re.sub(r'\W+', '_', name)
    return unidecode.unidecode(strippedChars).lower()


# inicializace
logging.getLogger().setLevel(logging.INFO)
pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

logging.info('Stahuji aktuální data')
os.system('python3 zpracuj.py')

logging.info('Načítám soubor 06')
df06 = pd.read_excel('data/2004-2006.xlsx')
# mazání duplicit
logging.info('Odstraňuji duplicity')
df06.drop_duplicates(['Číslo programu', 'Název programu', 'Číslo priority', 'Název priority', 'Číslo opatření',
                      'Název opatření', 'Číslo projektu', 'Název projektu', 'Stav projektu', 'Žadatel',
                      'Smlouva_ EU podíl', 'Smlouva_Národní veřejné prostředky', 'Proplaceno EU podíl',
                      'Proplaceno_národní veřejné prostředky', 'Zahájení projektu', 'Ukončení projektu'], inplace=True)

# normalizovat názvy sloupců
logging.info('Normalizuji názvy sloupců u df06')
df06.columns = [normalize_column_name(c) for c in df06.columns]

logging.info('Načítám soubor 13')
df13 = pd.read_excel('data/2007-2013.xlsx')
# mazání duplicit
logging.info('Odstraňuji duplicity')
df13 = df13[df13['Pořadí v rámci v projektu (filtr)'] == 1]

# normalizovat názvy sloupců
logging.info('Normalizuji názvy sloupců u df13')
df13.columns = [normalize_column_name(c) for c in df13.columns]

logging.info('Načítám soubor projekty')
dfnew = pd.read_csv('data/projekty.csv')

# rozbal json
logging.info('Rozbaluji json')
dfnew['zadatel_obec'] = dfnew['zadatel_adresa'].apply(lambda r: json.loads(r)['obnazev'])
dfnew['zadatel_okres'] = dfnew['zadatel_adresa'].apply(lambda r: json.loads(r)['oknazev'])
dfnew['zadatel_psc'] = dfnew['zadatel_adresa'].apply(lambda r: json.loads(r)['psc'])

logging.info('Odstranuji neuzitecne sloupce')
dfnew.drop(['zadatel_adresa', 'nazeva'], axis='columns', inplace=True)

# prejmenovat vsude kod projektu
df06.rename(columns={'cislo_projektu': 'kod_projektu'}, inplace=True)
df13.rename(columns={'cislo_projektu': 'kod_projektu'}, inplace=True)
dfnew.rename(columns={'kod': 'kod_projektu'}, inplace=True)


# db import
dbschema = 'eufondy'
engine = create_engine('postgresql://postgres:xxx@localhost:5432/postgres')
if not engine.dialect.has_schema(engine, dbschema):
    engine.execute(schema.CreateSchema(dbschema))

indLabel = 'kod_projektu'
logging.info('Nahravam do db soubor 06')
df06.set_index(indLabel, inplace=True)
df06.to_sql('dotace2006',
            engine,
            schema=dbschema,
            if_exists='replace',
            chunksize=100,  # definuje, kolik kusů naráz se zapisuje
            index=True,
            index_label=indLabel,
            method='multi')

logging.info('Nahravam do db soubor 13')
df13.set_index(indLabel, inplace=True)
df13.to_sql('dotace2013',
            engine,
            schema=dbschema,
            if_exists='replace',
            chunksize=100,  # definuje, kolik kusů naráz se zapisuje
            index=True,
            index_label=indLabel,
            method='multi')

logging.info('Nahravam do db soubor projekty')
dfnew.set_index(indLabel, inplace=True)
dfnew.to_sql('dotacenew',
             engine,
             schema=dbschema,
             if_exists='replace',
             chunksize=100,  # definuje, kolik kusů naráz se zapisuje
             index=True,
             index_label=indLabel,
             method='multi')
