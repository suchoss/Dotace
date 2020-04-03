import pandas as pd
import os
from sqlalchemy import create_engine, types, schema
import logging

# je potřeba soubor odsud:
#https://www.czechinvest.org/cz/Sluzby-pro-investory/Investicni-pobidky

# inicializace
logging.getLogger().setLevel(logging.INFO)
logging.info(f"Zpracovávám czechinvest")

DATA_DIR = 'data/'
file = "Udelene-investicni-pobidky.xls"
# DB CONNECTION SETTING
database = 'import'
dbschema = 'czechinvest'
postgre_cnn = os.environ.get('POSTGRES_CONNECTION')

if not postgre_cnn or postgre_cnn.isspace():
    logging.error('Missing environment variable. Please set environment variable in following format: [POSTGRES_CONNECTION="postgresql://username:password@localhost:5432"]')
    exit()
    
def save_to_postgres(dataframe, tablename):
    logging.info(f"Ukládám [{tablename}].")
    engine = create_engine(f"{postgre_cnn}/{database}")
    if not engine.dialect.has_schema(engine, dbschema):
        engine.execute(schema.CreateSchema(dbschema))

    # nastavit malé názvy sloupců, protože postgre je nemá rádo
    dataframe.columns = [c.lower() for c in dataframe.columns] 
    indLabel = dataframe.columns[0]
    dataframe.set_index(dataframe.columns[0], inplace=True) 
    dataframe.to_sql(tablename.lower(),
                     engine,
                     schema=dbschema,
                     if_exists='replace',
                     chunksize=100,  # definuje, kolik kusů naráz se zapisuje
                     index=True,
                     index_label=indLabel,
                     # dtype=types.String,
                     method='multi')  # spojuje inserty do větších kup, takže by to mělo být rychlejší


filePath = DATA_DIR + file

col_names= ["id", "prijemce", "ico", "projekt", "program", "rozhodnuti_mil_czk", "rok_podani","rozhodnuti_den","rozhodnuti_mesic","rozhodnuti_rok","zruseno" ]
use_cols= "A,B,C,D,F,K,W,X,Y,Z,AB"
logging.info(f"Zpracovávám soubor z [{filePath}]")
df = pd.read_excel(filePath, sheet_name="PROJECTS", skiprows=3, skipfooter=43, usecols=use_cols, header=None, names=col_names)
# odmáznutí prázdných řádků
df = df.dropna(subset=["id","prijemce"], how='all')

# odstranit nans
df = df.where(pd.notnull(df), None)

save_to_postgres(df, "dotace")