import pandas as pd
import os
from urllib.parse import urlparse
from urllib.request import urlretrieve
from sqlalchemy import create_engine, types, schema
import logging
import xml.etree.ElementTree as et
import gzip
import re

# inicializace
DATA_DIR = 'data/'
logging.getLogger().setLevel(logging.INFO)

# DB CONNECTION SETTING
database = 'import'
dbschema = 'szif'
postgre_cnn = os.environ.get('POSTGRES_CONNECTION')
#os.chdir("szif")

if not postgre_cnn or postgre_cnn.isspace():
    logging.error('Missing environment variable. Please set environment variable in following format: [POSTGRES_CONNECTION="postgresql://username:password@localhost:5432"]')
    exit()

#f"{DATA_DIR}spd_2017.xml.gz"

# output structure definition
df_cols = ["ico","jmeno", "obec", "okres", "zdroj","opatreni","castka_cr","castka_eu"]
dfmerged = pd.DataFrame(columns=df_cols)

# converts only specific SZIF xml
def xmlgz_to_dataframe(filePath):
    logging.info(f"Načítám [{filePath}].")
    xmlFile = gzip.open(filePath, 'r')
    xtree = et.parse(xmlFile)
    zadatele = xtree.findall("//zadatel")

    rows = []

    for zadatel in zadatele:
        z_jmeno = zadatel.find("jmeno_nazev").text if zadatel is not None else None
        z_obec = zadatel.find("obec").text if zadatel is not None else None
        z_okres = zadatel.find("okres").text if zadatel is not None else None
        # rozbalit platby
        for platba in zadatel.findall(".//platba"):
            rows.append({
                "jmeno": z_jmeno,
                "obec": z_obec,
                "okres": z_okres,
                "zdroj": platba.find("fond_typ_podpory").text if zadatel is not None else None,
                "opatreni": platba.find("opatreni").text if zadatel is not None else None,
                "castka_cr": platba.find("zdroje_cr").text if zadatel is not None else None,
                "castka_eu": platba.find("zdroje_eu").text if zadatel is not None else None,
            })
        # rozbalit platby prechodne vnitrostatni podpory
        for platbapvp in zadatel.findall(".//platba_pvp"):
            rows.append({
                "jmeno": z_jmeno,
                "obec": z_obec,
                "okres": z_okres,
                "zdroj": platbapvp.find("fond_typ_podpory").text if zadatel is not None else None,
                "opatreni": platbapvp.find("opatreni").text if zadatel is not None else None,
                "castka_cr": platbapvp.find("celkem_czk").text if zadatel is not None else None,
                "castka_eu": None,
            })
    return pd.DataFrame(rows, columns=df_cols)

def csvgz_to_dataframe(filePath):
    logging.info(f"Načítám [{filePath}].")
    df = pd.read_csv(filePath, compression="gzip")
    # drop last column with sum
    df.drop(df.columns[-1], axis=1, inplace=True)
    # normalize column names
    df.columns = df_cols
    return df

# je zapotřebí aby první sloupec dataframe obsahoval unikátní ID
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

filesToProcess = os.listdir(DATA_DIR)

for file in filesToProcess:
    filePath = DATA_DIR + file
    logging.info(f"Zpracovávám [{filePath}]")
    
    if "csv" in filePath.lower():
        df = csvgz_to_dataframe(filePath)
    elif "xml" in filePath.lower():
        df = xmlgz_to_dataframe(filePath)
    else:
        logging.warning(f"Z názvu souboru [{filePath}] není zřejmé, jestli jde o xml, nebo csv soubor. Soubor nebude zpracován.")

    logging.info("Generuji id.")
    # vygenerovat unikátní id záznamu
    indexName = file.split(".")[0].lower()
    allowedChars = re.findall(r"[a-z0-9\-]",indexName )
    indexName = "".join(allowedChars)
    idColumnName = "id"
    df.insert(0, idColumnName, None)
    df["id"] = indexName + "-" + df.index.astype(str, copy = False)

    dfmerged = pd.concat([dfmerged, df])

save_to_postgres(dfmerged, "dotace")
    

