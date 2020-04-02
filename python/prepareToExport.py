import os
import pandas as pd
import json
import numpy as np
import re
from datetime import datetime
from sqlalchemy import create_engine, types, schema
import logging

# set logging to file and stderr
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler("processing.log")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(fh)
logger.addHandler(ch)

logger.info('Inicializace')
report = []
# DB CONNECTION SETTING
# if not os.environ.get('POSTGRES_CONNECTION') or os.environ.get('POSTGRES_CONNECTION').isspace():
#     logger.error('Missing environment variable. Please set environment variable in following format: [POSTGRES_CONNECTION="postgresql://username:password@localhost:5432"]')
#     exit()
postgre_cnn_cleaning = os.environ.get('POSTGRES_CONNECTION') +"/cleaning"
postgre_cnn_export = os.environ.get('POSTGRES_CONNECTION') +"/export"


def dict2json(dictionary):
    return json.dumps(dictionary, ensure_ascii=False)


def save_to_postgres(dataframe, tablename, dbschema):
    logger.info(f"Ukladam [{tablename}] do schematu [{dbschema}].")
    engine = create_engine(f"{postgre_cnn_export}")
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


def sumrozhodnuti(x):
    if x == None:
        return 0
    return sum([r["castkaRozhodnuta"] for r in x if "castkaRozhodnuta" in r ])

def sumcerpani(x):
    if x == None:
        return 0
    suma = 0
    for roz in x:
        if "cerpani" in roz and roz["cerpani"] != None:
            for cer in roz["cerpani"]:
                if "castkaSpotrebovana" in cer and cer["castkaSpotrebovana"] != None:
                    suma += float(cer["castkaSpotrebovana"])
    return suma


logger.info('Load everything')
logger.info('Loading cedr')
cedr = pd.read_sql_table("dotace", postgre_cnn_cleaning, schema="cedr")
logger.info('Loading eufondy')
eufondy = pd.read_sql_table("dotace", postgre_cnn_cleaning, schema="eufondy")
logger.info('Loading dotinfo')
dotinfo = pd.read_sql_table("dotace", postgre_cnn_cleaning, schema="dotinfo")
logger.info('Loading szif')
szif = pd.read_sql_table("dotace", postgre_cnn_cleaning, schema="szif")

# zajistit unikátnost id
logger.info('nastavuji index')
cedr["iddotace"] = "cedr-" + cedr["iddotace"]
eufondy["iddotace"] = "eufondy-" + eufondy["iddotace"]
dotinfo["iddotace"] = "dotinfo-" + dotinfo["iddotace"]
szif["iddotace"] = "szif-" + szif["iddotace"]

#zapsat počty záznamů
report.append(f"Z cedr bylo nacteno {len(cedr.index)} polozek.")
report.append(f"Z eufondy bylo nacteno {len(eufondy.index)} polozek.")
report.append(f"Z dotinfo bylo nacteno {len(dotinfo.index)} polozek.")
report.append(f"Z szif bylo nacteno {len(szif.index)} polozek. Tyto zaznamy jsou unikatni (neexistuje o nich informace v CEDRu).")
# merge
logger.info('spojuji zdroje')
merged = pd.concat([cedr,eufondy,dotinfo])
# set index
merged = merged.set_index("iddotace", drop=False)

# nastav info o zdroji
logger.info('Hledam podobne dotace podle kodu projektu')
seznamZdroju = merged.groupby("kodprojektu").apply(lambda x: [ dict(nazev=z,url=u) for z,u in zip(x["zdroj"], x["url"])] )
merged["zdroje"] = merged.apply(lambda x: [dict(dct, isPrimary=True) if dct["nazev"] == x["zdroj"] else dct for dct in seznamZdroju[x["kodprojektu"]] ] ,axis=1 )

#nezapomenout zmergovat ještě SZIF
logger.info('pridavam SZIF')
szif = szif.set_index("iddotace", drop=False)
szif["zdroje"] = szif.apply(lambda x: [dict(nazev=x["zdroj"], url=x["url"], isPrimary=True )] ,axis=1)
merged = pd.concat([merged,szif])
# vynulovat nány 
merged = merged.where(pd.notnull(merged), None)
report.append(f"Po spojeni zdroju mame celkem {len(merged.index)} polozek.")

# načíst json zpět do dict (aby to neescapovalo vnořený json)
logger.info('rozbaluji dictionaries')
merged["rozhodnuti"] = merged["rozhodnuti"].apply(lambda x: json.loads(x) )
merged["prijemce"] = merged["prijemce"].apply(lambda x: json.loads(x) )
merged["program"] = merged["program"].apply(lambda x: json.loads(x) )
merged["chyba"] = merged["chyba"].apply(lambda x: json.loads(x) )
# zabalit merge a nahrát do exportu
logger.info('pripravuji data pro export')
merged["data"] = merged.apply(lambda x: dict(idDotace=x.iddotace, 
                            datumPodpisu=x.datumpodpisu,
                            kodProjektu=x.kodprojektu,
                            nazevProjektu=x.nazevprojektu,
                            datumAktualizace=x.datumaktualizace,
                            zdroje=x.zdroje,
                            prijemce=x.prijemce,
                            program=x.program,
                            rozhodnuti=x.rozhodnuti,
                            chyba=x.chyba ), axis=1)

###### zmergováno, ulozit do db

merged["data"] = merged["data"].map(dict2json)

#### nejsme schopni jednoznačně detekovat duplicity.#########
# protože:
# 1) jeden kód projektu může mít více příjemců
# 2) jeden příjemce může mít více částek
# 3) částky mohou být vyplaceny v různých letech -> a tady je jediným zdrojem s "plnou" informací CEDR
# například kód projetu ""PRA-V-36/2003"" kde je dotace duplicitní v rámci cedru samotného
# vs kód projektu ""CZ.1.01/2.1.00/09.0132"" kde duplicita není, ale rozhodnutí je nafouknuté
##############################################################
    #mergedclean = merged[(~merged['kodprojektu'].duplicated()) | merged['kodprojektu'].isna()][["iddotace","data"]]
    # statistika kolik jich zbylo podle zdrojů, vs kolik jich bylo původně
    #report.append(f"Po odstraneni duplicit existuje celkem {len(mergedclean.index)} polozek.")
save_to_postgres(merged[["iddotace","data"]], "dotace", "dotace")
    #save_to_postgres(merged[(~merged.duplicated("zdroj"))][["iddotace","data"]], "dotace", "dotace")


##### Počítání dalších informací
# unpack částka rozhodnutá sum
logger.info('Pocitani statistickych informaci')

logger.info('rozbaluji dalsi informace ze slovniku')
merged["ico"] = merged["prijemce"].apply(lambda x: x["ico"] )
merged["jmeno"] = merged["prijemce"].apply(lambda x: x["obchodniJmeno"] )
merged["sum_roz"] = merged["rozhodnuti"].apply(sumrozhodnuti)
merged["sum_cer"] = merged["rozhodnuti"].apply(sumcerpani)
merged["sum_max"] = merged.apply(lambda x: max(x["sum_roz"],x["sum_cer"]), axis=1)

#připravit pro exporty statistik
merged["rozhodnuti"] = merged["rozhodnuti"].map(dict2json)
merged["prijemce"] = merged["prijemce"].map(dict2json)
merged["program"] = merged["program"].map(dict2json)
merged["chyba"] = merged["chyba"].map(dict2json)

# merged.duplicated(["kodprojektu", "sum_roz"])
# | merged.duplicated(["kodprojektu", "sum_cer"])

f_not_szif = ~merged.zdroj.str.contains("szif")
f_not_cedr = ~merged.zdroj.str.contains("cedr")

#### statistiky
#REPORT 1 duplicity - potencionální duplicity pro další koumání
logger.info('hledam potencionalni duplicity')
duplicates = merged[merged.duplicated(["kodprojektu","sum_max","ico"], keep=False) & f_not_szif].drop("data", axis=1)
duplicates.sort_values("kodprojektu")
report.append(f"Celkem nalezeno {len(duplicates.index)} potencionalnich duplicit.")
#save_to_postgres(duplicates, "duplicity", "dotace")
duplicates.to_csv("duplicates.csv.gz", index=False, compression="gzip")

#REPORT 2 - tyto informace chybí v cedru (nepodařili se najít podle project identificator) - proč
logger.info('Zjistuji co chybi v cedru')
cedrmissing = merged[(~merged.duplicated("kodprojektu")) & f_not_szif & f_not_cedr ].drop("data", axis=1)
report.append(f"Nepodarilo se nalezt v cedru {len(cedrmissing.index)} polozek, ktere by tam mwli byt.")
#save_to_postgres(cedrmissing, "nenalezeno", "dotace")
cedrmissing.to_csv("cedrmissing.csv.gz", index=False, compression="gzip")

#REPORT 3 - chybové ?


with open("report.txt", "w") as txt_file:
    for line in report:
        txt_file.write(line + "\n")



