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
# DB CONNECTION SETTING
database = 'cleaning'
postgre_cnn_cleaning = os.environ.get('POSTGRES_CONNECTION')

postgre_cnn_import = os.environ.get('POSTGRES_CONNECTION') +"/import"

if not postgre_cnn_cleaning or postgre_cnn_cleaning.isspace():
    logger.error('Missing environment variable. Please set environment variable in following format: [POSTGRES_CONNECTION="postgresql://username:password@localhost:5432"]')
    exit()

def save_to_postgres(dataframe, tablename, dbschema):
    logger.info(f"Ukladam [{tablename}] do schematu [{dbschema}].")
    engine = create_engine(f"{postgre_cnn_cleaning}/{database}")
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

# from es_pandas import es_pandas


# ep = es_pandas(es_host)
# df = ep.to_pandas(es_index, heads=es_heads)
# df.to_csv("firmyorig.csv.gz", index=False, compression="gzip")



def float_to_text(input):
    return "{:.0f}".format(input) if not pd.isnull(input) else None

def fix_ico_format(ico: str):
    if not ico:
        return None
    ico = ico.replace(" ","")
    if not ico.isdigit():
        return None
    return ico.rjust(8, "0")

################### ICO check functions ###################
## podle následujících funkcí budeme moct kontrolovat iča
logger.info('pripravuji data pro nastaveni ico')
df = pd.read_csv("firmyorig.csv.gz", dtype=str, compression="gzip")

hlidacIcoJmeno = df.set_index("ico")["jmeno"].squeeze().to_dict()
df["jmeno"] = df["jmeno"].str.lower()
hlidacJmenoIco = df.set_index("jmeno").squeeze().to_dict()

df = None

# získá ičo podle jména, případně updatuje seznam
def getIcoFromJmeno(ico, jmeno):
    if not jmeno:
        return ico
    jmeno = jmeno.lower()
    if jmeno in hlidacJmenoIco:
        if ico and ico.isdigit():  # nechceme přepisovat případné ičo
            return ico
        return hlidacJmenoIco[jmeno]
    
    if ico and ico.isdigit(): # Ičo existuje, můžeme přidat název pokud neznáme
        hlidacJmenoIco[jmeno] = fix_ico_format(ico)
        return ico
    
    return None

# získá jméno podle iča
def getJmenoFromIco(ico):
    ico = fix_ico_format(ico)
    if ico in hlidacIcoJmeno:
        return hlidacIcoJmeno[ico]
    
    return None
################### ICO check functions ###################

# před exportem je potřeba všechny sloupce typu dict hodit do jsonu
def dict2json(dictionary):
    return json.dumps(dictionary, ensure_ascii=False)

def prep_dotace_for_export(dataframe):
    dataframe["chyba"] = dataframe["chyba"].map(dict2json)
    dataframe["program"] = dataframe["program"].map(dict2json)
    dataframe["prijemce"] = dataframe["prijemce"].map(dict2json)
    dataframe["rozhodnuti"] = dataframe["rozhodnuti"].map(dict2json)


## cedr
logger.info('Zpracovavam cedr ...')
# docasne
logger.info('nacitam adresy')
tempSidlo = pd.read_sql_table("adresasidlo", postgre_cnn_import, schema="cedr")
tempBydliste = pd.read_sql_table("adresabydliste", postgre_cnn_import, schema="cedr")

#adresy priradit k osobe primo
tempAdresy = pd.concat([tempSidlo[["idprijemce", "obecnazev", "iriobec", "psc"]], tempBydliste[["idprijemce", "obecnazev", "obec"]].rename(columns={'obec':'iriobec'}) ])
tempAdresy["psc"] = tempAdresy["psc"].apply(float_to_text)


tempObec = pd.read_sql_table("ciselnikobecv01", postgre_cnn_import, schema="cedr")
tempOkres = pd.read_sql_table("ciselnikokresv01", postgre_cnn_import, schema="cedr")
# je potřeba spravit id, protože u okresu tam je nadbytečný text
tempOkres["okresnad"] = tempOkres["id"].apply(lambda x: x.rsplit("/",1)[0])
# taky je zde spousta duplikátů
tempOkres = tempOkres.drop_duplicates(subset=["okresnad"], keep="first")[["okresnad","okresnazev"]]

logger.info('slucuji adresy')
# sloučit do jedné tabulky s osobou
tempObec = pd.merge(tempObec, tempOkres, on="okresnad", how="left")
tempObec = tempObec.rename(columns={"id":"iriobec"})
tempAdresy = pd.merge(tempAdresy[["idprijemce", "iriobec", "psc"]], tempObec[["iriobec","obecnazev","okresnazev"]], on="iriobec", how="left")

logger.info('nacitam prijemce')
cedrosoba = pd.read_sql_table("prijemcepomoci", postgre_cnn_import, schema="cedr")
cedrosoba = pd.merge(cedrosoba[["idprijemce", "ico","obchodnijmeno","jmeno","prijmeni","roknarozeni"]], tempAdresy[["idprijemce","psc","obecnazev","okresnazev"]], on="idprijemce", how="left" )

#uvolnit paměť
tempSidlo = None
tempBydliste = None
tempAdresy = None
tempObec = None
tempOkres = None

# nastavit obchodni jmeno tam kde chybí - bude pro nás primární hodnotou
cedrosoba["roknarozeni"] = cedrosoba["roknarozeni"].apply(float_to_text)
cedrosoba["obchodnijmeno"] = cedrosoba["obchodnijmeno"].fillna(cedrosoba["jmeno"] + " " + cedrosoba["prijmeni"])
# cedrosoba[cedrosoba["obchodnijmeno"].isna()]
# opravit ico na standardni format
cedrosoba["ico"] = cedrosoba["ico"].apply(lambda x: fix_ico_format(float_to_text(x)))
# nastavit chybné ičo na None
cedrosoba.loc[cedrosoba["ico"] == "00000001", "ico" ] = None
cedrosoba.loc[cedrosoba["ico"] == "99999999", "ico" ] = None

logger.info('Doplnuji ico a hlidac jmeno k osobe')
cedrosoba["ico"] = cedrosoba.apply(lambda x: getIcoFromJmeno(x["ico"], x["obchodnijmeno"]) if not x["roknarozeni"] else x["ico"] , axis=1)
cedrosoba["hlidacjmeno"] = cedrosoba["ico"].apply(lambda x: getJmenoFromIco(x) )
cedrosoba.drop(["jmeno", "prijmeni"], axis=1, inplace=True)
cedrosoba.set_index("idprijemce", inplace=True)

# save_to_postgres(cedrosoba,"prijemce","cedr")
# projet osoby z cedru a doplnit jméno, dojet cedr

#PRIJEMCE_COLUMNS = ['idprijemce', 'ico', 'obchodnijmeno', 'roknarozeni', 'psc', 'obecnazev', 'okresnazev', 'hlidacjmeno']


# cedr dotace
logger.info('Nacitam dotace')
dotace = pd.read_sql_table("dotace", postgre_cnn_import, schema="cedr")
dotace.dropna(axis=1, how='all', inplace=True)

# 1st joining program as program
logger.info('nacitam a doplnuji programy k dotaci')
f1 = pd.read_sql_table("ciselnikprogramv01", postgre_cnn_import, schema="cedr")
f1.set_index(f1.columns[0], inplace=True)
ciselnik = f1.apply(lambda row: dict(nazev=row["programnazev"], kod=row["programkod"]), axis=1)
dotace["program"] = dotace.iriprogram.map(ciselnik)
f1 = None
ciselnik = None

# cedr has some additional info stored in two similar tables instead of one
def load_and_merge_ciselnik(first_table, second_table, first_column, second_column):
    f1 = pd.read_sql_table(first_table, postgre_cnn_import, schema="cedr")
    f1.set_index(f1.columns[0], inplace=True)
    f2 = pd.read_sql_table(second_table, postgre_cnn_import, schema="cedr")
    f2.set_index(f2.columns[0], inplace=True)
    return pd.concat([f1,f2]).apply(lambda row: dict(nazev=row[first_column], kod=row[second_column]), axis=1)


# 2nd filling gaps with operacni program
dotace["program"] = dotace.program.combine_first(
    dotace.irioperacniprogram.map(
        load_and_merge_ciselnik("ciselnikcedroperacniprogramv01",
        "ciselnikmmroperacniprogramv01",
        "operacniprogramnazev",
        "operacniprogramkod"
        ))) 

# 3rd filling gaps with opatreni
dotace["program"] = dotace.program.combine_first(
    dotace.iriopatreni.map(
        load_and_merge_ciselnik("ciselnikcedropatreniv01",
        "ciselnikmmropatreniv01",
        "opatreninazev",
        "opatrenikod"
        )))

# 4th filling gaps with grantove schema
dotace["program"] = dotace.program.combine_first(
    dotace.irigrantoveschema.map(
        load_and_merge_ciselnik("ciselnikcedrgrantoveschemav01",
        "ciselnikmmrgrantoveschemav01",
        "grantoveschemanazev",
        "grantoveschemakod"
        )))

# nastavit chybějící hodnoty osoby na none
cedrosoba = cedrosoba.where(pd.notnull(cedrosoba), None)
# pripojit osoby k dotacím
logger.info('pripojuji osobu k dotaci')
packed = cedrosoba.apply(lambda row: dict(ico=row["ico"], 
                                          obchodniJmeno=row["obchodnijmeno"],
                                          hlidacJmeno=row["hlidacjmeno"], 
                                          rokNarozeni=row["roknarozeni"],
                                          obec=row["obecnazev"],
                                          okres=row["okresnazev"],
                                          psc=row["psc"],
                                          ), axis=1)
dotace["prijemce"] = dotace["idprijemce"].map(packed)
packed = None


# načíst rozhodnutí a čerpání
logger.info('Nacitam a pripravuji data o rozhodnuti a cerpani')
rozhodnuti = pd.read_sql_table("rozhodnuti", postgre_cnn_import, schema="cedr")
# odmazat refundace
rozhodnuti = rozhodnuti[rozhodnuti["refundaceindikator"] == False]
cerpani = pd.read_sql_table("rozpoctoveobdobi", postgre_cnn_import, schema="cedr")
# spocitat celkovou spotrebovanou castku
cerpani["castkavracena"].fillna(0, inplace=True)
cerpani["castkaspotrebovana"].fillna(0, inplace=True)
cerpani["castkaspotrebovana"] = cerpani["castkaspotrebovana"]-cerpani["castkavracena"]
cerpani = cerpani[["idrozhodnuti", "castkaspotrebovana", "rozpoctoveobdobi"]]
# pripravit dict
cerpani["cerpani"] = cerpani.apply(lambda x: dict(castkaSpotrebovana=x['castkaspotrebovana'],rok=x['rozpoctoveobdobi']), axis=1 )
cerpani = cerpani.groupby("idrozhodnuti")["cerpani"].apply(list)
rozhodnuti = pd.merge(rozhodnuti, cerpani, how="left", on="idrozhodnuti" )

# rozšířit rozhodnuti
finzdroj = pd.read_sql_table("ciselnikfinancnizdrojv01", postgre_cnn_import, schema="cedr")
poskytovatel = pd.read_sql_table("ciselnikdotaceposkytovatelv01", postgre_cnn_import, schema="cedr")
finzdroj = finzdroj[["id","financnizdrojnazev"]].rename(columns={'id':'irifinancnizdroj', 'financnizdrojnazev':'zdrojfinanci'})
poskytovatel = poskytovatel[["id","dotaceposkytovatelnazev"]].rename(columns={'id':'iriposkytovateldotace', 'dotaceposkytovatelnazev':'poskytovatel'})
# doplnit ico k poskytovateli
poskytovatel["icoposkytovatele"] = poskytovatel.apply(lambda x: getIcoFromJmeno(ico=None, jmeno=x["poskytovatel"]), axis=1)

logger.info('pridavam informace o zdroji financi k rozhodnuti')
rozhodnuti = pd.merge(rozhodnuti, finzdroj, how="left", on="irifinancnizdroj" )
rozhodnuti = pd.merge(rozhodnuti, poskytovatel, how="left", on="iriposkytovateldotace" )

rozhodnuti = rozhodnuti[["iddotace","castkapozadovana","castkarozhodnuta","rokrozhodnuti","navratnostindikator","cerpani","zdrojfinanci","poskytovatel","icoposkytovatele"]]
rozhodnuti = rozhodnuti.rename(columns={'rokrozhodnuti':'rok', 'navratnostindikator':'jepujcka'})

#převést nan na null
rozhodnuti = rozhodnuti.where(pd.notnull(rozhodnuti), None)
# udělat z rozhodnutí dict
logger.info('transformuji rozhodnuti')
rozhodnuti["rozhodnuti"] = rozhodnuti.apply(lambda x: dict(castkaPozadovana=x['castkapozadovana'],castkaRozhodnuta=x['castkarozhodnuta'],rok=x['rok'],jePujcka=x['jepujcka'],cerpani=x['cerpani'],zdrojFinanci=x['zdrojfinanci'],poskytovatel=x['poskytovatel'],icoPoskytovatele=x['icoposkytovatele']), axis=1 )
rozhodnuti = rozhodnuti.groupby("iddotace")["rozhodnuti"].apply(list)

logger.info('spojujii rozhodnuti s dotaci')
# spojit s dotací
dotace = pd.merge(dotace, rozhodnuti, how="left", on="iddotace")
cerpani = None
finzdroj = None
poskytovatel = None
rozhodnuti = None

# redukovat a nastavit další dodatečné informace
logger.info('kosmeticke upravy')
dotace = dotace[['iddotace', 'podpisdatum', 'projektidnetifikator',
        'projektnazev', 'dtaktualizace', 'program', 'prijemce', 'rozhodnuti']]
dotace = dotace.rename(columns={'projektidnetifikator':'kodprojektu', 
                                'podpisdatum':'datumpodpisu',
                                'dtaktualizace':'datumaktualizace',
                                'projektnazev':'nazevprojektu'})
dotace["zdroj"] = "cedr"
dotace["url"] = "http://cedropendata.mfcr.cz/c3lod/cedr/resource/Dotace/" + dotace["iddotace"]
dotace["chyba"] = dotace["rozhodnuti"].apply(lambda x: ["Chybí informace (rozhodnutí) o výši dotace."] if type(x) != list else [])
#vynulovat nans (zobrazoval se ve sloupci program)
dotace = dotace.where(pd.notnull(dotace), None)
prep_dotace_for_export(dotace)
save_to_postgres(dotace, "dotace", "cedr")
logger.info('Cedr uspesne zpracovan')


##################################### EU FONDY #####################################
logger.info('Zpracovavam EU fondy ...')
def create_eufondy_rozhodnuti(rozhodnutocr, cerpanocr, rozhodnutoeu, cerpanoeu):
    czcerpanodict = [dict(castkaSpotrebovana = cerpanocr)] if cerpanocr != 0 else None
    eucerpanodict = [dict(castkaSpotrebovana = cerpanoeu)] if cerpanoeu != 0 else None
    czrozhodnutodict = dict(castkaRozhodnuta = rozhodnutocr, poskytovatel="CZ", cerpani=czcerpanodict)
    eurozhodnutodict = dict(castkaRozhodnuta = rozhodnutoeu, poskytovatel="EU", cerpani=eucerpanodict)
    return [czrozhodnutodict, eurozhodnutodict]

logger.info('Nacitam data za do roku 2006')
dotace2006 = pd.read_sql_table("dotace2006", postgre_cnn_import, schema="eufondy")
#dotace2006[dotace2006.duplicated("kod_projektu")]
# jsou zde dvě duplicity
# CZ.04.4.82/1.2.00.1/0034
# CZ.04.4.83/2.1.00.1/0235

dotace2006.drop_duplicates("kod_projektu", keep="first", inplace=True)

# dotace2006.apply(lambda x: [dict(castkaRozhodnuta=x["smlouva__eu_podil"],
#                                      poskytovatel="EU",
#                                      cerpani=[(dict(castkaSpotrebovana=x["proplaceno_eu_podil"]))])],
#                 axis=1)

# doplnit nuly, kde neznáme hodnotu
logger.info('fixuji vadna data')
dotace2006["smlouva_narodni_verejne_prostredky"] = dotace2006["smlouva_narodni_verejne_prostredky"].fillna(0)
dotace2006["proplaceno_narodni_verejne_prostredky"] = dotace2006["proplaceno_narodni_verejne_prostredky"].fillna(0)
dotace2006["smlouva__eu_podil"] = dotace2006["smlouva__eu_podil"].fillna(0)
dotace2006["proplaceno_eu_podil"] = dotace2006["proplaceno_eu_podil"].fillna(0)

# zabalit čerpání a rozhodnutí do dict
logger.info('vytvarim rozhodnuti')
dotace2006["rozhodnuti"] = dotace2006.apply(lambda x: create_eufondy_rozhodnuti(x["smlouva_narodni_verejne_prostredky"],x["proplaceno_narodni_verejne_prostredky"],x["smlouva__eu_podil"],x["proplaceno_eu_podil"] ),axis=1)

# chybí jakékoliv informace o výši dotace
logger.info('detekuji chyby')
dotace2006["chyba"] = dotace2006.apply(lambda x: ["Chybí informace (rozhodnutí) o výši dotace."] if 
   ((x["smlouva_narodni_verejne_prostredky"] == 0) &
    (x["proplaceno_narodni_verejne_prostredky"] == 0) &
    (x["smlouva__eu_podil"] == 0) &
    (x["proplaceno_eu_podil"] == 0) )
    else [], axis=1)
dotace2006["chyba"] += dotace2006["zadatel"].apply(lambda x: [] if x else ["Chybí informace o příjemci dotace."]) 

logger.info('kosmeticke upravy')
dotace2006["iddotace"] = dotace2006["kod_projektu"].apply(lambda x: "04-06-" + x )
dotace2006["program"] = dotace2006.apply(lambda x: dict(nazev=x["nazev_programu"],kod=x["cislo_programu"] ), axis=1)
dotace2006["ico"] = dotace2006["zadatel"].apply(lambda x: getIcoFromJmeno(ico=None, jmeno=x) if x else None)
dotace2006["prijemce"] = dotace2006.apply(lambda x: dict(obchodniJmeno=x["zadatel"],obec=x["obec"],psc=x["psc"],ico=x["ico"],hlidacJmeno=getJmenoFromIco(x["ico"])), axis=1)

dotace2006 = dotace2006.rename(columns={'kod_projektu':'kodprojektu',
                                        'zahajeni_projektu':'datumpodpisu',
                                        'nazev_projektu':'nazevprojektu'})
dotace2006 = dotace2006[["iddotace","datumpodpisu","kodprojektu","nazevprojektu","program","prijemce","rozhodnuti","chyba"]]
dotace2006["zdroj"] = "eufondy 2004-2006"
dotace2006["url"] = "https://www.dotaceeu.cz/cs/evropske-fondy-v-cr/programove-obdobi-2004-2006-(1)/vysledky-cerpani"
logger.info('2006 hotovo')
## 2013
logger.info('Nacitam data do roku 2013')
dotace2013 = pd.read_sql_table("dotace2013", postgre_cnn_import, schema="eufondy")

#dotace2013[dotace2013.duplicated("kod_projektu")]
logger.info('fixuji vadna data')
dotace2013["rozhodnuti_smlouva_o_poskytnuti_dotace_eu_zdroje_"] = dotace2013["rozhodnuti_smlouva_o_poskytnuti_dotace_eu_zdroje_"].fillna(0)
dotace2013["proplacene_prostredky_prijemcum_vyuctovane_eu_zdroje_"] = dotace2013["proplacene_prostredky_prijemcum_vyuctovane_eu_zdroje_"].fillna(0)
dotace2013["rozhodnuti_smlouva_o_poskytnuti_dotace_verejne_prostredky_celke"] = dotace2013["rozhodnuti_smlouva_o_poskytnuti_dotace_verejne_prostredky_celke"].fillna(0)
dotace2013["proplacene_prostredky_prijemcum_vyuctovane_verejne_prostredky_c"] = dotace2013["proplacene_prostredky_prijemcum_vyuctovane_verejne_prostredky_c"].fillna(0)

# zabalit čerpání a rozhodnutí do dict
logger.info('vytvarim rozhodnuti')
dotace2013["rozhodnuti"] = dotace2013.apply(lambda x: create_eufondy_rozhodnuti(x["rozhodnuti_smlouva_o_poskytnuti_dotace_verejne_prostredky_celke"],x["proplacene_prostredky_prijemcum_vyuctovane_verejne_prostredky_c"],x["rozhodnuti_smlouva_o_poskytnuti_dotace_eu_zdroje_"],x["proplacene_prostredky_prijemcum_vyuctovane_eu_zdroje_"] ),axis=1)

# chybí jakékoliv informace o výši dotace
logger.info('detekuji chyby')
dotace2013["chyba"] = dotace2013.apply(lambda x: ["Chybí informace (rozhodnutí) o výši dotace."] if 
   ((x["rozhodnuti_smlouva_o_poskytnuti_dotace_eu_zdroje_"] == 0) &
    (x["proplacene_prostredky_prijemcum_vyuctovane_eu_zdroje_"] == 0) &
    (x["rozhodnuti_smlouva_o_poskytnuti_dotace_verejne_prostredky_celke"] == 0) &
    (x["proplacene_prostredky_prijemcum_vyuctovane_verejne_prostredky_c"] == 0) )
    else [], axis=1)
dotace2013["chyba"] += dotace2013["zadatel"].apply(lambda x: [] if x else ["Chybí informace o příjemci dotace."]) 

logger.info('kosmeticke upravy')
dotace2013["iddotace"] = dotace2013["kod_projektu"].apply(lambda x: "07-13-" + x )
dotace2013["program"] = dotace2013.apply(lambda x: dict(nazev=x["cislo_a_nazev_programu"].split(" ",1)[1],kod=x["cislo_a_nazev_programu"].split(" ",1)[0] ), axis=1)
dotace2013["ic_zadatele"] = dotace2013["ic_zadatele"].apply(fix_ico_format)
dotace2013["ic_zadatele"] = dotace2013.apply(lambda x: getIcoFromJmeno(ico=x["ic_zadatele"], jmeno=x["zadatel"]), axis=1)
dotace2013["prijemce"] = dotace2013.apply(lambda x: dict(obchodniJmeno=x["zadatel"],obec=x["obec_zadatele_nazev"],psc=x["adresa_zadatele"].split(" ",1)[0],ico=x["ic_zadatele"],hlidacJmeno=getJmenoFromIco(x["ic_zadatele"])), axis=1)

dotace2013 = dotace2013.rename(columns={'kod_projektu':'kodprojektu',
                                        'datum_podpisu_smlouvy_rozhodnuti':'datumpodpisu',
                                        'nazev_projektu':'nazevprojektu'})
dotace2013 = dotace2013[["iddotace","datumpodpisu","kodprojektu","nazevprojektu","program","prijemce","rozhodnuti","chyba"]]
dotace2013["zdroj"] = "eufondy 2007-2013"
dotace2013["url"] = "https://dotaceeu.cz/cs/evropske-fondy-v-cr/programove-obdobi-2007-2013/cerpani-v-obdobi-2007-2013"
logger.info('2013 hotovo')
## 2020
logger.info('Nacitam data do roku 2020')
dotace2020 = pd.read_sql_table("dotace2020", postgre_cnn_import, schema="eufondy")

logger.info('vytvarim rozhodnuti')
dotace2020["rozhodnuti"] = dotace2020.apply(lambda x: [dict(castkaRozhodnuta = x["financovani_czv"], poskytovatel="ESIF", rok=(x["datum_zahajeni"].split("-",1)[0] if x["datum_zahajeni"] else None)  )] ,axis=1)

logger.info('detekuji chyby')
dotace2020["chyba"] = dotace2020["financovani_czv"].apply(lambda x: ["Chybí informace (rozhodnutí) o výši dotace."] if x == 0 else [])

logger.info('kosmeticke upravy')
dotace2020["iddotace"] = dotace2020["id"].apply(lambda x: "14-20-" + str(x) )
dotace2020["zadatel_ico"] = dotace2020["zadatel_ico"].apply(lambda x: fix_ico_format(float_to_text(x)))
dotace2020["zadatel_ico"] = dotace2020.apply(lambda x: getIcoFromJmeno(ico=x["zadatel_ico"], jmeno=x["zadatel_nazev"]), axis=1)
dotace2020["prijemce"] = dotace2020.apply(lambda x: dict(obchodniJmeno=x["zadatel_nazev"],obec=x["zadatel_obec"],psc=x["zadatel_psc"],ico=x["zadatel_ico"],hlidacJmeno=getJmenoFromIco(x["zadatel_ico"])), axis=1)
dotace2020["program"] = None
dotace2020 = dotace2020.rename(columns={'kod_projektu':'kodprojektu',
                                        'datum_zahajeni':'datumpodpisu',
                                        'naz':'nazevprojektu'})
dotace2020 = dotace2020[["iddotace","datumpodpisu","kodprojektu","nazevprojektu","program","prijemce","rozhodnuti","chyba"]]
dotace2020["zdroj"] = "eufondy 2014-2020"
dotace2020["url"] = "https://ms14opendata.mssf.cz/SeznamProjektu.xml"
logger.info('2020 hotovo')
## zkontrolovat tenhle záznam v databázi dotace2020["rozhodnuti"].iloc[89]
# prep_dotace_for_export(dotace2020)
# save_to_postgres(dotace2020.iloc[89:92], "dotacetst", "eufondy")

logger.info('slucuji eufondy dohromady')
# všechny do jednoho
dotace = pd.concat([dotace2006, dotace2013, dotace2020])

prep_dotace_for_export(dotace)
save_to_postgres(dotace, "dotace", "eufondy")
logger.info('EUfondy uspesne zpracovany')

## dotinfo
logger.info('Zpracovavam Dotinfo ...')
dotinfo = pd.read_sql_table("dotace", postgre_cnn_import, schema="dotinfo")

logger.info('Nastavuji id, datum a nazev')
dotinfo["iddotace"] = dotinfo["url"].apply(lambda x: x.rsplit("/",1)[1])
dotinfo["datumpodpisu"] = dotinfo["dotace_datum_vydani_rozhodnuti"].apply(lambda x: datetime.strptime(x, '%d. %m. %Y').strftime("%Y-%m-%dT00:00:00.000Z") if x else None)
dotinfo["nazevprojektu"] = dotinfo["dotace_nazev_dotace"]

# harakiri s kodem projektu (přesnější je ev.č.dotace, ale není vždy vyplněné - doplním z kódu projektu)
logger.info('fixuji kod projektu')
dotinfo["dotace_evidencni_cislo_dotace"] = dotinfo["dotace_evidencni_cislo_dotace"].apply(lambda x: None if isinstance(x, str) and (x.isspace() or not x) else x)
dotinfo["kodprojektu"] = dotinfo["dotace_evidencni_cislo_dotace"]
dotinfo["kodprojektu"] = dotinfo["kodprojektu"].fillna(dotinfo["kod_projektu"])

logger.info('pridavam nazev dotace a program')
dotinfo["dotace_nazev_dotace"] = dotinfo["dotace_nazev_dotace"].apply(lambda x: None if isinstance(x, str) and (x.isspace() or not x) else x)
dotinfo["program"] = dotinfo["dotace_nazev_dotace"].apply(lambda x: dict(nazev=x.split(" ",1)[0],kod=x.split(" ",1)[1] if len(x.split(" ",1)) > 1 else None ) if x else None)
# kontrola dotinfo[~dotinfo["dotace_nazev_dotace"].str.contains(" ")]

# normalizovat obchodni jmeno a jmeno (přidat null kde není nic)
logger.info('normalizuji jmeno')
dotinfo["ucastnik_obchodni_jmeno"] = dotinfo["ucastnik_obchodni_jmeno"].apply(lambda x: None if isinstance(x, str) and (x.isspace() or not x) else x)
dotinfo["ucastnik_prijemce_dotace_jmeno"] = dotinfo["ucastnik_prijemce_dotace_jmeno"].apply(lambda x: None if isinstance(x, str) and (x.isspace() or not x) else x)
# odmazat cokoliv co není ičo
logger.info('fixuji ico a jmeno')
dotinfo["ucastnik_ic_ucastnika_ic_zahranicni"] = dotinfo["ucastnik_ic_ucastnika_ic_zahranicni"].apply(lambda x: fix_ico_format(x))
# poté tam kde je ičo, nebo obchodní jméno doplnit ičo + hlidacjmeno
dotinfo["ico"] = dotinfo.apply(lambda x: getIcoFromJmeno(ico=x["ucastnik_ic_ucastnika_ic_zahranicni"], jmeno=x["ucastnik_obchodni_jmeno"]), axis=1)
dotinfo["hlidacjmeno"] = dotinfo["ico"].apply(getJmenoFromIco)
# tam kde není obchodní jméno doplnit jméno
dotinfo["ucastnik_obchodni_jmeno"] = dotinfo["ucastnik_obchodni_jmeno"].fillna(dotinfo["ucastnik_prijemce_dotace_jmeno"])

# vytvořit příjemce
logger.info('vytvarim prijemce')
dotinfo["prijemce"] = dotinfo.apply(lambda x: dict(obchodniJmeno=x["ucastnik_obchodni_jmeno"],obec=x["ucastnik_nazev_obce_doruc_posta"],okres=x["ucastnik_nazev_okresu"],psc=x["ucastnik_psc"],ico=x["ico"],hlidacJmeno=x["hlidacjmeno"]), axis=1)
# rozhodnuti
logger.info('cistim rozhodnuti')
dotinfo["jepujcka"] = dotinfo["dotace_forma_financovani_dotace"].apply(lambda x: True if x.upper() == 'NFV' else False)

dotinfo["dotace_castka_pozadovana"] = dotinfo["dotace_castka_pozadovana"].apply(lambda x: None if isinstance(x, str) and (x.isspace() or not x) else x)
dotinfo["dotace_castka_pozadovana"] = dotinfo["dotace_castka_pozadovana"].apply(lambda x: float(re.sub("[^0-9,]","",x).replace(",",".")) if x else 0 )
dotinfo["dotace_castka_schvalena"] = dotinfo["dotace_castka_schvalena"].apply(lambda x: None if isinstance(x, str) and (x.isspace() or not x) else x)
dotinfo["dotace_castka_schvalena"] = dotinfo["dotace_castka_schvalena"].apply(lambda x: float(re.sub("[^0-9,]","",x).replace(",",".")) if x else 0 )

logger.info('detekuji chyby')
dotinfo["chyba"] = dotinfo.apply(lambda x: ["Chybí informace (rozhodnutí) o výši dotace."] if 
   ((x["dotace_castka_pozadovana"] == 0) &
    (x["dotace_castka_schvalena"] == 0))
    else [], axis=1)

dotinfo["chyba"] += dotinfo.apply(lambda x: ["Chybí informace o příjemci dotace."] if (x["ucastnik_obchodni_jmeno"] == None) and (x["ico"] == None) else [], axis=1) 

logger.info('doplnuji rozhodnuti')
dotinfo["rozhodnuti"] = dotinfo.apply(lambda x: [dict(castkaPozadovana=x["dotace_castka_pozadovana"],castkaRozhodnuta=x["dotace_castka_schvalena"], poskytovatel=x["poskytovatel_poskytovatel_nazev_os"],icoPoskytovatele=fix_ico_format(x["poskytovatel_ic_poskytovatele"]),jePujcka=x["jepujcka"])] ,axis=1)

# zdroj
logger.info('kosmeticke zmeny')
dotinfo["zdroj"] = "dotinfo"

dotinfo = dotinfo[["iddotace","datumpodpisu","nazevprojektu","kodprojektu","program","prijemce","rozhodnuti","url","zdroj","chyba"]]

prep_dotace_for_export(dotinfo)
save_to_postgres(dotinfo, "dotace", "dotinfo")
logger.info('Data z dotinfo uspesne zpracovana')

## szif
logger.info('Zpracovavam SZIF ...')
def create_szif_rozhodnuti(cerpanocr, cerpanoeu, zdroj, rok):
    czcerpanodict = [dict(castkaSpotrebovana = cerpanocr)] if cerpanocr != None else None
    eucerpanodict = [dict(castkaSpotrebovana = cerpanoeu)] if cerpanoeu != None else None
    czrozhodnutodict = dict(castkaRozhodnuta = 0, rok=rok, poskytovatel="CZ", zdroj=zdroj, cerpani=czcerpanodict)
    eurozhodnutodict = dict(castkaRozhodnuta = 0, rok=rok, poskytovatel="EU", zdroj=zdroj, cerpani=eucerpanodict)
    return [czrozhodnutodict, eurozhodnutodict]


szif = pd.read_sql_table("dotace", postgre_cnn_import, schema="szif")
# opravit špatně načtený datatyp
szif["castka_cr"] = szif["castka_cr"].astype('float')
szif["castka_eu"] = szif["castka_eu"].astype('float').fillna(0)
logger.info('doplnuji rozhodnuti')
szif["rozhodnuti"] = szif.apply(lambda x: create_szif_rozhodnuti(x["castka_cr"], x["castka_eu"], x["zdroj"], re.search(r"(\d{4})-",x["id"])[1]), axis=1 )
logger.info('doplnuji program')
szif["program"] = szif["opatreni"].apply(lambda x: dict(nazev=x))
logger.info('detekuji chyby')
szif["chyba"] = szif.apply(lambda x: [], axis=1)
logger.info('doplnuji ico')
szif["ico"] = szif.apply(lambda x: getIcoFromJmeno(fix_ico_format(float_to_text(x["ico"])), x["jmeno"]), axis=1)
logger.info('nastavuji nazev projektu')
szif["nazevprojektu"] = szif["opatreni"]
logger.info('doplnuji prijemce')
szif["prijemce"] = szif.apply(lambda x: dict(obchodniJmeno=x["jmeno"],obec=x["obec"],okres=x["okres"],ico=x["ico"],hlidacJmeno=getJmenoFromIco(x["ico"])), axis=1)
logger.info('kosmeticke upravy')
szif["zdroj"] = szif["id"].apply(lambda x: "szif " + re.search(r"spd(\d{4})-",x)[1] )
szif["url"] = "https://www.szif.cz/cs"
szif = szif.rename(columns={'id':'iddotace'})

szif = szif[['iddotace', 'nazevprojektu', 'program', 'prijemce', 'rozhodnuti', 'chyba', 'zdroj', 'url']]

prep_dotace_for_export(szif)
save_to_postgres(szif, "dotace", "szif")
logger.info('SZIF uspesne zpracovano')


## czechinvest
logger.info('Zpracovavam Czechinvest ...')
def create_czi_rozhodnuti(rozhodnuto, rok):
    czrozhodnutodict = dict(castkaRozhodnuta = rozhodnuto, rok=rok, zdroj="CZ")
    return [czrozhodnutodict]


czi = pd.read_sql_table("dotace", postgre_cnn_import, schema="czechinvest")

# fixnout data
czi["castka"] = czi["rozhodnuti_mil_czk"].apply(lambda x: x * 1000000)
czi["datumpodpisu"] = czi["rok_podani"].apply(lambda x: datetime.strptime(str(x), '%Y').strftime("%Y-%m-%dT00:00:00.000Z") if x else None)
czi["iddotace"] = czi["id"].astype(str)

czi["rozhodnuti_rok"] = czi["rozhodnuti_rok"].apply(lambda x: re.match(r"\d\d\d\d", x).group() if x!= None else None)
czi["rozhodnuti_rok"] = czi["rozhodnuti_rok"].astype(int)

# odstranit zrušené dotace
czi = czi[czi["zruseno"].isna()]

logger.info('doplnuji rozhodnuti')
czi["rozhodnuti"] = czi.apply(lambda x: create_czi_rozhodnuti(x["castka"], x["rozhodnuti_rok"]), axis=1 )
logger.info('zabaluji program')
czi["program"] = czi["program"].apply(lambda x: dict(nazev=x))
logger.info('detekuji chyby')
czi["chyba"] = czi.apply(lambda x: [], axis=1)
logger.info('doplnuji ico')
czi["ico"] = czi.apply(lambda x: getIcoFromJmeno(fix_ico_format(x["ico"]), x["prijemce"]), axis=1)
logger.info('nastavuji nazev projektu')
czi["nazevprojektu"] = czi["projekt"]
logger.info('doplnuji prijemce')
czi["prijemce"] = czi.apply(lambda x: dict(obchodniJmeno=x["prijemce"],ico=x["ico"],hlidacJmeno=getJmenoFromIco(x["ico"])), axis=1)

logger.info('kosmeticke upravy')
czi["zdroj"] = "czechinvest"
czi["url"] = "https://www.czechinvest.org/cz/Sluzby-pro-investory/Investicni-pobidky"
#czi = czi.rename(columns={'id':'iddotace'})

czi = czi[['iddotace', 'nazevprojektu', 'program', 'prijemce', 'rozhodnuti', 'chyba', 'zdroj', 'url']]

prep_dotace_for_export(czi)
save_to_postgres(czi, "dotace", "czechinvest")
logger.info('czechinvest uspesne zpracovan')

logger.info('Konec skriptu')



#test rest,.. atakdále
# dotace2006[((dotace2006["smlouva_narodni_verejne_prostredky"] == 0) &
#     (dotace2006["proplaceno_narodni_verejne_prostredky"] == 0) &
#     (dotace2006["smlouva__eu_podil"] == 0) &
#     (dotace2006["proplaceno_eu_podil"] == 0) )]

# dotace2006[~ dotace2006["chyba"].isna()]

# ['iddotace', 'datumpodpisu', 'nazevprojektu', 'program', 'prijemce',
#        'rozhodnuti', 'chyba', 'zdroj', 'url']
# ['iddotace', 'datumpodpisu', 'kodprojektu', 'nazevprojektu',
#        'datumaktualizace', 'program', 'prijemce', 'rozhodnuti',
#         'zdroj', 'url',
#        'chyba']


# ((~szif["id"].str.contains("spd2014")) & (~szif["id"].str.contains("spd2015")) & (~szif["id"].str.contains("spd2016"))& (~szif["id"].str.contains("spd2017"))& (~szif["id"].str.contains("spd2018")))
