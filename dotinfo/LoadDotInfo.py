import pandas as pd
import unidecode
import re
import logging

pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

FILE_PATH = 'data/dotinfo.json'

logging.info('Načítám json')
df = pd.read_json(FILE_PATH)

# drop paging
logging.info('Mažu paging')
df = df[df.type != 'paging']


def tab1_to_series(text):
    keys, values = zip(*[('dotace_' + dct['name'], dct['value']) for dct in text['tab1']['data']])
    return pd.Series(values, index=keys)


def tab2_to_series(text):
    keys, values = zip(*[('ucastnik_' + dct['name'], dct['value']) for dct in text['tab2']['data']])
    return pd.Series(values, index=keys)


def tab3_to_series(text):
    keys, values = zip(*[('identifikace_' + dct['name'], dct['value']) for dct in text['tab3']['data']])
    return pd.Series(values, index=keys)


def tab4_to_series(text):
    keys, values = zip(*[('poskytovatel_' + dct['name'], dct['value']) for dct in text['tab4']['data']])
    return pd.Series(values, index=keys)


def normalize_column_name(name):
    strippedChars = re.sub(r'\W+', '_', name)
    return unidecode.unidecode(strippedChars).lower()


logging.info('Rozbaluji tab1')
df = pd.concat([df, df['data'].apply(tab1_to_series)], axis=1)
logging.info('Rozbaluji tab2')
df = pd.concat([df, df['data'].apply(tab2_to_series)], axis=1)
logging.info('Rozbaluji tab3')
df = pd.concat([df, df['data'].apply(tab3_to_series)], axis=1)
logging.info('Rozbaluji tab4')
df = pd.concat([df, df['data'].apply(tab4_to_series)], axis=1)

# normalizovat názvy sloupců
logging.info('Normalizuji názvy sloupců')
df.columns = [normalize_column_name(c) for c in df.columns]
# odmazat zbytečné sloupce
logging.info('Promazávám zbytečné sloupce')
df = df.drop(['type', 'pagetitle', 'data'], axis='columns')

# připojení k postgres
dbschema = 'importcedr'
engine = create_engine('postgresql://postgres:xxx@localhost:5432/postgres')
if not engine.dialect.has_schema(engine, dbschema):
    engine.execute(schema.CreateSchema('importcedr'))

# drop views
runSqlQueries(engine, 'drop_views.sql')

indLabel = df.columns[0]
df.set_index(df.columns[0], inplace=True)

df.to_sql('dotace',
          engine,
          schema=dbschema,
          if_exists='replace',
          chunksize=100,  # definuje, kolik kusů naráz se zapisuje
          index=True,
          index_label=indLabel,
          method='multi')
