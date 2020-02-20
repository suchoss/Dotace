import pandas as pd
from sqlalchemy import create_engine, types, schema

dbschema = 'eufondy'
engine = create_engine('postgresql://postgres:xxx@localhost:5432/postgres')
if not engine.dialect.has_schema(engine, dbschema):
    engine.execute(schema.CreateSchema(dbschema))

df = pd.read_csv('df06.csv')
df.to_sql('06',
          engine,
          schema=dbschema,
          if_exists='replace',
          chunksize=100,  # definuje, kolik kusů naráz se zapisuje
          method='multi')

df = pd.read_csv('df13.csv')
df.to_sql('13',
          engine,
          schema=dbschema,
          if_exists='replace',
          chunksize=100,  # definuje, kolik kusů naráz se zapisuje
          method='multi')
