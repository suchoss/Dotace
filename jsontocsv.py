# script pro zploštění dotace z hlídače do csv

import pandas as pd
import json

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

# df = pd.read_json("expo.json", orient='records')

with open('expo.json', encoding='utf8') as f:
    d = json.load(f)

df = pd.json_normalize(d)

rozhodnuti = pd.json_normalize(data=d, record_path='Rozhodnuti',
                               meta=['IdDotace']).add_prefix("Rozhodnuti.")

##
# merged[merged.duplicated(["Rozhodnuti.IdDotace"])].sort_values(by=["Rozhodnuti.IdDotace"]).head(2)
rozhodnuti['rid'] = rozhodnuti.index
rozhodnuti.rid = rozhodnuti.rid.apply(str)
tojson = json.loads(rozhodnuti.to_json(orient='records'))
cerpani = pd.json_normalize(data=tojson, record_path='Rozhodnuti.Cerpani', meta=['rid']).add_prefix("Cerpani.")
rozcer = pd.merge(rozhodnuti, cerpani, how='left', left_on='rid', right_on="Cerpani.rid")
rozcer.drop(["Rozhodnuti.Cerpani"], axis=1, inplace=True)
merged = pd.merge(df, rozcer, how='left', left_on='IdDotace', right_on="Rozhodnuti.IdDotace")
merged.drop(["Rozhodnuti"], axis=1, inplace=True)

merged.to_csv("dotace_flat.csv", sep=";", encoding='utf8')