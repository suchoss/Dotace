import pandas as pd

# inicializace
pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

cedr = pd.read_csv('tst/cedrids.csv')
cedr['idprojektu'] = cedr['idprojektu'].str.lower().str.strip()

df06 = pd.read_excel('data/2004-2006.xlsx')
# mazání duplicit
df06.drop_duplicates(['Číslo programu', 'Název programu', 'Číslo priority', 'Název priority', 'Číslo opatření',
                      'Název opatření', 'Číslo projektu', 'Název projektu', 'Stav projektu', 'Žadatel',
                      'Smlouva_ EU podíl', 'Smlouva_Národní veřejné prostředky', 'Proplaceno EU podíl',
                      'Proplaceno_národní veřejné prostředky', 'Zahájení projektu', 'Ukončení projektu'], inplace=True)
df06['cip'] = df06['Číslo projektu'].str.lower().str.strip()
df06 = df06.assign(InCedr=df06['cip'].isin(cedr['idprojektu']).astype(int))

df13 = pd.read_excel('data/2007-2013.xlsx')
df13 = df13[df13['Pořadí v rámci v projektu (filtr)'] == 1]
df13['cip'] = df13['Číslo projektu'].str.lower().str.strip()
df13 = df13.assign(InCedr=df13['cip'].isin(cedr['idprojektu']).astype(int))

df06['InCedr'].value_counts()
df13['InCedr'].value_counts()

dotinfo = pd.read_csv('tst/dotinfo.csv')
dotinfo['cleanid'] = dotinfo['ids'].str.lower().str.strip()
dotinfo = dotinfo.assign(InCedr=dotinfo['cleanid'].isin(cedr['idprojektu']).astype(int))

dotinfo[dotinfo['InCedr'] == 1].head(10)['ids']