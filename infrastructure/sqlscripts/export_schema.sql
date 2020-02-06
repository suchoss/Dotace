-- script which prepares output tables
CREATE SCHEMA export;

create table export.dotace(
iddotace text,
datumpodpisu text,
nazevprojektu text,
idprojektu text,
kodprojektu text,
datumaktualizace text,
prijemceobchodnijmeno text,
prijemcejmeno text,
prijemceico double precision,
prijemceroknarozeni double precision,
prijemceobec text,
prijemceokres text,
prijemcepsc double precision,
programnazev text,
programkod text,
urlzdroje text,
nazevzdroje text,
hash text,
primary key (iddotace,nazevzdroje)
)

create table export.rozhodnuti(
iddotace text,
id text,
castkapozadovana double precision,
castkarozhodnuta double precision,
rok bigint,
jepujcka boolean,
zdrojfinanci text,
poskytovatel text,
nazevzdroje text,
hash text,
primary key (id,nazevzdroje),
FOREIGN KEY (iddotace,nazevzdroje) REFERENCES export.dotace(iddotace,nazevzdroje)
)

create table export.cerpani(
idrozhodnuti text,
id text,
castkaspotrebovana double precision,
rok bigint,
nazevzdroje text,
hash text,
FOREIGN KEY (idrozhodnuti,nazevzdroje) REFERENCES export.rozhodnuti(id,nazevzdroje)
)

