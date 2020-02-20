CREATE INDEX ON cedr.mv_dotace (idprojektu);
CREATE INDEX ON dotinfo.dotace ((lower(kod_projektu)));
CREATE INDEX ON eufondy.dotace2006 (kod_projektu);
CREATE INDEX ON eufondy.dotace2013 (kod_projektu);

select count(*) 
from eufondy.dotace2006 eu
where not exists (select 1 from cedr.mv_dotace ced where ced.kodprojektu = eu.kod_projektu)

select count(*) 
from eufondy.dotace2013 eu
where exists (select 1 from cedr.mv_dotace ced where ced.kodprojektu = eu.kod_projektu)
