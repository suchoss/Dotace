-- transfer data
insert into export.dotace select * from importcedr.mv_dotace;
insert into export.rozhodnuti select * from importcedr.mv_rozhodnuti;
insert into export.cerpani select * from importcedr.mv_cerpani;