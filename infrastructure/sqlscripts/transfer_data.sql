-- transfer data
-- cedr
truncate table export.dotacejson;

insert into export.dotacejson (iddotace, nazevzdroje, data) 
select d.iddotace, d.nazevzdroje,
      json_build_object(
	  	'idDotace', d.iddotace,
		'datumPodpisu', d.datumpodpisu,
		'nazevProjektu', d.nazevprojektu,
		'idProjektu', d.idprojektu,
		'kodProjektu', d.kodprojektu,
		'datumAktualizace', d.datumaktualizace,
		'urlZdroje', d.urlZdroje,
		'nazevZdroje', d.nazevzdroje,
		'prijemce', json_build_object(
			'obchodniJmeno', d.prijemceobchodnijmeno,
			'jmeno', d.prijemcejmeno,
			'ico', d.prijemceico,
			'rokNarozeni', d.prijemceroknarozeni,
			'obec', d.prijemceobec,
			'okres', d.prijemceokres,
			'psc', d.prijemcepsc
		),
		'program', json_build_object(
			'nazev', d.programnazev,
			'kod', d.programkod
		),
		'rozhodnuti', roz.rozhodnuti,
		'jeChyba', case when roz.iddotace is null then true else false end
	  ) as data
  from importcedr.mv_dotace d 
left join (
select r.iddotace, r.nazevzdroje,
      json_agg(json_build_object(
	  	'id', r.id,
		'castkaPozadovana', r.castkapozadovana,
		'castkaRozhodnuta', r.castkarozhodnuta,
		'rok', r.rok,
		'jePujcka', r.jepujcka,
		'zdrojFinanci', r.zdrojfinanci,
		'poskytovatel', r.poskytovatel,
		'cerpani', cer.cerpani
	  )) as rozhodnuti
  from importcedr.mv_rozhodnuti r 
left join (
select c.idrozhodnuti, c.nazevzdroje,
      json_agg(json_build_object(
	  	'id', c.id,
		'castkaSpotrebovana', c.castkaspotrebovana,
		'rok', c.rok
	  )) as cerpani
  from importcedr.mv_cerpani c 
group by c.idrozhodnuti, c.nazevzdroje
) cer on r.id = cer.idrozhodnuti and r.nazevzdroje = cer.nazevzdroje
group by r.iddotace, r.nazevzdroje
) roz on roz.iddotace = d.iddotace and roz.nazevzdroje = d.nazevzdroje;

-- dotinfo
insert into export.dotacejson (iddotace, nazevzdroje, data) 
select split_part(url, '/', 5) iddotace, 'dotinfo' nazevzdroje,
json_build_object(
    'idDotace', split_part(url, '/', 5),
    'datumPodpisu', to_date(nullif(dotace_datum_vydani_rozhodnuti,''), 'dd.mm.yyyy'),
    'nazevProjektu', dotace_nazev_dotace,
    'idProjektu', dotace_identifikator_dot_kod_is,
    'urlZdroje', url,
    'nazevZdroje', 'dotinfo',
    'kodProjektu', dotace_evidencni_cislo_dotace,
    'program', json_build_object(
        'nazev', dotace_vyuziti_dotace
    ),
    'prijemce', json_build_object(
        'obchodniJmeno', ucastnik_obchodni_jmeno,
        'ico', ucastnik_ic_ucastnika_ic_zahranicni,
        'jmeno', ucastnik_prijemce_dotace_jmeno,
        'obec', ucastnik_nazev_obce_doruc_posta,
        'psc', ucastnik_psc,
        'okres', ucastnik_nazev_okresu
    ),
    'rozhodnuti', json_build_array(json_build_object(
        'jePujcka', case when dotace_forma_financovani_dotace = 'NFV' then 'true' else 'false' end,
        'castkaPozadovana', NULLIF(REPLACE(REGEXP_REPLACE(dotace_castka_pozadovana,'[^\d,]','','g'),',','.'),'')::numeric,
        'castkaRozhodnuta', NULLIF(REPLACE(REGEXP_REPLACE(dotace_castka_schvalena,'[^\d,]','','g'),',','.'),'')::numeric,
        'poskytovatel', poskytovatel_poskytovatel_nazev_os,
        'icoPoskytovatele', poskytovatel_ic_poskytovatele
    )),
    'jeChyba', false
) as data
from importdotinfo.dotace;

--update hashes
update export.dotacejson set hash = md5(data::TEXT);