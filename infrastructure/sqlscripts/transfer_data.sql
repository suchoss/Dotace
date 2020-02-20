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
		'zdroje', json_build_array(
		json_build_object(
        	'nazev', d.nazevzdroje,
        	'url', d.urlzdroje,
        	'isPrimary', 'true'
    	)),
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
		'chyba', case when roz.iddotace is null then 'Krom refundace neexistuje záznam o rozhodnutí, nebo čerpání.' else null end
	  ) as data
  from cedr.mv_dotace d 
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
  from cedr.mv_rozhodnuti r 
left join (
select c.idrozhodnuti, c.nazevzdroje,
      json_agg(json_build_object(
	  	'id', c.id,
		'castkaSpotrebovana', c.castkaspotrebovana,
		'rok', c.rok
	  )) as cerpani
  from cedr.mv_cerpani c 
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
	'zdroje', json_build_array(
		json_build_object(
        	'nazev', 'dotinfo',
        	'url', url,
        	'isPrimary', 'true'
    	)),
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
    ))
) as data
from dotinfo.dotace di
where not exists (select 1 from cedr.mv_dotace ced where ced.idprojektu = di.kod_projektu);

--eufondy 2006
insert into export.dotacejson (iddotace, nazevzdroje, data)
select kod_projektu iddotace, 'eufondy 04_06' nazevzdroje,
json_build_object(
    'idDotace', kod_projektu,
    'datumPodpisu', zahajeni_projektu,
    'nazevProjektu', nazev_projektu,
    'idProjektu', kod_projektu,
	'zdroje', json_build_array(
		json_build_object(
        	'nazev', 'eufondy 04_06',
        	'isPrimary', 'true'
    	)),
    'program', json_build_object(
        'nazev', nazev_programu,
		'kod', cislo_programu
    ),
    'prijemce', json_build_object(
        'obchodniJmeno', zadatel,
        'obec', obec,
        'psc', psc
    ),
    'rozhodnuti', json_build_array(
		json_build_object(
        	'castkaRozhodnuta', smlouva__eu_podil,
        	'poskytovatel', 'EU',
        	'cerpani', json_build_array(json_build_object(
				'castkaSpotrebovana', proplaceno_eu_podil
			 ))
    	),
		json_build_object(
        	'castkaRozhodnuta', smlouva_narodni_verejne_prostredky ,
        	'poskytovatel', 'CZ',
			'cerpani', json_build_array(json_build_object(
				'castkaSpotrebovana', proplaceno_narodni_verejne_prostredky
			 ))
    	))
) as data
  from eufondy.dotace2006 dot
 where not exists (select 1 from cedr.mv_dotace ced where ced.idprojektu = dot.kod_projektu);

--eufondy 2013
insert into export.dotacejson (iddotace, nazevzdroje, data)
select kod_projektu iddotace, 'eufondy 07_13' nazevzdroje,
json_build_object(
    'idDotace', kod_projektu,
    'datumPodpisu', datum_podpisu_smlouvy_rozhodnuti,
    'nazevProjektu', nazev_projektu,
    'idProjektu', kod_projektu,
    'zdroje', json_build_array(
		json_build_object(
        	'nazev', 'eufondy 07_13',
        	'isPrimary', 'true'
    	)),
    'program', json_build_object(
        'nazev', cislo_a_nazev_programu
    ),
    'prijemce', json_build_object(
        'obchodniJmeno', zadatel,
        'obec', obec_zadatele_nazev,
        'psc', split_part(adresa_zadatele,' ',1),
		'ico', ic_zadatele
    ),
    'rozhodnuti', json_build_array(
		json_build_object(
        	'castkaRozhodnuta', rozhodnuti_smlouva_o_poskytnuti_dotace_eu_zdroje_,
        	'poskytovatel', 'EU',
        	'cerpani', json_build_array(json_build_object(
				'castkaSpotrebovana', proplacene_prostredky_prijemcum_vyuctovane_eu_zdroje_
			 ))
    	),
		json_build_object(
        	'castkaRozhodnuta', rozhodnuti_smlouva_o_poskytnuti_dotace_verejne_prostredky_celke,
        	'poskytovatel', 'CZ',
			'cerpani', json_build_array(json_build_object(
				'castkaSpotrebovana', proplacene_prostredky_prijemcum_vyuctovane_verejne_prostredky_c
			 ))
    	))
) as data
  from eufondy.dotace2013 dot
 where not exists (select 1 from cedr.mv_dotace ced where ced.idprojektu = dot.kod_projektu);

--eufondy 2020
insert into export.dotacejson (iddotace, nazevzdroje, data)
select id iddotace, 'eufondy 13_20' nazevzdroje,
json_build_object(
    'idDotace', id,
    'datumPodpisu', datum_zahajeni,
    'nazevProjektu', naz,
    'idProjektu', kod_projektu,
    'zdroje', json_build_array(
		json_build_object(
        	'nazev', 'eufondy 13_20',
        	'isPrimary', 'true'
    	)),
    'prijemce', json_build_object(
        'obchodniJmeno', zadatel_nazev,
        'obec', zadatel_obec,
		'okres', zadatel_okres,
        'psc', zadatel_psc,
		'ico', zadatel_ico
    ),
    'rozhodnuti', json_build_array(
		json_build_object(
        	'castkaRozhodnuta', financovani_czv,
        	'poskytovatel', 'ESIF'
    	))
) as data
  from eufondy.dotacenew dot
 where not exists (select 1 from cedr.mv_dotace ced where ced.idprojektu = dot.kod_projektu);