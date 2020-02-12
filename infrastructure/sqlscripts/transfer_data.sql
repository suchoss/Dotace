-- transfer data
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
		'rozhodnuti', roz.rozhodnuti
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

--update hashes
update export.dotacejson set hash = md5(data::TEXT);