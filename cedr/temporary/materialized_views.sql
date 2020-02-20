create materialized view cedr.mv_dotace as 
select dot.iddotace, dot.podpisdatum datumpodpisu, dot.projektnazev nazevprojektu, dot.projektidnetifikator idprojektu, 
   		  dot.projektkod kodprojektu, dot.dtaktualizace datumaktualizace, pri.obchodnijmeno prijemceobchodnijmeno, 
		  pri.jmeno || ' ' || pri.prijmeni prijemcejmeno, pri.ico prijemceico, pri.roknarozeni prijemceroknarozeni,
		  adr.obecnazev prijemceobec, adr.okresnazev prijemceokres, adr.psc prijemcepsc,
		  COALESCE(pro.programnazev, opr.nazev, opa.nazev, grs.nazev) programnazev, 
		  COALESCE(pro.programkod, opr.kod, opa.kod, grs.kod) programkod,
		  'http://cedropendata.mfcr.cz/c3lod/cedr/resource/Dotace/'|| dot.iddotace as urlzdroje,
		  'cedr' as nazevzdroje
     from cedr.dotace dot
left join cedr.prijemcepomoci pri on pri.idprijemce = dot.idprijemce
left join cedr.v_adresa adr on pri.idprijemce = adr.idprijemce
left join cedr.ciselnikprogramv01 pro on dot.iriprogram = pro.id
left join cedr.v_operacniprogram opr on dot.irioperacniprogram = opr.id
left join cedr.v_opatreni opa on dot.iriopatreni = opa.id
left join cedr.v_grantoveschema grs on dot.irigrantoveschema = grs.id;

create materialized view cedr.mv_rozhodnuti as 
select roz.iddotace, roz.idrozhodnuti id, roz.castkapozadovana, roz.castkarozhodnuta, roz.rokrozhodnuti rok,
	   roz.navratnostindikator jepujcka, fzd.financnizdrojnazev zdrojfinanci, pos.dotaceposkytovatelnazev poskytovatel,
	   'cedr' as nazevzdroje
from cedr.rozhodnuti roz
left join cedr.ciselnikfinancnizdrojv01 fzd on roz.irifinancnizdroj = fzd.id
left join cedr.ciselnikdotaceposkytovatelv01 pos on roz.iriposkytovateldotace = pos.id
where roz.refundaceindikator = 'false';

create materialized view cedr.mv_cerpani as 
select obd.idrozhodnuti, obd.idobdobi id, obd.castkacerpana - coalesce(obd.castkavracena,0) castkaspotrebovana, obd.rozpoctoveobdobi rok,
	   'cedr' as nazevzdroje
  from cedr.rozpoctoveobdobi obd
 where exists(select 1 
			from cedr.mv_rozhodnuti roz
			where obd.idrozhodnuti = roz.id);