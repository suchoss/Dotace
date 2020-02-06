create materialized view importcedr.mv_dotace as 
select dot.iddotace, dot.podpisdatum datumpodpisu, dot.projektnazev nazevprojektu, dot.projektidnetifikator idprojektu, 
   		  dot.projektkod kodprojektu, dot.dtaktualizace datumaktualizace, pri.obchodnijmeno prijemceobchodnijmeno, 
		  pri.jmeno || ' ' || pri.prijmeni prijemcejmeno, pri.ico prijemceico, pri.roknarozeni prijemceroknarozeni,
		  adr.obecnazev prijemceobec, adr.okresnazev prijemceokres, adr.psc prijemcepsc,
		  COALESCE(pro.programnazev, opr.nazev, opa.nazev, grs.nazev) programnazev, 
		  COALESCE(pro.programkod, opr.kod, opa.kod, grs.kod) programkod,
		  'http://cedropendata.mfcr.cz/c3lod/cedr/resource/Dotace/'|| dot.iddotace as urlzdroje,
		  'cedr' as nazevzdroje,
		  md5(ROW(dot.iddotace, dot.podpisdatum, dot.projektnazev, dot.projektidnetifikator, 
   		  dot.projektkod, dot.dtaktualizace, pri.obchodnijmeno, 
		  pri.jmeno || ' ' || pri.prijmeni, pri.ico, pri.roknarozeni,
		  adr.obecnazev, adr.okresnazev, adr.psc,
		  COALESCE(pro.programnazev, opr.nazev, opa.nazev, grs.nazev), 
		  COALESCE(pro.programkod, opr.kod, opa.kod, grs.kod))::TEXT) as hash
     from importcedr.dotace dot
left join importcedr.prijemcepomoci pri on pri.idprijemce = dot.idprijemce
left join importcedr.v_adresa adr on pri.idprijemce = adr.idprijemce
left join importcedr.ciselnikprogramv01 pro on dot.iriprogram = pro.id
left join importcedr.v_operacniprogram opr on dot.irioperacniprogram = opr.id
left join importcedr.v_opatreni opa on dot.iriopatreni = opa.id
left join importcedr.v_grantoveschema grs on dot.irigrantoveschema = grs.id;

create materialized view importcedr.mv_rozhodnuti as 
select roz.iddotace, roz.idrozhodnuti id, roz.castkapozadovana, roz.castkarozhodnuta, roz.rokrozhodnuti rok,
	   roz.navratnostindikator jepujcka, fzd.financnizdrojnazev zdrojfinanci, pos.dotaceposkytovatelnazev poskytovatel,
	   'cedr' as nazevzdroje,
	   md5(ROW(roz.iddotace, roz.idrozhodnuti, roz.castkapozadovana, roz.castkarozhodnuta, roz.rokrozhodnuti,
	   		   roz.navratnostindikator, fzd.financnizdrojnazev, pos.dotaceposkytovatelnazev)::TEXT) as hash
from importcedr.rozhodnuti roz
left join importcedr.ciselnikfinancnizdrojv01 fzd on roz.irifinancnizdroj = fzd.id
left join importcedr.ciselnikdotaceposkytovatelv01 pos on roz.iriposkytovateldotace = pos.id
where roz.refundaceindikator = 'false';

create materialized view importcedr.mv_cerpani as 
select obd.idrozhodnuti, obd.idobdobi id, obd.castkaspotrebovana, obd.rozpoctoveobdobi rok,
	   'cedr' as nazevzdroje,
	   md5(ROW(obd.idrozhodnuti, obd.idobdobi, obd.castkaspotrebovana, obd.rozpoctoveobdobi)::TEXT) as hash
  from importcedr.rozpoctoveobdobi obd
 where exists(select 1 
			from importcedr.mv_rozhodnuti roz
			where obd.idrozhodnuti = roz.id);

/*
CREATE INDEX ind_mv_cerpani ON importcedr.mv_cerpani USING HASH (idrozhodnuti);
CREATE INDEX ind_mv_dotace ON importcedr.mv_dotace USING HASH (iddotace);	
CREATE INDEX ind_mv_rozhodnuti2 ON importcedr.mv_rozhodnuti USING HASH (iddotace);
CREATE INDEX ind_mv_rozhodnuti ON importcedr.mv_rozhodnuti USING HASH (id);
*/