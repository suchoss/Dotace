# Dotace
Projekt pro stažení různých zdrojů dotací, jejich sjednocení a uložení do jedné databáze

## Problémy se zdroji dat

### Cedr

- Zbytečně složitá struktura - pro většinu lidí nepochopitelná, obyčejný člověk nebude mít čas, aby něco takového koumal
- Spousta dat je naprosto nesmyslně ve dvou tabulkách namísto jedné a informaci je potřeba zbytečně dohledávat 2x:
  - ciselnikcedroperacniprogramv01 + ciselnikmmroperacniprogramv01
  - ciselnikcedropatreniv01 + ciselnikmmropatreniv01
  - ciselnikcedrgrantoveschemav01 + ciselnikmmrgrantoveschemav01
  - adresasidlo + adresabydliste

- Dokumentace je zkratkovitá a některé věci se z ní nedají jednoduše (například co znamená refundace indikátor?!) pochopit. Nebýt pomoci lidí z dotačního parazita a ochoty některých lidí z CEDRu, tak bychom nedali ani správnou reprezentaci dat dokupy.

- `ciselnikobecv01` nejde jednoduše napojit na `ciselnikokresv01`, protože ID nesedí - je potřeba upravit id a odseknout poslední část u ciselniku okres
`prijemcepomoci` - obsahuje nesmyslné ičo u různých subjektů (00000001, 99999999)

- V rámci CEDRu je také nekonzistentní způsob uložení informace např.
  - *PRA-V-36/2003*, kdy jeden kód projektu má dva záznamy v dotacích pro stejného příjemce
  - *CZ.1.01/2.1.00/09.0132*, kdy jeden kód projektu má jeden záznam v dotacích, s 601 záznamy o čerpání
    - navíc konkrétně u této dotace pro ŘSD nechápu, proč by měli dostat dotaci od "Státní fond kinematografie" [1 507 871 Kč v roce 2010 a 1 507 871 Kč v roce 2013], obzvlášť když název projektu je "Dálnice D3 Tábor-Veselí nad Lužnicí")

- Častokrát se v identifikátoru projektu vyskytují nesmyslné údaje - např. slovo "smlouvy"

### EU Fondy
- past za pastí - stejná autorita tři rozdílné zdroje s rozdílnou strukturou, rozdílnými názvy sloupců a každý má jiný obsah:
  - soubor 2004 - 2006 (xls); chybí ičo k identifikaci subjektů
  - soubor 2007 - 2013 (xls)
  - soubor 2014 - 2020 (xml)
- Soubory jsou navíc poschovávané na různých místech webu (ještě že jim alespoň funguje online chat, kde jsou ochotní a pomohli)
- Dokumentace? 

Dotacím chybí id

### DotInfo

- PEKLO!
- web vypadá, jako by ho dělal o přestávkách student střední školy (a možná i to by byla urážka studentům středních škol)
- http://dotinfo.cz/ -> žádné přesměrování, odkazuje na úvodní stránku IIS ?!?!
- https://www.dotinfo.cz/ -> konečně
- https://data.mfcr.cz/cs/dataset/dotace-dotinfo -> dalo práci najít soubor, který se dá zpracovat. Člověk aby byl detektiv
- V době prvotního zpracování (začátek 2020) byl k dispozici ke stažení soubor s daty k 13. 7. 2017, dnes je tam už soubor s daty k 29. 1. 2020

- Data v souboru jsou v šíleném stavu 
  - u sloupce evidenčního čísla dotace se objevují hodnoty null
  - uprostřed souboru se namísto odělovače ";" začne používat TABulátor 
  - můj osobní favorit je tento řádek: `chyba;xxxxxxxxxx;LRS;LRS Chvaly, o.p.s.  ;24805807;NULL;Ministerstvo zdravotnictví;24341;1.00;0.00;NULL;;;`

### SZIF

- velmi málo informací k dotacím
- starší dotace nedávají k dispozici (nebýt paměti internetu, tak se k datům za roky 2014 - 2016 už nikdo nedostane)
- aktuálně jsou k dispozici na jejich webu https://www.szif.cz/irj/portal/szif/seznam-prijemcu-dotaci informace o dotacích pouze za 2017 a 2018
- zvláštní struktura xml, nikde jsem nenašel její popis
- chybí ičo 
- chybí bližší popis některých informací

### czechinvest
- chybějící informace o zrušené dotaci v české verzi
- ve sloupečku s rokem se místy objevuje celé datum


------------------
## Report ze zpracování

    Z cedr bylo nacteno 2002391 polozek.  
    Z eufondy bylo nacteno 129481 polozek.  
    Z dotinfo bylo nacteno 145426 polozek.  
    Z szif bylo nacteno 657891 polozek. Tyto zaznamy jsou unikatni (neexistuje o nich informace v CEDRu).  
    Z czechinvest bylo nacteno 1029 polozek. Tyto zaznamy jsou unikatni (neexistuje o nich informace v CEDRu).  
    Po spojeni zdroju mame celkem 2936218 polozek.  
    Celkem nalezeno 294402 potencionalnich duplicit.  
    Nepodarilo se nalezt v cedru 113330 polozek, ktere by tam meli byt.  



## poděkování:
O.Kokeš  
M.Sebera  
A.Petrák  
Dotační parazit  