--===========================================
-- Suppression et création de la table temporaire 'kpi_affilieoffice'
-- Cette table est destinée à calculer un KPI spécifique lié aux affiliés d'office.
--===========================================
DROP TABLE IF EXISTS ${gp_schema}.${gp_prefix}kpi_affilieoffice${gp_suffix};

CREATE TABLE ${gp_schema}.${gp_prefix}kpi_affilieoffice${gp_suffix} AS (

    --===========================================
    -- CTE : CONTRAT_COLLECT
    -- Sélectionne les contrats collectifs actifs et validés à la date du jour.
    --===========================================
    WITH CONTRAT_COLLECT AS (
        SELECT ctrcllect.*
        FROM tlak_ctr.lak_ctr_contrat_collectif AS ctrcllect
        WHERE 1=1
            AND ctrcllect.codsttctr = 'VA'
            AND current_date <= COALESCE(ctrcllect.datfineffpedctr,TO_DATE('99991231','yyyymmdd'))
            AND current_date BETWEEN datdebvld AND COALESCE(ctrcllect.datfinvld,TO_DATE('99991231','yyyymmdd'))
    ),

    --===========================================
    -- CTE : etablissement
    -- Extrait les établissements actifs et validés à la date du jour.
    --===========================================
    etablissement AS (
        SELECT *
        FROM tlak_per.lak_pers_etablissement et
        WHERE 1=1
            AND current_date BETWEEN datdebvld AND datfinvld
            AND et.codeta = 'VA'
    ),

    --===========================================
    -- CTE : personne_morale
    -- Extrait les personnes morales actives et de type 'RS' (Raison Sociale) à la date du jour.
    --===========================================
    personne_morale AS (
        SELECT *
        FROM tlak_per.lak_pers_personne_morale
        WHERE 1=1
            AND current_date BETWEEN datdebvld AND datfinvld
            AND TypNmPerPhy = 'RS'
    ),

    --===========================================
    -- CTE : entreprise
    -- Extrait les entreprises actives et validées ('VA') à la date du jour.
    --===========================================
    entreprise AS (
        SELECT *
        FROM tlak_per.lak_pers_entreprise etre
        WHERE 1=1
            AND current_date BETWEEN datdebvld AND datfinvld
            AND current_date BETWEEN etre.datdebeffetr AND COALESCE(etre.datfineffetr, TO_DATE('99991231','yyyymmdd'))
            AND etaetr = 'VA'
    ),

    --===========================================
    -- CTE : dim_etablissement
    -- Consolide les informations des établissements, personnes morales et entreprises.
    -- Similaire à 'sql_r_etablissement.sql', crée un référentiel d'établissements enrichi.
    --===========================================
    dim_etablissement AS (
        SELECT
            etab.cdrrefsrcper, etab.refsrcper, etab.idrefapl, refunqetb,
            persm.sglpm AS DntScaEtb, -- Raison sociale de l'établissement
            etab.siretetb, SUBSTR(etab.siretetb,1,9) AS siren,
            etab.refunqetr, -- Identifiant entreprise
            dntetr AS dntetr, -- Raison sociale de l'entreprise
            etab.codeta, etab.datdebacvetb, etab.datfinacvetb,
            etab.datdebvld, etab.datfinvld, etab.src, etab.typtrt
        FROM etablissement etab
        INNER JOIN personne_morale persm ON etab.refsrcper = persm.refsrcper AND etab.idrefapl = persm.idrefapl
        LEFT OUTER JOIN entreprise etre ON etab.refunqetr = etre.refsrcper AND etab.idrefapl = etre.idrefapl
    ),

    --===========================================
    -- CTE : societe
    -- Extrait les informations des sociétés actives à la date du jour.
    --===========================================
    societe AS (
        SELECT *
        FROM tlrf.r_orga_societe
        WHERE 1=1
            AND current_date BETWEEN datdebvld AND datfinvld
            AND current_date BETWEEN datdebeff AND COALESCE(datfineff,TO_DATE('99991231','yyyymmdd'))
    ),

    --===========================================
    -- CTE : opportunite_besoin_all
    -- Récupère toutes les opportunités/besoins, en dédoublonnant par agent, contrat, société,
    -- et type de besoin, en retenant le statut le plus récent.
    -- Exclut les avenants.
    --===========================================
    opportunite_besoin_all AS (
        SELECT * FROM (
            SELECT DISTINCT ldob.*, ctdr.noctrcoll AS noctrclt,
            DENSE_RANK() OVER (PARTITION BY refsrcper_cne, ctdr.noctrcoll, ldob.codsoc, codfambesn, ldob.idrefapl ORDER BY hrdtsttoppbesn DESC, codsttoppbesn DESC) AS numstt
            FROM tlak_rad.lak_dco_opportunite_besoin ldob
            LEFT JOIN tlak_rad.lak_dco_decouverte ctdr ON ldob.ideoppbesn = ctdr.ideoppbesn AND ctdr.codsoc = ldob.codsoc AND ldob.idrefapl = ctdr.idrefapl
            WHERE 1=1 AND avenant IS FALSE
        ) opp
    ),

    --===========================================
    -- CTE : Salarie
    -- Extrait les informations des salariés actifs à la date du jour.
    -- Calcule le prénom et le nom à partir de l'adresse e-mail.
    --===========================================
    Salarie AS (
        SELECT DISTINCT lpsa.refsrcper AS idkpep, lpsa.idesal, lpsa.matrh, lpsa.orgrh, lpsa.grppop, lpsa.codpop, lpsa.codsoc,
        INITCAP(REGEXP_REPLACE(SPLIT_PART(SPLIT_PART(mailemp, '@', 1), '.', 1), '-', ' ', 'g')) AS prenom,
        INITCAP(SPLIT_PART(SPLIT_PART(mailemp, '@', 1), '.', 2)) AS nom,
        lpsa.nirsal, lpsa.siretemp, lpsa.siren, lpsa.mailemp AS mail
        FROM tlak_per.lak_pers_salarie_aff lpsa
        WHERE current_date BETWEEN datdebvld AND datfinvld
    ),

    --===========================================
    -- CTE : Affil_salarie
    -- Extrait les statuts d'affiliation des salariés.
    --===========================================
    Affil_salarie AS (
        SELECT DISTINCT lpa.idesal, lpa.refunqctr, lpa.codsoc, lpa.sttaff, lpa.datdebsttaff, lpa.datfinsttaff
        FROM tlak_per.lak_pers_affiliation lpa
    ),

    --===========================================
    -- CTE : KPEP_personne_physique
    -- Extrait les informations des personnes physiques du KPEP.
    --===========================================
    KPEP_personne_physique AS (
        SELECT refsrcper, noinsee, COALESCE(nmusg, nmnai) AS nom, pnusu AS prenom, lppp.datnai
        FROM tlak_per.lak_pers_personne_physique lppp
        WHERE lppp.datfinvld = '9999-12-31' AND idrefapl = 'PERS'
    ),

    --===========================================
    -- CTE : Assiette_affil
    -- Construit l'assiette des agents potentiellement à affilier.
    -- Joint les statuts d'affiliation des salariés avec les informations des salariés et des personnes physiques KPEP.
    -- Filtre sur le statut 'PREAFFILIE' et dédoublonne par agent, contrat et statut.
    --===========================================
    Assiette_affil AS (
        SELECT ast.*, t.refsrcper AS idkpep_per, t.datnai FROM (
            SELECT DISTINCT afs.refunqctr AS codctrcol, afs.codsoc,
            lpsa.idkpep, lpsa.idesal, lpsa.matrh, lpsa.orgrh, lpsa.grppop, lpsa.codpop, lpsa.nirsal, sttaff, lpsa.siretemp, lpsa.siren, lpsa.nom, lpsa.prenom,
            lpsa.mail,
            ROW_NUMBER() OVER (PARTITION BY lpsa.idkpep, afs.refunqctr, sttaff ORDER BY datdebsttaff ASC) rn,
            TO_DATE(datdebsttaff,'YYYY-MM-DD') AS dat_demaff
            FROM Affil_salarie afs
            LEFT JOIN Salarie lpsa ON afs.idesal = lpsa.idesal
        ) ast
        LEFT JOIN KPEP_personne_physique t ON t.noinsee = ast.nirsal AND UPPER(t.nom) = UPPER(ast.nom) AND UPPER(t.prenom) = UPPER(ast.prenom)
        WHERE 1=1 AND rn=1 AND sttaff = 'PREAFFILIE'
    ),

    --===========================================
    -- CTE : parcours_ferme
    -- Identifie les parcours d'opportunité/besoin qui sont fermés ('OPB-STT-FER').
    -- Retient le statut le plus récent pour chaque agent, société, contrat.
    --===========================================
    parcours_ferme AS (
        SELECT DISTINCT pf.* FROM (
            SELECT DISTINCT *, CAST(hrdtsttoppbesn AS DATE) AS dat_pcrferm,
            ROW_NUMBER() OVER (PARTITION BY opb.refsrcper_cne, opb.codsoc, opb.noctrclt, opb.idrefapl ORDER BY opb.hrdtsttoppbesn DESC) AS rn
            FROM opportunite_besoin_all opb
            WHERE 1=1 AND codsttoppbesn IN ('OPB-STT-FER')
        ) AS pf
        WHERE rn = 1
    ),

    --===========================================
    -- CTE : parcours_vba
    -- Identifie les parcours d'opportunité/besoin clôturés avec le motif 'OPB-CLO-VBA'.
    -- Retient le statut le plus récent pour chaque agent, société, contrat.
    --===========================================
    parcours_vba AS (
        SELECT vb.* FROM (
            SELECT DISTINCT *, CAST(hrdtsttoppbesn AS DATE) AS dat_vba,
            ROW_NUMBER() OVER (PARTITION BY opb.refsrcper_cne, opb.codsoc, opb.noctrclt, opb.idrefapl ORDER BY opb.hrdtsttoppbesn DESC) AS rn
            FROM opportunite_besoin_all opb
            WHERE 1=1 AND codmtfcltoppbesn IN ('OPB-CLO-VBA')
        ) AS vb
        WHERE 1=1 AND rn = 1
    ),

    --===========================================
    -- CTE : parcours_initie_CT
    -- Identifie les parcours d'opportunité/besoin initiés ou en cours de devis.
    -- Filtre sur les statuts 'OPB-STT-BESAV' ou 'OPB-STT-DVTENCRS' et retient le statut le plus récent.
    --===========================================
    parcours_initie_CT AS (
        SELECT DISTINCT *, CAST(hrdtsttoppbesn AS DATE) AS dat_pcrouv FROM (
            SELECT DISTINCT opb.*,
            ROW_NUMBER() OVER (PARTITION BY opb.refsrcper_cne, opb.codsoc, opb.codfambesn, opb.noctrclt, opb.idrefapl ORDER BY opb.hrdtsttoppbesn DESC, opb.codsttoppbesn DESC) AS rn
            FROM opportunite_besoin_all opb
            WHERE 1=1 AND current_date BETWEEN opb.datdebvld AND opb.datfinvld
        ) ss
        WHERE rn=1 AND codsttoppbesn IN ('OPB-STT-BESAV','OPB-STT-DVTENCRS')
    ),

    --===========================================
    -- CTE : DISPENSES
    -- Extrait les informations des dispenses, en dédoublonnant et en classifiant leur statut.
    -- Retient la dispense la plus pertinente pour chaque agent et contrat collectif.
    --===========================================
    DISPENSES AS (
        SELECT * FROM (
            SELECT
                pop.matrh, pop.codsoc, pop.idkpep, pop.codctrcol, pop.siretemp, pop.siren, sttdisp, codsttdmedisp,
                ROW_NUMBER () OVER (PARTITION BY lcd.refsrcper, lcd.noctrclt ORDER BY lcd.codsttdmedisp DESC, lcd.datfin DESC, lcd.datdeb DESC, lcd.datvld DESC) AS num,
                CASE WHEN codsttdmedisp = 'TERMINE' AND sttdisp = 'ACCEPTE' THEN lcd.datvld ELSE NULL END AS datdispense,
                lcd.datenr AS dateEnregistrementDispense, lcd.datdeb AS datdebdispense, lcd.datfin AS datfindispense,
                CASE WHEN codsttdmedisp = 'TERMINE' AND sttdisp = 'ACCEPTE' THEN '1' ELSE '0' END AS disp_accepte,
                CASE WHEN codsttdmedisp = 'TERMINE' AND sttdisp = 'REFUSE' THEN '1' ELSE '0' END AS disp_refuse,
                CASE WHEN codsttdmedisp = 'INITIE' AND sttdisp = 'SOUMIS' THEN '1' ELSE '0' END AS disp_soumis
            FROM tlak_ctr.lak_ctr_dispense lcd
            INNER JOIN Assiette_affil pop ON lcd.refsrcper = pop.idkpep AND lcd.codctrcol = pop.codctrcol AND lcd.datfinvld = '9999-12-31'
        ) rq
        WHERE num = 1
    ),

    --===========================================
    -- CTE : ident_kpep
    -- Extraction des identifiants KPEP valides à partir de la table de matching KRUP.
    --===========================================
    ident_kpep AS (
        SELECT id_source AS idkpep, compagny_code, target_record, source_system
        FROM TLRF.r_krup_matching_results rkmr
        WHERE current_date BETWEEN rkmr.datdebvld AND rkmr.datfinvld
        AND rkmr.source_system = 'PERS' AND id_source LIKE 'KPEP%'
    ),

    --===========================================
    -- CTE : ident_AI
    -- Extraction des identifiants d'Assuré Individuel (AI) valides.
    --===========================================
    ident_AI AS (
        SELECT id_source AS refsrcper_ai, compagny_code, target_record, source_system
        FROM TLRF.r_krup_matching_results rkmr
        WHERE current_date BETWEEN rkmr.datdebvld AND rkmr.datfinvld
        AND rkmr.source_system = 'RC-CTR'
    ),

    --===========================================
    -- CTE : agent_affilies
    -- Consolidation des informations des assurés et de leurs contrats actifs et validés.
    -- Similaire à la CTE 'agent_affilies' de 'sql_accolade_raw'.
    --===========================================
    agent_affilies AS (
        SELECT DISTINCT ctrindiv.codofrorg, COALESCE(ident_kpep.idkpep, bij.idesrc2) AS idkpep, ass.refsrcper, ass.codsoc, ctrcllect.noctrclt, ass.etaas, ctrindiv.datsscctr, noctr1dv
        FROM tlak_ctr.lak_ctr_assure ass
        LEFT JOIN ident_AI ON ident_AI.refsrcper_ai = ass.refsrcper
        LEFT JOIN ident_kpep ON ident_AI.target_record = ident_kpep.target_record
        LEFT JOIN tlrf.r_krup_liens_pers bij on  ass.refsrcper=bij.idesrc1 and syssrc1='RC-CTR' and syssrc2='PERS' and ass.codsoc=bij.codsoc  --23/10/2025
    inner join tlak_ctr.lak_ctr_contrat_individuel as ctrindiv
    on ass.refunqctr=ctrindiv.refunqctr and ass.idrefapl=ctrindiv.idrefapl  and ass.codsoc=ctrindiv.codsoc
    left join tlak_ctr.lak_ctr_contrat_collectif as ctrcllect --23/10/2025
    on ctrindiv.noctrclt=ctrcllect.noctrclt and ctrindiv.codsoc =ctrcllect.codsoc and ctrindiv.idrefapl=ctrcllect.idrefapl
    where 1=1
    and cast(current_date as date) between ass.datdebvld and ass.datfinvld
    and current_date between ctrindiv.datdebvld and ctrindiv.datfinvld
    and ctrindiv.indinv='N' and ass.codtypas='ASSPRI' --10/10/2025
    --and ctrindiv.codofrorg like 'MESMEN%'
    and ctrcllect.codsttctr ='VA' --03/11/2025
    and  current_date <=COALESCE (ctrcllect.datfineffpedctr,to_date('99991231','yyyymmdd'))  --03/11/2025
    --and  current_date between ctrcllect.datdebvld and COALESCE (ctrcllect.datfinvld,to_date('99991231','yyyymmdd')) --03/11/2025
)
--select count(distinct refsrcper) from agent_affilies where codofrorg like 'MESMEN%';--idkpep in ('KPEP00000345948529','KPEP00001036014303','KPEP00001000425422');
 --select * from agent_affilies where refsrcper='1950883';
--select * from tlak_ctr.lak_ctr_assure ;
,agent_encours_affiliation as ( --29/10/2025 -- SOU 20251112
--select distinct idkpep, idesal,matrh,orgrh, dat_encaff, codsoc,codctrcol   -- SOU 20251112
-- ,sum(top_propem) as top_propem,sum(top_infcomp) as top_infcomp,sum(top_meg) as top_meg,sum(top_sign) as top_sign  -- SOU 20251112
--from (  -- SOU 20251112
select distinct i.idkpep,i.idesal, i.matrh, i.orgrh, opb.codsttoppbesn, cast(opb.hrdtsttoppbesn as date) as dat_encaff  --24/09/2025
,inict.refsrcper_cne inict --23/09/2025 
,dsp.idkpep dsp
,af.idkpep affil
,i.codsoc, i.codctrcol  --11/09/2025
,case when opb.codsttoppbesn ='OPB-STT-PROPEM' and opb.numstt=1 then 1 else 0 end as top_propem
,case when opb.codsttoppbesn ='OPB-STT-INFCOMP' and opb.numstt=1 then 1 else 0 end as top_infcomp
,case when opb.codsttoppbesn ='OPB-STT-MEG' and opb.numstt=1 then 1 else 0 end as top_meg
,case when opb.codsttoppbesn ='OPB-STT-SIGN' and opb.numstt=1 then 1 else 0 end as top_sign
from assiette_affil i
left outer join parcours_initie_CT inict on i.idkpep=inict.refsrcper_cne  and i.codsoc=inict.codsoc and i.codctrcol=inict.noctrclt--24/09/2025
left  outer join agent_affilies af on  i.idkpep=af.idkpep  and i.codsoc=af.codsoc  and i.codctrcol=af.noctrclt
left join DISPENSES dsp on i.idkpep=dsp.idkpep and i.codctrcol=dsp.codctrcol  and i.codsoc=dsp.codsoc 
left join opportunite_besoin_all opb on i.idkpep=opb.refsrcper_cne  and i.codsoc=opb.codsoc and i.codctrcol=opb.noctrclt --19/09/2025
where 1=1
and (inict.refsrcper_cne is null ) --23/09/2025 
and af.idkpep is null --- exclure les affiliés
and datdispense is null --29/09/2025 
and  opb.numstt=1 and opb.refsrcper_cne is not null 
--) req   -- SOU 20251112
--group by idkpep, idesal,matrh,orgrh, dat_encaff, codsoc,codctrcol   -- SOU 20251112
--) -- SOU 20251112
)
--select * from agent_encours_affiliation where idkpep ='KPEP00000504193604';
--select * from tlak_rad.lak_dco_opportunite_besoin ldob where refsrcper_cne ='KPEP00000504193604';
,detail_agent  as (
select distinct suivi_affil.idkpep as refsrcper
,suivi_affil.nom
,suivi_affil.prenom
,suivi_affil.mail
,suivi_affil.datnai
--,idkpep_per
,af.refsrcper as idagent_acld
,suivi_affil.orgrh
--,suivi_affil.idesal  --24/09/2025
,suivi_affil.matrh  --24/09/2025
--,suivi_affil.codctrcol as num_ctr_collectif
--,af.noctr1dv --24/09/2025
--,suivi_affil.codpop
--,suivi_affil.grppop
,suivi_affil.nirsal
--,ctrcllect.codnatctr 
--,soc.libcrt as lib_societe
--,etab.DntScaEtb as raison_sociale_etablissement
,etab.siretetb 
,etab.siren
--,etab.refunqetr as identifiant_entreprise
--,dntetr as raison_sociale_entreprise
--,adrfr.codcnt as code_centre
--,LIBRGN as region_francaise
--,dat_encaff
--,af.datsscctr
--,datdispense --29/09/2025
--,dat_pcrouv
--,dat_vba  --07/11/2025
--,dat_pcrferm --03/11/2025
--,dat_demaff as datrec
--,coalesce(case when coalesce(datdispense,cast('1900-01-01' as date)) >= coalesce(af.datsscctr,cast('1900-01-01' as date)) then datdispense else af.datsscctr end,dat_encaff,dat_pcrouv,dat_demaff) as datstt -- 29/09/2025
--,COALESCE(adrfr.codpysptl,adretr.codpysptl) AS pays
--,COALESCE(pay.LIB_PAYS,'#') AS LIB_PAYS
,1 as top_agent_a_affilier  --16/09/2025  
 --,case when inict.refsrcper_cne is null and encrs.idkpep is null and  dsp.idkpep is null and af.idkpep is null and pf.refsrcper_cne is null then '1' else '0' end as top_agent_en_attente_prcs   --06/11/2025      
 ,disp_accepte as top_agent_dispense  --29/09/2025
 --,disp_soumis as top_agent_dmd_dispense --29/09/2025
-- ,case when af.idkpep is null and pf.refsrcper_cne is null then disp_soumis else '0' end as top_agent_dmd_dispense  -- SOU 20251113
-- ,disp_refuse as top_agent_disp_rejete --29/09/2025
 --,case when inict.refsrcper_cne is not null and datdispense is null  and af.idkpep is null and pf.refsrcper_cne is null then '1' else '0' end as top_agent_parcours_ouvert     -- 03/11/2025
 --,case when encrs.idkpep is not null  and af.idkpep is null and pf.refsrcper_cne is null then '1' else '0' end as top_encours_affil --03/11/2025
 ,case when af.refsrcper is not null and af.datsscctr > coalesce(datdispense,cast('1900-01-01' as date)) and etaas ='VA' then '1' else '0' end as top_aff   -- 29/09/2025
 --,case when af.refsrcper is not null and etaas ='VA' then '1' else '0' end as topagtaffbrut   -- SOU 20251112
,case when af.refsrcper is not null and etaas ='RA' then '1' else '0' end as top_rad   -- 24/09/2025
 ,case when af.refsrcper is not null and etaas ='VA' and disp_accepte='1' then '1' else '0' end as top_aff_dsp  -- SOU 20251112
 --,case when encrs.idkpep is not null  and af.idkpep is null and pf.refsrcper_cne is null then encrs.top_propem end as top_propem  --03/11/2025
 --,case when encrs.idkpep is not null  and af.idkpep is null and pf.refsrcper_cne is null then encrs.top_infcomp  end as top_infcomp  --03/11/2025
 --,case when encrs.idkpep is not null  and af.idkpep is null and pf.refsrcper_cne is null then encrs.top_meg end as top_meg  --03/11/2025
 --,case when encrs.idkpep is not null  and af.idkpep is null and pf.refsrcper_cne is null then encrs.top_sign end as top_sign --03/11/2025
 ,case when vba.refsrcper_cne is not null then '1' else '0' end as top_vba --07/11/2025
,case when pf.refsrcper_cne is not null then '1' else '0' end as top_pcf
 ,ctrcllect.codofrorg
from  assiette_affil suivi_affil
left outer join CONTRAT_COLLECT ctrcllect on suivi_affil.codctrcol =ctrcllect.noctrclt and suivi_affil.codsoc=ctrcllect.codsoc 
inner join parcours_vba vba on suivi_affil.idkpep=vba.refsrcper_cne  and suivi_affil.codsoc=vba.codsoc and suivi_affil.codctrcol=vba.noctrclt  --07/11/2025
left outer join parcours_initie_CT inict on suivi_affil.idkpep=inict.refsrcper_cne  and suivi_affil.codsoc=inict.codsoc and suivi_affil.codctrcol=inict.noctrclt --18/09/2025
left join parcours_ferme pf on suivi_affil.idkpep=pf.refsrcper_cne  and suivi_affil.codsoc=pf.codsoc and suivi_affil.codctrcol=pf.noctrclt --18/09/2025
left  outer join agent_affilies af on  (suivi_affil.idkpep=af.idkpep or suivi_affil.idkpep_per=af.idkpep)  and suivi_affil.codsoc=af.codsoc and suivi_affil.codctrcol=af.noctrclt --07/11/2025
--left  outer join agent_encours_affiliation encrs on  suivi_affil.idkpep=encrs.idkpep  and suivi_affil.codsoc=encrs.codsoc and suivi_affil.codctrcol=encrs.codctrcol --18/09/2025
left join DISPENSES dsp on suivi_affil.idkpep=dsp.idkpep and suivi_affil.codctrcol=dsp.codctrcol  and suivi_affil.codsoc=dsp.codsoc 
--left outer join societe as soc on suivi_affil.codsoc =soc.codsoc
left outer join dim_etablissement etab on suivi_affil.siren=etab.siren  and suivi_affil.siretemp =etab.siretetb 
--LEFT JOIN ADRESSE_POSTALE_FRANCAISE adrfr  ON suivi_affil.idkpep=adrfr.num_personne and suivi_affil.codsoc=adrfr.codsoc and adrfr.idrefapl='PERS' --11/09/2025
--LEFT JOIN ADRESSE_POSTALE_ETRANGERE adretr ON suivi_affil.idkpep=adretr.num_personne and suivi_affil.codsoc=adretr.codsoc and adretr.idrefapl='PERS' --11/09/2025
--LEFT JOIN dim_region reg on adrfr.codcnt=reg.codcnt 
--LEFT JOIN R_TRAN_PAYS pay on pay.COD_PAYS=COALESCE(adrfr.codpysptl,adretr.codpysptl)
where 1=1
--and suivi_affil.idkpep in ('KPEP00000001728826','KPEP00001038965632','KPEP00001036447105','KPEP00001034006804','KPEP00000740007814','KPEP00000471341406')
)
, resultat AS (
SELECT
    dt.codofrorg,
    dt.refsrcper,
    dt.siren,
    dt.siretetb as siret,
    NULLIF(dt.orgrh, '') AS orgrh,
    upper(coalesce(lppp.nmusg, lppp.nmnai)) as nom, 
    upper(lppp.pnusu) as prenom, 
    lppp.datnai as datnai,
    dt.mail as email, 
    dt.matrh as matrh
from
    detail_agent dt 
    left join tlak_per.lak_pers_personne_physique lppp  
        on lppp.refsrcper= dt.refsrcper
where 1=1
    and lppp.datfinvld = '9999-12-31' and idrefapl = 'PERS'
    and dt.top_pcf='1' and dt.top_vba='1' and dt.top_aff ='1'
    --and top_agent_dispense    <>'1'
    --and top_aff_dsp <>'1'
    --and   top_rad <>'1'
    and codofrorg like '%MESMEN%'
)
SELECT
    codofrorg,
    siret,
    count(DISTINCT refsrcper) AS nbagt
FROM
    resultat r
GROUP BY 1, 2
) DISTRIBUTED BY (siret);

ANALYSE ${gp_schema}.${gp_prefix}kpi_affilieoffice${gp_suffix};
