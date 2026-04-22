--===========================================
-- Suppression et création de la table temporaire 'accolade_raw'
-- Cette table agrège les données des agents affiliés Accolade/KPEP.
--===========================================
DROP TABLE IF EXISTS ${gp_schema}.${gp_prefix}accolade_raw${gp_suffix};

CREATE TABLE ${gp_schema}.${gp_prefix}accolade_raw${gp_suffix} AS (

    --===========================================
    -- CTE : ident_kpep
    -- Extraction des identifiants KPEP valides à partir de la table de matching KRUP.
    -- Filtre sur les systèmes source 'PERS' et les ID commençant par 'KPEP'.
    --===========================================
    WITH ident_kpep AS (
        SELECT
            id_source AS idkpep,
            compagny_code,
            target_record,
            source_system
        FROM
            TLRF.r_krup_matching_results rkmr
        WHERE 1=1
            AND current_date BETWEEN rkmr.datdebvld AND rkmr.datfinvld
            AND rkmr.source_system = 'PERS'
            AND id_source LIKE 'KPEP%'
    ),

    --===========================================
    -- CTE : ident_AI
    -- Extraction des identifiants d'Assuré Individuel (AI) valides.
    -- Filtre sur le système source 'RC-CTR' (Référentiel Contrat).
    --===========================================
    ident_AI AS (
        SELECT
            id_source AS refsrcper_ai,
            compagny_code,
            target_record,
            source_system
        FROM
            TLRF.r_krup_matching_results rkmr
        WHERE 1=1
            AND current_date BETWEEN rkmr.datdebvld AND rkmr.datfinvld
            AND rkmr.source_system = 'RC-CTR'
    ),

    --===========================================
    -- CTE : agent_affilies
    -- Consolidation des informations des assurés et de leurs contrats.
    -- Jointure des tables d'assurés, contrats individuels et collectifs avec les identifiants KPEP/AI.
    -- Filtres : Assurés principaux (ASSPRI), contrats individuels non invalidés,
    -- contrats collectifs validés ('VA') et actifs à la date du jour.
    --===========================================
    agent_affilies AS (
        SELECT DISTINCT
            ctrindiv.codofrorg,
            COALESCE(ident_kpep.idkpep, bij.idesrc2) AS idkpep, -- ID KPEP consolidé
            ass.refsrcper,
            ass.codsoc,
            ctrcllect.noctrclt,
            ass.etaas,
            ctrindiv.datsscctr,
            noctr1dv
        FROM
            tlak_ctr.lak_ctr_assure ass
            LEFT JOIN ident_AI
                ON ident_AI.refsrcper_ai = ass.refsrcper
            LEFT JOIN ident_kpep
                ON ident_AI.target_record = ident_kpep.target_record
            LEFT JOIN tlrf.r_krup_liens_pers bij
                ON ass.refsrcper = bij.idesrc1
                AND syssrc1 = 'RC-CTR'
                AND syssrc2 = 'PERS'
                AND ass.codsoc = bij.codsoc
            INNER JOIN tlak_ctr.lak_ctr_contrat_individuel AS ctrindiv
                ON ass.refunqctr = ctrindiv.refunqctr
                AND ass.idrefapl = ctrindiv.idrefapl
                AND ass.codsoc = ctrindiv.codsoc
            LEFT JOIN tlak_ctr.lak_ctr_contrat_collectif AS ctrcllect
                ON ctrindiv.noctrclt = ctrcllect.noctrclt
                AND ctrindiv.codsoc = ctrcllect.codsoc
                AND ctrindiv.idrefapl = ctrcllect.idrefapl
        WHERE 1=1
            AND CAST(current_date AS DATE) BETWEEN ass.datdebvld AND ass.datfinvld
            AND current_date BETWEEN ctrindiv.datdebvld AND ctrindiv.datfinvld
            AND ctrindiv.indinv = 'N'
            AND ass.codtypas = 'ASSPRI'
            AND ctrcllect.codsttctr = 'VA'
            AND current_date <= COALESCE(ctrcllect.datfineffpedctr, TO_DATE('99991231','yyyymmdd'))
    )

    --===========================================
    -- Sélection finale pour la table 'accolade_raw'
    -- Enrichissement avec le code KPEP validé de la personne.
    --===========================================
    SELECT
        CAST(current_date AS DATE) AS datextract,           -- Date d'extraction
        COALESCE(ekp.cdrkpep, -1) AS cdrkpep,               -- Code KPEP validé de la personne (-1 si non trouvé)
        aff.codofrorg,                                      -- Code de l'offre organisationnelle
        aff.idkpep,                                         -- Identifiant KPEP de l'agent
        aff.refsrcper,                                      -- Référence source de la personne
        aff.codsoc,                                         -- Code société
        aff.noctrclt,                                       -- Numéro du contrat collectif
        aff.etaas,                                          -- État de l'assuré
        CAST(aff.datsscctr AS TIMESTAMP) AS datsscctr,      -- Date de souscription du contrat individuel
        aff.noctr1dv,                                       -- Numéro du contrat individuel
        CASE WHEN aff.idkpep IS NULL THEN 1 ELSE 0 END AS idkpep_null -- Indicateur si l'ID KPEP est nul
    FROM
        agent_affilies aff
        LEFT JOIN tedo_cr.ed_kpep_personne ekp
            ON aff.idkpep = ekp.idkpep
            AND ekp.sttvld = 1
)
DISTRIBUTED BY (cdrkpep);

--===========================================
-- Analyse de la table pour l'optimisation des requêtes.
--===========================================
ANALYSE ${gp_schema}.${gp_prefix}accolade_raw${gp_suffix};
