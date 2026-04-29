--===========================================
-- Sélection finale : Compte le nombre de personnes ayant plusieurs KPEP et des contrats souscrits cette année.
--===========================================
WITH ass AS (
    --===========================================
    -- CTE : ass
    -- Sélectionne les assurés principaux (rgas=1) actifs et valides dans Accolade (RC-CTR).
    -- Filtre sur les contrats non radiés (dateffderrad future ou nulle).
    --===========================================
    SELECT
        refunqctr,
        refsrcper,
        codtypas,
        dateffprrah,
        dateffderrad
    FROM tlak_ctr.lak_ctr_assure lca
    WHERE lca.rgas = 1
      AND lca.etaas = 'VA'
      AND lca.datfinvld = '9999-12-31'
      AND (current_date <= dateffderrad OR dateffderrad IS NULL)
      AND idrefapl = 'RC-CTR'
),
ens_ctr AS (
    --===========================================
    -- CTE : ens_ctr
    -- Sélectionne les numéros de contrats individuels qui ont fait l'objet d'une souscription
    -- validée depuis le 1er janvier 2024.
    --===========================================
    SELECT
        souscription.num_ctr_indiv
    FROM
        tsac.ldco_contrat_operation_contrat_indiv souscription
    WHERE souscription.code_acte_gest IN ('ACSO','AC21')
      AND souscription.ETAT_OPE = 'VA'
      AND TO_CHAR(souscription.DATE_CRE,'yyyymmdd') >= '20240101'
),
contrats AS (
    --===========================================
    -- CTE : contrats
    -- Sélectionne les contrats individuels actifs et validés dans Accolade.
    --===========================================
    SELECT
        noctr1dv,
        codofrorg,
        codsoc
    FROM tlak_ctr.lak_ctr_contrat_individuel lcci
    WHERE lcci.codsttctr = 'VA'
      AND lcci.datfinvld = '9999-12-31'
      AND idrefapl = 'RC-CTR'
),
rup AS (
    --===========================================
    -- CTE : rup
    -- Récupère le matching entre les identifiants Accolade (num_personne) et KPEP (KPEP).
    -- Filtre sur les matchings valides à long terme et les systèmes 'RC-CTR' et 'PERS'.
    --===========================================
    SELECT
        r_acc.id_source AS num_personne,
        r_kpep.id_source AS KPEP,
        r_kpep.compagny_code
    FROM tlrf.r_krup_matching_results r_acc
    INNER JOIN tlrf.r_krup_matching_results r_kpep ON (
        r_acc.target_record = r_kpep.target_record
        AND r_kpep.source_system = 'PERS'
        AND r_kpep.id_source LIKE 'KPEP%'
        AND r_kpep.datfinvld = '99991231'
    )
    WHERE
        r_acc.datfinvld = '99991231'
        AND r_acc.source_system = 'RC-CTR'
)
--===========================================
-- Sélection finale : Compte les personnes ayant plusieurs KPEP pour des contrats souscrits depuis 2024.
--===========================================
SELECT DISTINCT COUNT(DISTINCT refsrcper) AS volume
FROM (
    SELECT DISTINCT
        contrats.noctr1dv,
        contrats.codofrorg,
        ass.refsrcper,
        rup.KPEP,
        dateffprrah,
        COUNT(1) OVER (PARTITION BY refsrcper, noctr1dv) AS RN -- Compte le nombre de KPEP distincts par personne et contrat
    FROM
        ass
    INNER JOIN ens_ctr ON (ass.refunqctr = ens_ctr.num_ctr_indiv) -- Jointure avec les contrats souscrits depuis 2024
    INNER JOIN contrats ON (ass.refunqctr = contrats.noctr1dv)
    INNER JOIN rup ON (ass.refsrcper || '' = rup.num_personne AND TRIM(rup.compagny_code) = TRIM(contrats.codsoc)) -- Jointure avec le RUP
) t
WHERE RN >= 2; -- Filtre pour ne retenir que les personnes ayant au moins 2 KPEP pour le même contrat
