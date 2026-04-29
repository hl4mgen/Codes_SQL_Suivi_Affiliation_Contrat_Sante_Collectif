--===========================================
-- Sélection finale : Compte le nombre de contrats actifs pour lesquels
-- l'assuré n'a pas de KPEP rapproché dans le RUP.
--===========================================
WITH ass AS (
    --===========================================
    -- CTE : ass
    -- Sélectionne les assurés (rang=1) actifs à la date du jour.
    -- Utilise ROW_NUMBER pour s'assurer de ne prendre que la dernière version de l'assuré.
    --===========================================
    SELECT * FROM (
        SELECT
            NUM_CTR_INDIV,
            NUM_PERSONNE,
            TYPE_ASSURE,
            date_prem_adhesion,
            date_dern_radiation,
            ROW_NUMBER() OVER (PARTITION BY site,NUM_CTR_INDIV,rang ORDER BY NUM_OPE_NIV_2 DESC,NUM_OPE_NIV_1 ) AS RN
        FROM tsac.ldco_contrat_assure
        WHERE rang=1
    ) t
    WHERE RN=1 AND current_date <= NVL(date_dern_radiation,TO_DATE('20991231','yyyymmdd'))
),
contrats AS (
    --===========================================
    -- CTE : contrats
    -- Sélectionne les contrats individuels actifs et valides.
    -- Jointure avec 'tlrf.r_orga_transco_code_societe_accolade' pour récupérer le code société.
    -- Utilise ROW_NUMBER pour s'assurer de ne prendre que la dernière version du contrat.
    --===========================================
    SELECT * FROM (
        SELECT
            num_ctr_indiv,
            offre,
            rotcsa.codsoc,
            ROW_NUMBER() OVER (PARTITION BY site,NUM_CTR_INDIV ORDER BY NUM_OPE_NIV_1 DESC) AS RN
        FROM
            tsac.ldco_contrat_contrat_individuel lcci
        LEFT JOIN tlrf.r_orga_transco_code_societe_accolade rotcsa ON (
            TRIM(rotcsa.codentrtm) = TRIM(lcci.entite_rattachement)
            AND TRIM(rotcsa.codcntges) = TRIM(lcci.centre_gestion)
            AND rotcsa.datfinvld = '99991231'
        )
        WHERE date_invalid_dwh IS NULL
    ) t WHERE RN=1
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
-- Sélection finale : Compte les contrats actifs pour lesquels
-- l'assuré n'a pas de KPEP rapproché dans le RUP.
--===========================================
SELECT COUNT(*) AS volume
FROM (
    SELECT
        contrats.num_ctr_indiv,
        contrats.offre,
        ass.num_personne
    FROM
        ass
    INNER JOIN contrats ON (ass.num_ctr_indiv = contrats.num_ctr_indiv)
    LEFT JOIN rup ON (ass.num_personne || '' = rup.num_personne) -- Jointure avec le RUP pour trouver le KPEP
    WHERE rup.num_personne IS NULL -- Condition clé : le num_personne n'a pas de KPEP rapproché dans le RUP
) t;
