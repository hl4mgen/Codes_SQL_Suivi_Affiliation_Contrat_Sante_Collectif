--===========================================
-- Requête principale (version 1 - utilisant des tables TSAC)
-- Cette requête semble être une tentative ou une version précédente,
-- car elle est suivie d'une autre requête utilisant des tables TLAK.
-- Elle vise à compter les assurés principaux par offre et société.
--===========================================
SELECT T1.offre, T2.nb_personne AS nombre
FROM
(
    --===========================================
    -- Sous-requête T1 : Compte les assurés principaux actifs à la date du jour.
    -- Utilise les tables TSAC (tsac.ldco_contrat_contrat_individuel et tsac.ldco_contrat_assure).
    --===========================================
    SELECT DISTINCT
        CI.offre,
        ASSUP.CODE_SOC_APPART AS CODSOC,
        ASSUP.champ_4 AS TYPE_ASSURE,
        COUNT(*) AS Nb_Personne
    FROM
    (
        -- Sélectionne les contrats individuels (CI) les plus récents.
        SELECT * FROM (
            SELECT CI.*, ROW_NUMBER() OVER (PARTITION BY CI.NUM_CTR_INDIV, CI.SITE ORDER BY CI.NUM_OPE_NIV_1 DESC) NUMLIGNE
            FROM tsac.ldco_contrat_contrat_individuel CI
            WHERE CI.IND_INVALID = 'N'
        )T1
        WHERE NUMLIGNE = 1
    )CI
    INNER JOIN
    (
        -- Sélectionne les assurés principaux (ASSUP) les plus récents.
        SELECT DISTINCT ASS.num_ctr_indiv, ASS.rang, ASS.num_personne, ASS.type_assure, ASS.code_etat, ASS.date_prem_adhesion, ASS.date_dern_radiation, ASS.CODE_SOC_APPART, RTP.champ_4
        FROM
        (
            SELECT *
            FROM (
                SELECT ASR.* , ROW_NUMBER() OVER (PARTITION BY ASR.NUM_CTR_INDIV, ASR.RANG, ASR.code_soc_appart, ASR.SITE ORDER BY ASR.date_effet_ope DESC, ASR.NUM_OPE_NIV_1 DESC, ASR.NUM_OPE_NIV_2 DESC, ASR.DATAPP DESC) NUMLIGNE
                FROM tsac.ldco_contrat_assure ASR
                WHERE ASR.IND_INVALID = 'N'
            )B
            WHERE NUMLIGNE = 1
        )ASS
        INNER JOIN
        (
            -- Référence pour les types d'assurés.
            SELECT * FROM tlrf.r_ldco_param_flux_reference rlpfr
            WHERE 1=1
            AND code_table_aneto = 'U0057'
            AND sttvld = 1
        )RTP
        ON ASS.type_assure = RTP.champ_1
        WHERE 1=1
        AND RTP.champ_4 = 'ASSURE' -- Filtre pour ne retenir que les assurés principaux
    )ASSUP
    ON CI.num_ctr_indiv = ASSUP.num_ctr_indiv
    WHERE 1=1
    -- Conditions pour un contrat "ouvert" à la date du jour (non résilié et non radié)
    AND CURRENT_DATE < NVL(CI.date_der_resiliation,TO_DATE('2099-12-31','YYYY-MM-DD'))
    AND CURRENT_DATE < NVL(ASSUP.date_dern_radiation,TO_DATE('2099-12-31','YYYY-MM-DD'))
    GROUP BY CI.offre, ASSUP.CODE_SOC_APPART, ASSUP.champ_4
)T1
LEFT OUTER JOIN
(
    --===========================================
    -- Sous-requête T2 : Semble être une duplication ou une variation de T1.
    -- Les conditions de filtrage sur les dates sont différentes,
    -- visant à inclure les contrats dont la période de validité inclut la date du jour.
    --===========================================
    SELECT
        CI.offre,
        ASSUP.CODE_SOC_APPART AS CODSOC,
        ASSUP.champ_4 AS TYPE_ASSURE,
        COUNT(*) AS Nb_Personne
    FROM
    (
        -- Sélectionne les contrats individuels (CI) les plus récents.
        SELECT * FROM (
            SELECT CI.*, ROW_NUMBER() OVER (PARTITION BY CI.NUM_CTR_INDIV, CI.SITE ORDER BY CI.NUM_OPE_NIV_1 DESC) NUMLIGNE
            FROM tsac.ldco_contrat_contrat_individuel CI
            WHERE CI.IND_INVALID = 'N'
        )T1
        WHERE NUMLIGNE = 1
    )CI
    INNER JOIN
    (
        -- Sélectionne les assurés principaux (ASSUP) les plus récents.
        SELECT DISTINCT ASS.num_ctr_indiv, ASS.rang, ASS.num_personne, ASS.type_assure, ASS.code_etat, ASS.date_prem_adhesion, ASS.date_dern_radiation, ASS.CODE_SOC_APPART, RTP.champ_4
        FROM
        (
            SELECT *
            FROM (
                SELECT ASR.* , ROW_NUMBER() OVER (PARTITION BY ASR.NUM_CTR_INDIV, ASR.RANG, ASR.code_soc_appart, ASR.SITE ORDER BY ASR.date_effet_ope DESC, ASR.NUM_OPE_NIV_1 DESC, ASR.NUM_OPE_NIV_2 DESC, ASR.DATAPP DESC) NUMLIGNE
                FROM tsac.ldco_contrat_assure ASR
                WHERE ASR.IND_INVALID = 'N'
            )B
            WHERE NUMLIGNE = 1
        )ASS
        INNER JOIN
        (
            -- Référence pour les types d'assurés.
            SELECT * FROM tlrf.r_ldco_param_flux_reference rlpfr
            WHERE 1=1
            AND code_table_aneto = 'U0057'
            AND sttvld = 1
        )RTP
        ON ASS.type_assure = RTP.champ_1
        WHERE 1=1
    )ASSUP
    ON CI.num_ctr_indiv = ASSUP.num_ctr_indiv
    WHERE 1=1
    -- Conditions pour un contrat "ouvert" à la date du jour (période de validité incluant CURRENT_DATE)
    AND CURRENT_DATE BETWEEN CI.date_prem_souscript AND NVL(CI.date_der_resiliation,TO_DATE('2099-12-31','YYYY-MM-DD'))
    AND CURRENT_DATE BETWEEN ASSUP.date_prem_adhesion AND NVL(ASSUP.date_dern_radiation,TO_DATE('2099-12-31','YYYY-MM-DD'))
    GROUP BY CI.offre, ASSUP.CODE_SOC_APPART, ASSUP.champ_4
)T2
ON T1.offre = T2.offre
AND T1.codsoc = T2.codsoc
AND T1.type_assure = T2.type_assure
ORDER BY T1.offre, T1.codsoc, T1.type_assure;

--===========================================
-- Requête alternative (version 2 - utilisant des tables TLAK)
-- Cette requête semble être la version la plus pertinente et à jour,
-- utilisant les tables TLAK pour compter les assurés principaux par offre.
--===========================================
WITH ass AS (
    -- Sélectionne les assurés avec des contrats ouverts (non radiés ou radiés après la date du jour).
    -- Filtre sur les assurés principaux (ASSPRI) ou ceux avec un rôle spécifique (rgas='1').
    SELECT *
    FROM tlak_ctr.lak_ctr_assure lca
    WHERE 1 = 1
    AND (
        (dateffderrad > current_date AND dateffprrah < current_date AND dateffderrad - dateffprrah > 1) -- Contrat actif avec date de fin future
        OR (dateffderrad IS NULL AND dateffprrah < current_date) -- Contrat actif sans date de fin
    )
    AND current_date BETWEEN datdebvld AND datfinvld
    AND (codtypas = 'ASSPRI' OR rgas = '1')
),
CTR AS (
    -- Sélectionne les contrats individuels ouverts (non terminés ou terminés après la date du jour).
    -- Filtre sur les contrats individuels actifs et liés à Accolade (idrefapl = 'RC-CTR').
    SELECT noctr1dv AS numctr, codofrorg AS offre, CTRIND.*
    FROM tlak_ctr.lak_ctr_contrat_individuel CTRIND
    WHERE 1=1
    AND (
        (datfinctr > current_date AND datdebeffpedctr < current_date AND datfinctr - datdebeffpedctr > 1) -- Contrat actif avec date de fin future
        OR (datfinctr IS NULL AND datdebeffpedctr < current_date) -- Contrat actif sans date de fin
    )
    AND current_date BETWEEN datdebvld AND datfinvld
    AND idrefapl = 'RC-CTR' -- Accolade
)
-- Sélection finale : Compte le nombre distinct d'assurés principaux par offre.
SELECT COUNT(1), offre
FROM
(
    SELECT DISTINCT ass.refsrcper, ass.dateffderrad, CTR.numctr, CTR.offre
    FROM ass
    INNER JOIN CTR
        ON CTR.numctr = ass.refunqctr
        AND CTR.codsoc = ass.codsoc
        AND CTR.idrefapl = ass.idrefapl
) req1
GROUP BY offre
ORDER BY offre;
