--===========================================
-- Requête principale : Compte les personnes uniques par offre, société et type d'assuré.
--===========================================
SELECT
    CI.offre,
    ASSUP.CODE_SOC_APPART AS CODSOC,
    ASSUP.champ_4 AS TYPE_ASSURE,
    COUNT(DISTINCT ASSUP.num_personne) AS Nb_Personne
FROM
(
    --===========================================
    -- Sous-requête CI : Sélectionne les contrats individuels (CI) les plus récents.
    -- Filtre sur les contrats non invalidés (IND_INVALID = 'N') et retient la dernière version
    -- de chaque contrat individuel en utilisant ROW_NUMBER() sur NUM_OPE_NIV_1.
    --===========================================
    SELECT * FROM (
        SELECT CI.* , ROW_NUMBER() OVER (PARTITION BY CI.NUM_CTR_INDIV, CI.SITE ORDER BY CI.NUM_OPE_NIV_1 DESC) NUMLIGNE
        FROM tsac.ldco_contrat_contrat_individuel CI
        WHERE CI.IND_INVALID = 'N'
    )T1
    WHERE NUMLIGNE = 1
)CI
INNER JOIN
(
    --===========================================
    -- Sous-requête ASSUP : Sélectionne les assurés (ASS) les plus récents et leurs types.
    -- Filtre sur les assurés non invalidés (IND_INVALID = 'N') et retient la dernière version
    -- de chaque assuré en utilisant ROW_NUMBER() sur NUM_OPE_NIV_1 et NUM_OPE_NIV_2.
    -- Jointure avec le référentiel des types d'assurés (tlrf.r_ldco_param_flux_reference)
    -- pour enrichir avec le champ_4 (qui peut indiquer 'ASSURE' ou autre).
    --===========================================
    SELECT ASS.num_ctr_indiv, ASS.rang, ASS.num_personne, ASS.type_assure, ASS.code_etat, ASS.date_prem_adhesion, ASS.date_dern_radiation, ASS.CODE_SOC_APPART, RTP.champ_4
    FROM
    (
        SELECT *
        FROM (
            SELECT ASR.* , ROW_NUMBER() OVER (PARTITION BY ASR.NUM_CTR_INDIV, ASR.RANG, ASR.SITE ORDER BY ASR.NUM_OPE_NIV_1 DESC, ASR.NUM_OPE_NIV_2 DESC) NUMLIGNE
            FROM tsac.ldco_contrat_assure ASR
            WHERE ASR.IND_INVALID = 'N'
        )B
        WHERE NUMLIGNE = 1
    )ASS
    INNER JOIN
    (
        SELECT * FROM tlrf.r_ldco_param_flux_reference rlpfr
        WHERE 1=1
        AND code_table_aneto = 'U0057'
        AND sttvld = 1
    )RTP
    ON ASS.type_assure = RTP.champ_1
    WHERE 1=1
    -- La condition 'and RTP.champ_4 = 'ASSURE'' est commentée, donc tous les types d'assurés sont inclus.
)ASSUP
ON CI.num_ctr_indiv = ASSUP.num_ctr_indiv
WHERE 1=1
--===========================================
-- Conditions de filtrage pour les contrats et adhésions actifs à la date du jour.
-- Un contrat est considéré actif si la date du jour est entre sa date de première souscription
-- et sa date de dernière résiliation (ou une date future par défaut).
-- Une adhésion est considérée active si la date du jour est entre sa date de première adhésion
-- et sa date de dernière radiation (ou une date future par défaut).
--===========================================
AND CURRENT_DATE BETWEEN CI.date_prem_souscript AND NVL(CI.date_der_resiliation,TO_DATE('20991231','YYYYMMDD'))
AND CURRENT_DATE BETWEEN ASSUP.date_prem_adhesion AND NVL(ASSUP.date_dern_radiation,TO_DATE('20991231','YYYYMMDD'))
-- La condition 'and CI.offre in (...)' est commentée, donc toutes les offres sont incluses.
GROUP BY CI.offre, ASSUP.CODE_SOC_APPART, ASSUP.champ_4
ORDER BY CI.offre, ASSUP.CODE_SOC_APPART, ASSUP.champ_4;
