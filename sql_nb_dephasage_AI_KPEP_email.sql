--==============================================================================
-- 06/2024 : Comptage du nbre de KPEP en déphasages avec ACCOLADE sur les Email
--==============================================================================

--===========================================
-- CTE : EmailsKPEP
-- Objectif : Récupérer l'adresse e-mail la plus pertinente des personnes KPEP.
--===========================================
WITH EmailsKPEP AS (
    --===========================================
    -- Sous-CTE : KPEP
    -- Extrait les identifiants KPEP et les 'num_acc' (ID Accolade/AI) associés via le RUP.
    -- Filtre sur les KPEP actifs et valides à la date du jour.
    --===========================================
    WITH KPEP AS (
        SELECT DISTINCT
            KPEP.idkpep,
            UPPER(COALESCE(KPEP.nmusg, KPEP.nmptr)) AS Nom_kpep,
            UPPER(KPEP.pnusu) AS Prenom_kpep,
            KPEP.datnai AS DatNai_kpep,
            rup.id_source AS num_acc
        FROM tedo_cr.ed_kpep_personnephysique KPEP
        INNER JOIN tlrf.r_krup_matching_results rup2 ON (
            rup2.id_source = KPEP.idkpep
            AND rup2.source_system = 'PERS'
            AND CURRENT_DATE BETWEEN rup2.datdebvld AND rup2.datfinvld
        )
        INNER JOIN tlrf.r_krup_matching_results rup ON (
            rup.target_record = rup2.target_record
            AND rup.source_system = 'RC-CTR'
            AND CURRENT_DATE BETWEEN rup.datdebvld AND rup.datfinvld
        )
        WHERE 1=1
        AND CURRENT_DATE BETWEEN KPEP.DATDEBVLD AND KPEP.DATFINVLD
        AND LEFT(KPEP.idkpep,4) = 'KPEP'
    ),
    --===========================================
    -- Sous-CTE : MailKPEP
    -- Extrait les adresses e-mail KPEP de type personnel ou professionnel.
    -- Priorise la dernière adresse e-mail effective pour chaque KPEP.
    --===========================================
    MailKPEP AS (
        SELECT DISTINCT
            idkpep,
            UPPER(TRIM(adrmsg)) AS Mail_kpep,
            ROW_NUMBER() OVER (PARTITION BY idkpep ORDER BY datdebadreml DESC, datdebvld DESC, datcso DESC) AS Rang
        FROM tedo_cr.ed_kpep_adresseelectronique
        WHERE 1 = 1
        AND typem IN ('PE','PR')
        AND CURRENT_DATE BETWEEN datdebvld AND datfinvld
        AND CURRENT_DATE BETWEEN datdebadreml AND NVL(datfineffeml,'99991231')
    )
    -- Sélection finale de EmailsKPEP : Joint les infos KPEP et leur e-mail prioritaire.
    SELECT
        KPEP.*,
        MailKPEP.Mail_kpep
    FROM KPEP
    INNER JOIN MailKPEP ON MailKPEP.idkpep = KPEP.idkpep -- Ne retient que les personnes KPEP ayant un e-mail
    WHERE MailKPEP.Rang = 1 -- Assure de prendre le mail le plus pertinent si Rang est utilisé
),

--===========================================
-- CTE : EmailsAI
-- Objectif : Récupérer l'adresse e-mail principale des Assurés Individuels (AI) depuis TSAC.
--===========================================
EmailsAI AS (
    SELECT * FROM (
        SELECT DISTINCT
            TO_CHAR(num_personne) AS num_acc,
            UPPER(TRIM(valeur_coordonnee)) AS mail_ai,
            ROW_NUMBER() OVER (PARTITION BY num_personne ORDER BY num_evenement DESC, date_deb_effet DESC) AS Rang
        FROM tsac.ldco_personne_coord_communication
        WHERE 1=1
        AND nature_coordonnee = 'MAIL'
        AND type_coordonnee = 'MPCP' -- Mail principal
        AND CURRENT_DATE BETWEEN date_deb_effet AND NVL(date_fin_effet,'99991231')
    ) A WHERE A.Rang = 1
)

--===========================================
-- Sélection finale : Compte les ID AI dont l'e-mail ne correspond pas à l'e-mail KPEP.
--===========================================
SELECT COUNT(DISTINCT(ACC.num_acc))
FROM EmailsKPEP KPEP
INNER JOIN EmailsAI ACC ON ACC.num_acc = KPEP.num_acc -- Jointure sur l'ID Accolade/AI
INNER JOIN (
    --===========================================
    -- Sous-requête : Identifie les ID AI dont l'e-mail ne correspond PAS à l'e-mail KPEP.
    -- Utilise une clause NOT EXISTS pour trouver les déphasages.
    --===========================================
    SELECT B.num_acc
    FROM EmailsAI B
    WHERE NOT EXISTS (
        SELECT 1 FROM EmailsKPEP A
        WHERE A.num_acc = B.num_acc
        AND B.mail_ai = A.Mail_kpep -- Comparaison directe des adresses e-mail
    )
) AI2 ON AI2.num_acc = KPEP.num_acc;
