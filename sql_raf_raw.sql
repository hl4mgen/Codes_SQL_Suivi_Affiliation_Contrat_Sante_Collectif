--===========================================
-- Suppression et création de la table temporaire 'raf_raw'
-- Cette table agrège les données brutes des agents à affilier provenant du flux RAF.
--===========================================
DROP TABLE IF EXISTS ${gp_schema}.${gp_prefix}raf_raw${gp_suffix};

CREATE TABLE ${gp_schema}.${gp_prefix}raf_raw${gp_suffix} AS (

    --===========================================
    -- CTE : affiliations
    -- Extraction et préparation des données brutes d'affiliation depuis la source 'st_lafm_donneesaffiliation'.
    -- Calcule des champs dérivés comme 'ao_fichier', 'stt_preaff', et des dates extraites du nom de fichier.
    -- Un 'row_number' est appliqué pour dédoublonner les entrées par 'idkpep' et 'siret',
    -- en privilégiant les statuts 'OK' pour la pré-affiliation.
    -- Les doublons explicites et les entrées sans 'idkpep' sont exclus.
    --===========================================
    WITH affiliations AS (
        SELECT
            SUBSTRING(nmfic,1,2) AS ao_fichier,
            CASE
                WHEN stt LIKE '%OK%' THEN 'OK'
                ELSE 'KO'
            END AS stt_preaff,
            idkpep,
            noctracld,
            siren,
            siret,
            matsal,
            noinsee,
            nmusg,
            nmnai,
            pn,
            datnai,
            mail,
            stt,
            CAST(hrdt AS DATE) AS dat_hrdt_lafm,
            SUBSTRING(nmfic FROM LENGTH(nmfic) - 17 FOR 8) AS dat_dde_aff,
            SUBSTRING(nmfic FROM LENGTH(nmfic) - 17 FOR 14) AS hrdt_dde_aff,
            codsoc,
            orga_rh,
            SUBSTRING(nmfic,1,17) AS grpemp_PAF,
            ROW_NUMBER() OVER ( PARTITION BY idkpep, siret ORDER BY CASE WHEN stt LIKE '%OK%' THEN 'OK' ELSE 'KO' END DESC) AS rn_lafm
        FROM
            tsam.st_lafm_donneesaffiliation
        WHERE
            idkpep IS NOT NULL
            AND (UPPER(mtfrejet) IS NULL OR UPPER(mtfrejet) NOT LIKE '%DOUBLON%')
    ),

    --===========================================
    -- CTE : contratscollectifs
    -- Sélectionne les informations des contrats collectifs actifs et validés.
    -- Filtre les contrats dont la période de validité et la date de fin effective
    -- incluent la date du jour, et dont le statut est 'VA' (Validé).
    --===========================================
    contratscollectifs AS (
        SELECT
            ctr.codnatctr,
            ctr.cdrrefunqctr,
            ctr.refunqctr,
            ctr.codsoc,
            ctr.noctrcltpnr,
            ctr.dateffprrssc,
            ctr.datfineffpedctr,
            ctr.codsttctr,
            ctr.codofrorg
        FROM
            tlak_ctr.lak_ctr_contrat_collectif AS ctr
        WHERE 1=1
            AND current_date BETWEEN datdebvld AND datfinvld
            AND ctr.codsttctr = 'VA'
            AND current_date <= COALESCE(ctr.datfineffpedctr,TO_DATE('99991231','yyyymmdd'))
    )

    --===========================================
    -- Sélection finale pour la table 'raf_raw'
    -- Joint les données d'affiliation avec les contrats collectifs et les informations KPEP.
    -- Le filtre 'aff.rn_lafm = 1' assure que seuls les enregistrements uniques et prioritaires sont retenus.
    --===========================================
    SELECT
        COALESCE(ekp.cdrkpep, -1) AS cdrkpep,               -- Code KPEP de la personne, -1 si non trouvé
        ctrcoll.codofrorg,                                  -- Code de l'offre organisationnelle du contrat collectif
        ctrcoll.noctrcltpnr,                                -- Numéro du contrat collectif partenaire
        aff.ao_fichier,                                     -- Type de fichier d'affiliation
        aff.stt_preaff,                                     -- Statut de pré-affiliation ('OK' ou 'KO')
        aff.idkpep,                                         -- Identifiant KPEP de l'agent
        aff.noctracld,                                      -- Numéro de contrat collectif lié à l'affiliation
        aff.siren,                                          -- Numéro SIREN de l'entreprise
        aff.siret,                                          -- Numéro SIRET de l'établissement
        aff.matsal,                                         -- Matricule salarié
        aff.noinsee,                                        -- Numéro INSEE
        aff.nmusg,                                          -- Nom d'usage
        aff.nmnai,                                          -- Nom de naissance
        aff.pn,                                             -- Prénom
        aff.datnai,                                         -- Date de naissance
        aff.mail,                                           -- Adresse e-mail
        aff.stt,                                            -- Statut brut de l'affiliation
        aff.dat_hrdt_lafm,                                  -- Date d'horodatage du fichier LAFM
        aff.dat_dde_aff,                                    -- Date de demande d'affiliation (extraite du nom de fichier)
        aff.hrdt_dde_aff,                                   -- Horodatage de la demande d'affiliation (extrait du nom de fichier)
        aff.codsoc,                                         -- Code société
        COALESCE(NULLIF(aff.orga_rh, ''), 'Z#') AS orga_rh, -- Organisation RH (remplace vide par 'Z#')
        aff.grpemp_paf                                      -- Groupe d'employés PAF (extrait du nom de fichier)
    FROM
        affiliations aff
        LEFT JOIN contratscollectifs ctrcoll
            ON aff.noctracld = ctrcoll.refunqctr
        LEFT JOIN tedo_cr.ed_kpep_personne ekp
            ON aff.idkpep = ekp.idkpep
            AND ekp.sttvld = 1
    WHERE
        aff.rn_lafm = 1 -- Retient uniquement l'enregistrement prioritaire après dédoublonnage
)
DISTRIBUTED BY (cdrkpep);

--===========================================
-- Analyse de la table pour l'optimisation des requêtes.
--===========================================
ANALYSE ${gp_schema}.${gp_prefix}raf_raw${gp_suffix};
