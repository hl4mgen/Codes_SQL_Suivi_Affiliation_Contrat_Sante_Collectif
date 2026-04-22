--===========================================
-- Suppression et création de la table temporaire 'glot_raw'
-- Cette table agrège les données brutes des agents lotis (GLOT).
--===========================================
DROP TABLE IF EXISTS ${gp_schema}.${gp_prefix}glot_raw${gp_suffix};

CREATE TABLE ${gp_schema}.${gp_prefix}glot_raw${gp_suffix} AS (

    --===========================================
    -- CTE : glot_ranked
    -- Extraction des données de lotissement d'affiliation depuis 'tsam.st_baselotismnt_lotissement_affiliation'.
    -- Exclut les statuts 'EN_DOUBLON' et 'SUPPRIME'.
    -- Un 'row_number' est appliqué pour dédoublonner les entrées par 'idkpep' et 'numero_contrat',
    -- en retenant le dernier enregistrement ('datapp' DESC) pour chaque combinaison.
    --===========================================
    WITH glot_ranked AS (
        SELECT
            sbla.*,
            ROW_NUMBER() OVER (PARTITION BY sbla.idkpep, sbla.numero_contrat ORDER BY sbla.datapp DESC) rn_glot
        FROM
            tsam.st_baselotismnt_lotissement_affiliation sbla
        WHERE 1=1
            AND sbla.statut != 'EN_DOUBLON'
            AND sbla.statut != 'SUPPRIME'
    )

    --===========================================
    -- Sélection finale pour la table 'glot_raw'
    -- Joint les données de lotissement filtrées avec les informations KPEP.
    -- Le filtre 'rn_glot = 1' assure que seuls les enregistrements les plus récents sont retenus.
    --===========================================
    SELECT
        COALESCE(ekp.cdrkpep, -1) AS cdrkpep,               -- Code KPEP de la personne, -1 si non trouvé
        glo.idkpep AS idkpep,                               -- Identifiant KPEP de l'agent
        glo.statut AS stt,                                  -- Statut du lotissement
        CAST(glo.date_creation AS TIMESTAMP) AS datcrea,    -- Date de création du lotissement
        CAST(glo.date_attente_lotissement AS TIMESTAMP) AS datattlot, -- Date d'attente de lotissement
        glo.date_lotissement AS datlot,                     -- Date de lotissement
        CAST(glo.date_publication AS TIMESTAMP) AS datpub,  -- Date de publication
        glo.nature_contrat AS natctr,                       -- Nature du contrat
        glo.numero_contrat AS noctr,                        -- Numéro du contrat
        CAST(glo.datgestaffil AS TIMESTAMP) AS datgestaffil,-- Date de gestion de l'affiliation
        SUBSTR(glo.codofr, 8) AS codofr                      -- Code offre (partie après le 7ème caractère)
    FROM
        glot_ranked glo
        LEFT JOIN tedo_cr.ed_kpep_personne ekp
            ON glo.idkpep = ekp.idkpep
            AND ekp.sttvld = 1
    WHERE
        rn_glot = 1 -- Retient uniquement l'enregistrement le plus récent après dédoublonnage
)
DISTRIBUTED BY (cdrkpep);

--===========================================
-- Analyse de la table pour l'optimisation des requêtes.
--===========================================
ANALYSE ${gp_schema}.${gp_prefix}glot_raw${gp_suffix};
