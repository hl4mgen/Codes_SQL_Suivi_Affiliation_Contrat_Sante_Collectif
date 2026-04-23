--===========================================
-- Suppression et création de la table temporaire 'div_accolade_sans_kpep'
-- Cette table identifie les agents affiliés dans Accolade mais sans ID KPEP associé.
--===========================================
DROP TABLE IF EXISTS ${gp_schema}.${gp_prefix}div_accolade_sans_kpep${gp_suffix};

CREATE TABLE ${gp_schema}.${gp_prefix}div_accolade_sans_kpep${gp_suffix} AS (
    SELECT
        raw.datextract,                                     -- Date d'extraction des données
        raw.codofrorg,                                      -- Code de l'offre organisationnelle du contrat
        raw.idkpep,                                         -- Identifiant KPEP (sera NULL ici, car c'est la condition de filtrage)
        raw.refsrcper,                                      -- Référence source de la personne (ID de l'assuré)
        raw.noctrclt,                                       -- Numéro du contrat collectif
        raw.etaas,                                          -- État de l'assuré
        CAST(raw.datsscctr AS DATE) AS datsscctr,           -- Date de souscription du contrat individuel
        raw.noctr1dv,                                       -- Numéro du contrat individuel
        COALESCE(pph.nmusg, pph.nmnai) AS nom,              -- Nom de l'agent (d'usage ou de naissance)
        pph.pnusu AS prenom,                                -- Prénom de l'agent
        pph.datnai                                          -- Date de naissance de l'agent
    FROM
       ${gp_schema}.${gp_prefix}accolade_raw${gp_suffix} raw -- Utilise la table brute des affiliés Accolade
    LEFT JOIN tlak_per.lak_pers_personne_physique pph       -- Jointure avec les informations des personnes physiques
        ON  raw.refsrcper = pph.refsrcper
        AND current_date BETWEEN pph.datdebvld AND pph.datfinvld -- Filtre sur la validité de la personne physique
    WHERE
        raw.idkpep_null = 1                                 -- Condition clé : sélectionne les enregistrements où l'ID KPEP est manquant
        AND raw.codofrorg LIKE 'MESMEN%'                    -- Filtre sur les offres organisationnelles 'MESMEN%'
    ORDER BY
        nom ASC, prenom ASC
)
DISTRIBUTED RANDOMLY; -- Stratégie de distribution aléatoire pour la table

--===========================================
-- Analyse de la table pour l'optimisation des requêtes.
--===========================================
ANALYSE ${gp_schema}.${gp_prefix}div_accolade_sans_kpep${gp_suffix};
