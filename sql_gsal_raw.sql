--===========================================
-- Suppression et création de la table temporaire 'gsal_raw'
-- Cette table agrège les données brutes des agents à affilier provenant du flux GSAL (Salariés).
--===========================================
DROP TABLE IF EXISTS ${gp_schema}.${gp_prefix}gsal_raw${gp_suffix};

CREATE TABLE ${gp_schema}.${gp_prefix}gsal_raw${gp_suffix} AS (

    --===========================================
    -- CTE : sal
    -- Extraction des informations des salariés depuis 'tlak_per.lak_pers_salarie_aff'.
    -- Jointure avec 'ed_kpep_personne' pour récupérer le 'cdrkpep' validé.
    -- Un 'row_number' est appliqué pour dédoublonner les entrées par 'refsrcper' et 'siretemp',
    -- en retenant la situation la plus récente ('datdebdetsal' DESC).
    --===========================================
    WITH sal AS (
        SELECT
            COALESCE(ekp.cdrkpep, -1) AS cdrkpep,
            idesal,
            refsrcper,
            datdebsal,
            datfinsal,
            nirsal,
            mailemp,
            siretemp,
            matrh,
            orgrh,
            datdebdetsal,
            datfindetsal,
            ROW_NUMBER() OVER (PARTITION BY refsrcper, siretemp ORDER BY datdebdetsal DESC) rn
        FROM
            tlak_per.lak_pers_salarie_aff aff
            LEFT JOIN tedo_cr.ed_kpep_personne ekp
                ON aff.refsrcper = ekp.idkpep
                AND ekp.sttvld = 1
    ),

    --===========================================
    -- CTE : der_sit_sal
    -- Sélectionne la dernière situation salariale pour chaque salarié,
    -- en utilisant le 'rn=1' calculé dans la CTE 'sal'.
    --===========================================
    der_sit_sal AS (
        SELECT
            cdrkpep,
            idesal,
            refsrcper,
            datdebsal,
            datfinsal,
            nirsal,
            mailemp,
            siretemp,
            matrh,
            orgrh,
            datdebdetsal,
            datfindetsal
        FROM
            sal
        WHERE
            sal.rn = 1
    ),

    --===========================================
    -- CTE : stt_sall
    -- Extraction des statuts d'affiliation des salariés depuis 'tlak_per.lak_pers_affiliation'.
    -- Filtre sur les statuts valides à la date du jour.
    -- Un 'row_number' est appliqué pour dédoublonner les entrées par 'idesal',
    -- en retenant le statut le plus récent ('datdebsttaff' DESC).
    --===========================================
    stt_sall AS (
        SELECT
            idesal,
            refunqctr,
            codsoc,
            iderefapl,
            sttaff,
            datdebsttaff,
            datfinsttaff,
            ROW_NUMBER() OVER(PARTITION BY idesal ORDER BY datdebsttaff DESC) rn
        FROM
            tlak_per.lak_pers_affiliation
        WHERE
            current_date BETWEEN datdebvld AND datfinvld
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
    ),

    --===========================================
    -- CTE : der_stt_sall
    -- Sélectionne le dernier statut d'affiliation pour chaque salarié et l'enrichit
    -- avec le code offre organisationnelle du contrat collectif associé.
    -- Utilise le 'rn=1' de la CTE 'stt_sall'.
    --===========================================
    der_stt_sall AS (
        SELECT
            stt_sall.idesal,
            stt_sall.refunqctr,
            stt_sall.codsoc,
            stt_sall.iderefapl,
            stt_sall.sttaff,
            stt_sall.datdebsttaff,
            stt_sall.datfinsttaff,
            ctrcoll.codofrorg
        FROM
            stt_sall
            LEFT JOIN contratscollectifs ctrcoll
                ON stt_sall.refunqctr = ctrcoll.refunqctr
                AND stt_sall.codsoc = ctrcoll.codsoc
        WHERE 1=1
            AND stt_sall.rn = 1
    )

    --===========================================
    -- Sélection finale pour la table 'gsal_raw'
    -- Joint la dernière situation salariale avec le dernier statut d'affiliation.
    -- Consolide toutes les informations pertinentes pour les agents GSAL.
    --===========================================
    SELECT
        der_sit_sal.cdrkpep,                                -- Code KPEP de la personne
        der_sit_sal.idesal,                                 -- Identifiant du salarié
        der_sit_sal.refsrcper AS idkpep,                    -- Identifiant KPEP de l'agent
        der_sit_sal.datdebsal,                              -- Date de début de salariat
        der_sit_sal.datfinsal,                              -- Date de fin de salariat
        der_sit_sal.nirsal,                                 -- Numéro d'inscription au répertoire (NIR) du salarié
        der_sit_sal.mailemp,                                -- E-mail de l'employé
        der_sit_sal.siretemp AS siret,                      -- Numéro SIRET de l'employeur
        der_sit_sal.matrh,                                  -- Matricule RH
        COALESCE(NULLIF(der_sit_sal.orgrh, ''), 'Z#') AS orgrh, -- Organisation RH (remplace vide par 'Z#')
        der_sit_sal.datdebdetsal,                           -- Date de début de la situation salariale
        der_sit_sal.datfindetsal,                           -- Date de fin de la situation salariale
        der_stt_sall.refunqctr,                             -- Référence unique du contrat
        der_stt_sall.codsoc,                                -- Code société
        der_stt_sall.iderefapl,                             -- Identifiant de référence de l'application
        der_stt_sall.sttaff,                                -- Statut d'affiliation
        der_stt_sall.datdebsttaff,                          -- Date de début du statut d'affiliation
        der_stt_sall.datfinsttaff,                          -- Date de fin du statut d'affiliation
        der_stt_sall.codofrorg                              -- Code de l'offre organisationnelle du contrat collectif
    FROM
        der_sit_sal
        INNER JOIN der_stt_sall
            ON der_sit_sal.idesal = der_stt_sall.idesal
)
DISTRIBUTED BY (cdrkpep);

--===========================================
-- Analyse de la table pour l'optimisation des requêtes.
--===========================================
ANALYSE ${gp_schema}.${gp_prefix}gsal_raw${gp_suffix};
