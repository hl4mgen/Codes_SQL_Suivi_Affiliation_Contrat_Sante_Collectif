--===========================================
-- Suppression et création de la table temporaire 'dblstt_affdisp'
-- Cette table identifie les agents qui sont à la fois affiliés et dispensés.
--===========================================
DROP TABLE IF EXISTS ${gp_schema}.${gp_prefix}dblstt_affdisp${gp_suffix};

CREATE TABLE ${gp_schema}.${gp_prefix}dblstt_affdisp${gp_suffix} AS (

    --===========================================
    -- CTE : REFASSURE
    -- Récupère les paramètres de référence pour les assurés (code_table_aneto = 'U0057', champ_4='ASSURE').
    -- Utilisé pour valider le type d'assuré dans la CTE ACCOLADE.
    --===========================================
    WITH REFASSURE AS (
        SELECT *
        FROM tlrf.r_ldco_param_flux_reference
        WHERE code_table_aneto = 'U0057'
          AND current_date BETWEEN datdebvld AND datfinvld
          AND champ_4 = 'ASSURE'
    ),

    --===========================================
    -- CTE : DISPENSE
    -- Sélectionne les informations des dispenses terminées et acceptées.
    -- Filtre sur les dispenses dont la validité est '9999-12-31' (valide indéfiniment ou à long terme)
    -- et dont le statut est 'TERMINE' et 'ACCEPTE'.
    --===========================================
    DISPENSE AS (
        SELECT refsrcper, noctrclt, datenr AS dateEnrDisp, datdeb AS datedebdisp, datfin AS datefindisp, datvld AS dateValidationDisp
        FROM tlak_ctr.lak_ctr_dispense lcd
        WHERE datfinvld = '9999-12-31'
          AND codsttdmedisp = 'TERMINE'
          AND sttdisp = 'ACCEPTE'
    ),

    --===========================================
    -- CTE : AFFIL
    -- Identifie les agents qui ont une entrée dans les données d'affiliation (tsam.st_lafm_donneesaffiliation)
    -- et qui correspondent à une dispense enregistrée.
    -- Jointure sur 'idkpep' et 'noctracld' avec la CTE DISPENSE.
    --===========================================
    AFFIL AS (
        SELECT DISTINCT idkpep, noctracld, dataff, disp.dateEnrDisp, disp.datedebdisp, disp.datefindisp, disp.dateValidationDisp
        FROM tsam.st_lafm_donneesaffiliation sld
        INNER JOIN DISPENSE disp ON disp.refsrcper = sld.idkpep AND disp.noctrclt = sld.noctracld
    ),

    --===========================================
    -- CTE : ACCOLADE
    -- Extrait les informations des contrats individuels actifs et validés dans Accolade.
    -- Jointure des contrats collectifs, individuels et assurés, avec matching KRUP
    -- pour lier les identifiants d'assuré (rupIdAI) aux identifiants KPEP (rupIdKpep).
    -- Filtre sur les contrats collectifs et individuels actifs à la date du jour,
    -- et sur les assurés dont le type correspond à la référence REFASSURE.
    --===========================================
    ACCOLADE AS (
        SELECT DISTINCT lca.refsrcper AS rupIdAI, rklp2.id_source AS rupIdKpep, lcci.datsscctr, lcci.noctr1dv, lcci.datfinctr, lcci.codofrorg, lcci.noctrclt
        FROM tlak_ctr.lak_ctr_contrat_collectif lccc
        INNER JOIN tlak_ctr.lak_ctr_contrat_individuel lcci ON lcci.noctrclt = lccc.noctrclt AND lcci.datdebvld <= current_date AND lcci.datfinvld > current_date
        INNER JOIN tlak_ctr.lak_ctr_assure lca ON lcci.refunqctr = lca.refunqctr AND lca.datdebvld <= current_date AND lca.datfinvld > current_date
        INNER JOIN tlrf.r_krup_matching_results rklp1 ON rklp1.id_source = lca.refsrcper AND rklp1.sttvld = 1
        INNER JOIN tlrf.r_krup_matching_results rklp2 ON rklp1.target_record = rklp2.target_record AND rklp2.sttvld = 1 AND rklp2.source_system='PERS'
        INNER JOIN REFASSURE rf ON lca.codtypas = rf.champ_1
        WHERE
            1=1
            AND lccc.datdebvld <= current_date AND lccc.datfinvld > current_date
            AND lccc.idrefapl = 'RC-CTR' AND lcci.codsttctr = 'VA'
    )

    --===========================================
    -- Sélection principale pour la table 'dblstt_affdisp'
    -- Joint les agents identifiés comme "affiliés" (AFFIL) avec ceux "actifs dans Accolade" (ACCOLADE).
    -- Le filtre final 'current_date < datefindisp' s'assure que la dispense est toujours active.
    --===========================================
    SELECT DISTINCT
        COALESCE(ekp.cdrkpep, -1) AS cdrkpep,               -- Code KPEP de la personne, -1 si non trouvé
        codofrorg,                                          -- Code de l'offre organisationnelle du contrat
        aff.idkpep,                                         -- Identifiant KPEP de l'agent
        aff.noctracld,                                      -- Numéro de contrat collectif lié à l'affiliation
        aff.dataff::date AS dataff,                         -- Date d'affiliation
        aff.dateenrdisp,                                    -- Date d'enregistrement de la dispense
        aff.datedebdisp::date AS datedebdisp,               -- Date de début de la dispense
        aff.datefindisp::date AS datefindisp,               -- Date de fin de la dispense
        rupIdAI AS numpersaccolade,                         -- Identifiant de la personne dans Accolade
        acc.noctr1dv,                                       -- Numéro du contrat individuel
        acc.datsscctr::date AS datsscctr,                   -- Date de souscription du contrat individuel
        datfinctr::date AS datfinctr,                       -- Date de fin du contrat individuel
        dateValidationDisp                                  -- Date de validation de la dispense
    FROM
        AFFIL aff
        INNER JOIN ACCOLADE acc
            ON acc.rupIdKpep = aff.idkpep AND aff.noctracld = acc.noctrclt
        LEFT JOIN tedo_cr.ed_kpep_personne ekp
            ON aff.idkpep = ekp.idkpep
            AND ekp.sttvld = 1
    WHERE 1=1
        AND current_date < datefindisp -- La dispense est toujours active à la date du jour
)
DISTRIBUTED BY (cdrkpep);

--===========================================
-- Analyse de la table pour l'optimisation des requêtes.
--===========================================
ANALYSE ${gp_schema}.${gp_prefix}dblstt_affdisp${gp_suffix};
