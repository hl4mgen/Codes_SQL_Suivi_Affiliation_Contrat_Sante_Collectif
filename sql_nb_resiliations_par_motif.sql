--===========================================
-- Sélection finale : Compte les résiliations par code société et motif.
--===========================================
SELECT code_soc_appart, motif_resiliation, COUNT(1)
FROM (
    --===========================================
    -- Sous-requête req2 : Sélectionne les contrats individuels résiliés uniques.
    --===========================================
    SELECT DISTINCT code_soc_appart, num_ctr_indiv, motif_resiliation
    FROM (
        --===========================================
        -- Sous-requête req1 : Identifie les opérations de création de contrat (ACRE) qui sont des résiliations.
        -- Jointure avec 'tsac.ldco_contrat_operation_contrat_indiv' pour les opérations et 'tsac.ldco_contrat_contrat_individuel' pour les motifs.
        -- Filtre sur les opérations de type 'ACRE' (création), validées ('VA'), de type '1', et non annulées.
        -- La date de création de l'opération est limitée à l'année en cours.
        --===========================================
        SELECT
            souscription.code_soc_appart,
            souscription.num_ctr_indiv,
            motif_resiliation,
            ci.datapp AS datapp,
            MAX(ci.datapp) OVER (PARTITION BY ci.num_ctr_indiv) AS maxdatapp
        FROM
            tsac.ldco_contrat_operation_contrat_indiv souscription
        LEFT JOIN tsac.ldco_contrat_operation_contrat_indiv lcoci2
            ON souscription.num_ctr_indiv = lcoci2.num_ctr_indiv
            AND souscription.num_ope_niv_0 = lcoci2.ope_ann
            AND souscription.code_acte_gest = lcoci2.code_acte_gest
        LEFT JOIN tsac.ldco_contrat_contrat_individuel ci
            ON ci.num_ctr_indiv = souscription.num_ctr_indiv
            AND ci.motif_resiliation != '@'
            AND ci.ind_invalid = 'N'
        WHERE souscription.code_acte_gest IN ('ACRE') -- Opération de création de contrat
            AND souscription.ETAT_OPE = 'VA'           -- Opération validée
            AND souscription.type_ope = '1'            -- Type d'opération spécifique
            AND lcoci2.ope_ann IS NULL                 -- Opération non annulée
            AND souscription.DATE_CRE BETWEEN TO_DATE(DATE_PART('year', CURRENT_DATE)||'0101','yyyymmdd') AND CURRENT_DATE -- Créé cette année
    ) req1
    WHERE datapp = maxdatapp OR datapp IS NULL -- Retient la dernière version du contrat individuel
) req2
GROUP BY code_soc_appart, motif_resiliation
ORDER BY motif_resiliation;
