--===========================================
-- Sélection finale : Compte le volume de contrats résiliés pour l'année en cours,
-- parmi ceux souscrits également cette année.
--===========================================
SELECT
    COUNT(DISTINCT radiation.num_ctr_indiv) AS vol_contrat -- Nombre de contrats individuels uniques résiliés
FROM
    tsac.ldco_contrat_operation_contrat_indiv radiation -- Table des opérations de résiliation
LEFT JOIN
    tsac.ldco_contrat_operation_contrat_indiv lcoci2 -- Jointure pour vérifier l'annulation de l'opération de résiliation
    ON radiation.num_ctr_indiv = lcoci2.num_ctr_indiv
    AND radiation.num_ope_niv_0 = lcoci2.ope_ann
    AND radiation.code_acte_gest = lcoci2.code_acte_gest
WHERE
    radiation.code_acte_gest IN ('ACRE') -- Filtre sur les opérations de type 'ACRE' (création de résiliation)
    AND radiation.ETAT_OPE = 'VA'        -- Filtre sur les opérations validées
    AND radiation.type_ope = '1'         -- Type d'opération spécifique
    AND lcoci2.ope_ann IS NULL           -- S'assure que l'opération de résiliation n'a pas été annulée
    AND radiation.DATE_CRE BETWEEN TO_DATE('20240101','yyyymmdd') AND CURRENT_DATE -- Filtre sur la date de création de l'opération de résiliation (depuis le 01/01/2024)
    AND radiation.num_ctr_indiv IN (
        --===========================================
        -- Sous-requête : Sélectionne les numéros de contrats individuels souscrits cette année.
        --===========================================
        SELECT
            num_ctr_indiv
        FROM
            tsac.ldco_contrat_operation_contrat_indiv souscription -- Table des opérations de souscription
        WHERE
            souscription.code_acte_gest IN ('ACSO','AC21') -- Filtre sur les codes d'acte de gestion de souscription
            AND souscription.ETAT_OPE = 'VA'               -- Filtre sur les opérations validées
            AND souscription.DATE_CRE BETWEEN TO_DATE('20240101','yyyymmdd') AND CURRENT_DATE -- Filtre sur la date de création de l'opération de souscription (depuis le 01/01/2024)
    );
