--===========================================
-- Sélection finale : Compte le volume de contrats résiliés pour l'année en cours.
--===========================================
SELECT DISTINCT
    COUNT(DISTINCT souscription.num_ctr_indiv) AS vol_contrat -- Nombre de contrats individuels uniques
FROM
    tsac.ldco_contrat_operation_contrat_indiv souscription -- Table des opérations sur les contrats individuels
LEFT JOIN
    tsac.ldco_contrat_operation_contrat_indiv lcoci2 -- Jointure pour vérifier l'annulation d'opérations
    ON souscription.num_ctr_indiv = lcoci2.num_ctr_indiv
    AND souscription.num_ope_niv_0 = lcoci2.ope_ann
    AND souscription.code_acte_gest = lcoci2.code_acte_gest
WHERE
    souscription.code_acte_gest IN ('ACRE') -- Filtre sur les opérations de type 'ACRE' (création de résiliation)
    AND souscription.ETAT_OPE = 'VA'        -- Filtre sur les opérations validées
    AND souscription.type_ope = '1'         -- Type d'opération spécifique
    AND lcoci2.ope_ann IS NULL              -- S'assure que l'opération n'a pas été annulée
    AND souscription.DATE_CRE BETWEEN TO_DATE('20240101','yyyymmdd') AND CURRENT_DATE; -- Filtre sur la date de création de l'opération (depuis le 01/01/2024)
