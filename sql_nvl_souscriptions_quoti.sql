--===========================================
-- Sélection finale : Compte le volume de contrats correspondant à de nouvelles souscriptions.
--===========================================
SELECT
    COUNT(DISTINCT souscription.num_ctr_indiv) AS vol_contrat -- Nombre de contrats individuels uniques
FROM
    tsac.ldco_contrat_operation_contrat_indiv souscription -- Table des opérations sur les contrats individuels
WHERE
    souscription.code_acte_gest IN ('ACSO','AC21') -- Filtre sur les codes d'acte de gestion de souscription ('ACSO', 'AC21')
    AND souscription.ETAT_OPE = 'VA'               -- Filtre sur les opérations validées
    AND souscription.DATE_CRE BETWEEN TO_DATE('20240101','yyyymmdd') AND CURRENT_DATE; -- Filtre sur la date de création de l'opération (depuis le 01/01/2024)
