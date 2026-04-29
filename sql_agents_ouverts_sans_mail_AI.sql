--===========================================
-- Sélection finale : Compte le nombre d'assurés sans adresse e-mail.
--===========================================
WITH ass AS (
    --===========================================
    -- CTE : ass
    -- Sélectionne les assurés (rang=1) actifs à la date du jour.
    -- Utilise ROW_NUMBER pour s'assurer de ne prendre que la dernière version de l'assuré.
    --===========================================
    SELECT * FROM (
        SELECT
            NUM_CTR_INDIV,
            NUM_PERSONNE,
            TYPE_ASSURE,
            date_prem_adhesion,
            date_dern_radiation,
            ROW_NUMBER() OVER (PARTITION BY site,NUM_CTR_INDIV,rang ORDER BY NUM_OPE_NIV_2 DESC,NUM_OPE_NIV_1 ) AS RN
        FROM tsac.ldco_contrat_assure
        WHERE rang=1
    ) t
    WHERE RN=1 AND current_date <= NVL(date_dern_radiation,TO_DATE('20991231','yyyymmdd'))
),
coord AS (
    --===========================================
    -- CTE : coord
    -- Sélectionne la dernière coordonnée de communication de type 'MAIL' pour chaque personne.
    -- Utilise ROW_NUMBER pour s'assurer de ne prendre que la dernière version de la coordonnée.
    --===========================================
    SELECT * FROM (
        SELECT
            cc.num_personne,
            cc.VALEUR_COORDONNEE,
            ROW_NUMBER() OVER (PARTITION BY site,num_personne ORDER BY NUM_EVENEMENT DESC) AS RN
        FROM tsac.ldco_personne_coord_communication cc
        WHERE NATURE_COORDONNEE = 'MAIL'
    )t
    WHERE RN=1
)
--===========================================
-- Sélection finale : Compte les assurés actifs qui n'ont pas de coordonnée e-mail.
--===========================================
SELECT
    COUNT(DISTINCT ass.num_personne)
FROM
    ass
LEFT JOIN coord ON (ass.num_personne = coord.num_personne)
WHERE coord.num_personne IS NULL; -- Condition clé : l'assuré n'a pas de coordonnée e-mail trouvée
