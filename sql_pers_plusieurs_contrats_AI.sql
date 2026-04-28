--===========================================
-- CTE : pers
-- Extrait les informations des personnes physiques (nom, prénom, NIR, date de naissance)
-- depuis le référentiel des personnes physiques (lak_pers_personne_physique).
-- Filtre sur les personnes actives à la date du jour et liées au référentiel contrat (RC-CTR).
--===========================================
WITH pers AS (
    SELECT
        refsrcper AS num_personne,
        cvl AS CIVILITE,
        sex AS sexe,
        nmusg AS NOM_LONG,
        nmnai AS NOM_JEUNE_FILLE,
        pnusu AS prenom,
        datnai AS DATE_NAISSANCE,
        NOINSEE AS CODE_INSEE,
        clenoinsee AS CLE_INSEE
    FROM tlak_per.lak_pers_personne_physique lppp
    WHERE 1=1
        AND idrefapl = 'RC-CTR'
        AND current_date BETWEEN datdebvld AND datfinvld
),

--===========================================
-- CTE : ens_dbl
-- Compte le nombre d'identifiants uniques (num_personne) pour chaque combinaison
-- de nom, prénom, NIR et date de naissance.
-- Cette CTE est une étape intermédiaire pour identifier les doublons potentiels
-- basés sur les informations d'identité.
--===========================================
ens_dbl AS (
    SELECT
        nom_long,
        prenom,
        code_insee,
        cle_insee,
        DATE_NAISSANCE,
        COUNT(DISTINCT num_personne)
    FROM pers
    GROUP BY 1,2,3,4,5
),

--===========================================
-- CTE : dbl_personnes
-- Sélectionne les informations des personnes physiques qui pourraient avoir des doublons
-- ou des identifiants multiples basés sur les critères d'identité (nom, prénom, NIR, date de naissance).
--===========================================
dbl_personnes AS (
    SELECT
        ens_dbl.nom_long,
        ens_dbl.prenom,
        ens_dbl.code_insee,
        ens_dbl.cle_insee,
        ens_dbl.DATE_NAISSANCE,
        pers.num_personne
    FROM ens_dbl
    INNER JOIN pers ON (
        UPPER(ens_dbl.nom_long) = UPPER(pers.nom_long) AND
        UPPER(ens_dbl.prenom) = UPPER(pers.prenom) AND
        ens_dbl.code_insee = pers.code_insee AND
        ens_dbl.CLE_INSEE = pers.CLE_INSEE AND
        ens_dbl.DATE_NAISSANCE = pers.DATE_NAISSANCE
    )
),

--===========================================
-- CTE : ass
-- Identifie les assurés principaux (ASSPRI) avec des contrats actifs et sans date de fin.
-- Deux blocs UNION ALL :
-- 1. Contrats actifs à la date du jour.
-- 2. Contrats futurs (date de début dans le futur) sans date de fin.
-- Filtre sur les assurés principaux ('ASSPRI') et les contrats validés ('VA').
--===========================================
ass AS (
    SELECT
        refunqctr AS NUM_CTR_INDIV,
        refsrcper AS NUM_PERSONNE,
        codtypas AS TYPE_ASSURE,
        dateffprrah AS date_prem_adhesion,
        dateffderrad AS date_dern_radiation,
        datdebvld,
        codsoc
    FROM tlak_ctr.lak_ctr_assure lca
    WHERE 1 = 1
        AND current_date BETWEEN datdebvld AND datfinvld
        AND codtypas = 'ASSPRI'
        AND current_date BETWEEN dateffprrah AND NVL(dateffderrad,TO_DATE('29991231','yyyymmdd')) -- Contrats actifs à date
        AND etaas = 'VA'
    UNION ALL
    SELECT
        refunqctr AS NUM_CTR_INDIV,
        refsrcper AS NUM_PERSONNE,
        codtypas AS TYPE_ASSURE,
        dateffprrah AS date_prem_adhesion,
        dateffderrad AS date_dern_radiation,
        datdebvld,
        codsoc
    FROM tlak_ctr.lak_ctr_assure lca
    WHERE 1 = 1
        AND current_date BETWEEN datdebvld AND datfinvld
        AND codtypas = 'ASSPRI'
        AND dateffderrad IS NULL -- Contrats sans date de fin
        AND current_date < dateffprrah -- Et dont la date de début est dans le futur
        AND etaas = 'VA'
)

--===========================================
-- Sélection finale
-- Joint les assurés principaux identifiés (ass) avec les personnes potentiellement doublons (dbl_personnes).
-- Regroupe par identité et code société, puis filtre pour ne retenir que les personnes
-- ayant plus d'un contrat individuel actif (count(distinct ass.NUM_CTR_INDIV) > 1).
--===========================================
SELECT
    dbl_personnes.nom_long,
    dbl_personnes.prenom,
    dbl_personnes.code_insee,
    dbl_personnes.cle_insee,
    dbl_personnes.DATE_NAISSANCE,
    codsoc,
    COUNT(DISTINCT ass.NUM_CTR_INDIV) AS nbr_contrats_ouverts
FROM ass
INNER JOIN dbl_personnes ON (dbl_personnes.num_personne = ass.NUM_PERSONNE)
GROUP BY 1,2,3,4,5,6
HAVING COUNT(DISTINCT ass.NUM_CTR_INDIV) > 1;
