--===========================================
-- Sélection finale : Compte le nombre de personnes ayant plusieurs contrats AI actifs.
--===========================================
SELECT COUNT(1) FROM (
    --===========================================
    -- Sous-requête req1 : Identifie les personnes ayant plus d'un contrat AI actif.
    --===========================================
    SELECT
        dbl_personnes.nom_long,
        dbl_personnes.prenom,
        dbl_personnes.code_insee,
        dbl_personnes.cle_insee,
        dbl_personnes.DATE_NAISSANCE,
        COUNT(DISTINCT ass.NUM_CTR_INDIV)
    FROM
        --===========================================
        -- CTE : ass
        -- Identifie les assurés principaux (ASSPRI) avec des contrats actifs et sans date de fin.
        -- Deux blocs UNION ALL :
        -- 1. Contrats actifs à la date du jour.
        -- 2. Contrats futurs (date de début dans le futur) sans date de fin.
        -- Filtre sur les assurés principaux ('ASSPRI') et les contrats validés ('VA').
        --===========================================
        (
            SELECT
                refunqctr AS NUM_CTR_INDIV,
                refsrcper AS NUM_PERSONNE,
                codtypas AS TYPE_ASSURE,
                dateffprrah AS date_prem_adhesion,
                dateffderrad AS date_dern_radiation,
                datdebvld
            FROM tlak_ctr.lak_ctr_assure lca
            WHERE 1 = 1
                AND current_date BETWEEN datdebvld AND datfinvld
                AND codtypas = 'ASSPRI'
                AND current_date BETWEEN dateffprrah AND NVL(dateffderrad,TO_DATE('29991231','yyyymmdd'))
                AND etaas = 'VA'
            UNION ALL
            SELECT
                refunqctr AS NUM_CTR_INDIV,
                refsrcper AS NUM_PERSONNE,
                codtypas AS TYPE_ASSURE,
                dateffprrah AS date_prem_adhesion,
                dateffderrad AS date_dern_radiation,
                datdebvld
            FROM tlak_ctr.lak_ctr_assure lca
            WHERE 1 = 1
                AND current_date BETWEEN datdebvld AND datfinvld
                AND codtypas = 'ASSPRI'
                AND dateffderrad IS NULL
                AND current_date < dateffprrah
                AND etaas = 'VA'
        ) ass
    INNER JOIN
        --===========================================
        -- CTE : dbl_personnes
        -- Sélectionne les informations des personnes physiques qui pourraient avoir des doublons
        -- ou des identifiants multiples basés sur les critères d'identité.
        --===========================================
        (
            SELECT
                ens_dbl.nom_long,
                ens_dbl.prenom,
                ens_dbl.code_insee,
                ens_dbl.cle_insee,
                ens_dbl.DATE_NAISSANCE,
                pers.num_personne
            FROM
                --===========================================
                -- CTE : ens_dbl
                -- Compte le nombre d'identifiants uniques pour chaque combinaison
                -- de nom, prénom, NIR et date de naissance.
                --===========================================
                (
                    SELECT
                        nom_long,
                        prenom,
                        code_insee,
                        cle_insee,
                        DATE_NAISSANCE,
                        COUNT(DISTINCT num_personne)
                    FROM
                        --===========================================
                        -- CTE : pers
                        -- Extrait les informations des personnes physiques depuis
                        -- 'lak_pers_personne_physique', filtrées sur 'RC-CTR' et validité.
                        --===========================================
                        (
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
                        ) pers
                    GROUP BY 1,2,3,4,5
                ) ens_dbl
            INNER JOIN
                (
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
                ) pers ON (
                    UPPER(ens_dbl.nom_long) = UPPER(pers.nom_long) AND
                    UPPER(ens_dbl.prenom) = UPPER(pers.prenom) AND
                    ens_dbl.code_insee = pers.code_insee AND
                    ens_dbl.CLE_INSEE = pers.CLE_INSEE AND
                    ens_dbl.DATE_NAISSANCE = pers.DATE_NAISSANCE
                )
        ) dbl_personnes ON (dbl_personnes.num_personne = ass.NUM_PERSONNE)
    GROUP BY 1,2,3,4,5
    HAVING COUNT(DISTINCT ass.NUM_CTR_INDIV) > 1
) req1;
