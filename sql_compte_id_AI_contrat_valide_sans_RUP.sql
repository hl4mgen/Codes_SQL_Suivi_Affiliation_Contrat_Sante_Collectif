--===========================================
-- Requête principale : Compte le nombre de personnes uniques.
--===========================================
SELECT COUNT(1) FROM (

    --===========================================
    -- Sous-requête interne : Sélectionne les personnes avec des contrats actifs.
    --===========================================
    SELECT DISTINCT(num_personne) FROM (
        SELECT
            prs.num_personne,
            MIN(prs.datchg) AS min_chargement_TSAC,
            ctr.type_assure,
            lpa.libelle_acheminement,
            prs.nom_long,
            prs.prenom,
            prs.date_naissance,
            lcci.etat_contrat,
            ctr.code_etat
        FROM
            tsac.ldco_personne_personne_physique prs -- Table des personnes physiques
        INNER JOIN
            tsac.ldco_contrat_assure ctr -- Table des assurés
            ON prs.num_personne = ctr.num_personne
        LEFT JOIN (
            -- Sélectionne les contrats individuels les plus récents (par num_ope_niv_1 et date_maj_dwh).
            SELECT
                num_ctr_indiv,
                etat_contrat,
                num_ope_niv_1,
                MAX(num_ope_niv_1) OVER (PARTITION BY num_ctr_indiv) maxopectr,
                date_maj_dwh,
                MAX(date_maj_dwh) OVER (PARTITION BY num_ctr_indiv) maxmaj
            FROM tsac.ldco_contrat_contrat_individuel
            WHERE ind_invalid = 'N'
        ) lcci
            ON lcci.num_ctr_indiv = ctr.num_ctr_indiv
            AND lcci.date_maj_dwh = lcci.maxmaj -- Jointure sur la dernière mise à jour du contrat individuel
        LEFT JOIN (
            -- Sélectionne la dernière adresse d'acheminement pour chaque personne.
            SELECT
                num_personne,
                num_evenement,
                libelle_acheminement,
                MAX(num_evenement) OVER (PARTITION BY num_personne) AS maxeve
            FROM tsac.ldco_personne_adresse
        ) lpa
            ON prs.num_personne = lpa.num_personne
            AND lpa.num_evenement = maxeve -- Jointure sur le dernier événement d'adresse
        WHERE
            -- Exclut les personnes dont le num_personne est déjà présent comme id_source 'RC-CTR' dans le RUP.
            prs.num_personne NOT IN (SELECT CAST(id_source AS NUMERIC) FROM tlrf.r_krup_matching_results WHERE source_system = 'RC-CTR')
            -- Exclut les contrats individuels dont l'état est 'RE' (Résilié) ou 'RA' (Radié).
            AND ctr.num_ctr_indiv NOT IN (SELECT num_ctr_indiv FROM tsac.ldco_contrat_contrat_individuel WHERE etat_contrat IN ('RE','RA'))
            -- Exclut les assurés dont le code_etat est 'RE' (Résilié) ou 'RA' (Radié).
            AND prs.num_personne NOT IN (SELECT num_personne FROM tsac.ldco_contrat_assure WHERE code_etat IN ('RE','RA'))
        GROUP BY 1,3,4,5,6,7,8,9
    ) zz
) yy;
