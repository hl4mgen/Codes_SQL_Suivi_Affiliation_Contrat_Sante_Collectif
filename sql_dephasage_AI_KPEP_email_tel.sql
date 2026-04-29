--===========================================
-- CTE : AdrKPEP
-- Objectif : Récupérer l'adresse postale la plus pertinente des personnes KPEP.
--===========================================
WITH AdrKPEP AS (
    --===========================================
    -- Sous-CTE : KPEP
    -- Extrait les identifiants KPEP et les 'num_acc' (ID Accolade/AI) associés.
    -- Filtre sur les KPEP actifs et valides à la date du jour.
    --===========================================
    WITH KPEP AS (
        SELECT DISTINCT
            KPEP.idkpep,
            UPPER(COALESCE(KPEP.nmusg, KPEP.nmptr)) AS Nom_kpep,
            UPPER(KPEP.pnusu) AS Prenom_kpep,
            KPEP.datnai AS DatNai_kpep,
            sigest.idpersi AS num_acc
        FROM tedo_cr.ed_kpep_personnephysique KPEP
        INNER JOIN tedo_cr.ed_kpep_si_gestion sigest ON (
            sigest.idkpep = KPEP.idkpep
            AND sigest.codsi IN ('RC-CTR', 'FICRC', 'FICEMP')
            AND CURRENT_DATE BETWEEN sigest.datdebvld AND sigest.datfinvld
        )
        WHERE 1=1
        AND CURRENT_DATE BETWEEN KPEP.DATDEBVLD AND KPEP.DATFINVLD
        AND LEFT(KPEP.idkpep,4) = 'KPEP'
    ),
    --===========================================
    -- Sous-CTE : AdrKPEP (détail)
    -- Extrait les adresses postales KPEP de type "courrier habituel" (CO-HA).
    -- Priorise la dernière adresse effective pour chaque KPEP.
    --===========================================
    AdrKPEP AS (
        SELECT DISTINCT
            idkpep,
            typadr,
            codpysptl,
            novoi,
            libvoi,
            sctptl,
            codptl,
            codptlloc,
            loc,
            ROW_NUMBER() OVER (PARTITION BY idkpep ORDER BY datdebeffutl DESC, datdebvld DESC, datcso DESC) AS Rang
        FROM tedo_cr.ed_kpep_adressepostale
        WHERE 1 = 1
        AND typutladrptl = 'CO'
        AND modutladr = 'HA'
        AND CURRENT_DATE BETWEEN datdebvld AND datfinvld
        AND CURRENT_DATE BETWEEN datdebeffutl AND NVL(datfineffutl,'99991231')
    )
    -- Sélection finale de AdrKPEP : Joint les infos KPEP et leur adresse prioritaire.
    SELECT
        KPEP.*,
        AdrKPEP.typadr,
        AdrKPEP.codpysptl,
        DECODE(TO_CHAR(novoi),'0','',TO_CHAR(novoi)) AS novoi,
        AdrKPEP.libvoi,
        AdrKPEP.sctptl,
        AdrKPEP.codptl,
        AdrKPEP.loc
    FROM KPEP
    INNER JOIN AdrKPEP ON AdrKPEP.idkpep = KPEP.idkpep
    WHERE AdrKPEP.Rang = 1
),

--===========================================
-- CTE : AdrAI
-- Objectif : Récupérer l'adresse postale la plus pertinente des Assurés Individuels (AI) depuis TSAC.
--===========================================
AdrAI AS (
    SELECT B.* FROM (
        SELECT
            A.*,
            ROW_NUMBER() OVER (PARTITION BY A.RefSrcPer ORDER BY CASE A.Type_adresse WHEN 'Explicite' THEN 1 ELSE 2 END ASC) AS RangGlobal -- Priorité à l'adresse Explicite
        FROM (
            --===========================================
            -- Sous-CTE : AdresseExp
            -- Extrait les adresses explicites de souscription (ADSO) depuis TSAC.
            -- Filtre sur les adresses actives et de rôle 'ADSO', priorise la dernière effective.
            --===========================================
            WITH AdresseExp AS (
                SELECT * FROM (
                    SELECT DISTINCT
                        TO_CHAR(ADR.num_personne) AS RefSrcPer,
                        'Explicite' AS Type_adresse,
                        ADR.pays AS Pays_adresse,
                        DECODE(num_voie,NULL,'',num_voie) AS num_voie,
                        DECODE(lib_voie,NULL,'',lib_voie) AS lib_voie,
                        DECODE(lib_lieudit,NULL,'',lib_lieudit) AS lib_lieudit,
                        DECODE(boite_postale,NULL,'',boite_postale) AS boite_postale,
                        DECODE(code_postal,NULL,'',code_postal) AS code_postal,
                        DECODE(insee_localite,NULL,'',insee_localite) AS insee_localite,
                        DECODE(lib_localite,NULL,'',lib_localite) AS lib_localite,
                        DECODE(nom_bur_cedex,NULL,'',nom_bur_cedex) AS lib_localite_cedex,
                        DECODE(complement_geo,NULL,'',complement_geo) AS complement_geo,
                        DECODE(pt_remise_courrier,NULL,'',pt_remise_courrier) AS pt_remise_courrier,
                        RTP.libcog AS Pays,
                        RTP.codiso2 AS codiso2,
                        ADR.role_adresse AS usage_adresse,
                        ADR.date_deb_effet AS date_deb_effet,
                        ADR.date_fin_effet AS date_fin_effet,
                        ADR.datchg AS date_chgt,
                        ADR.code_soc_appart AS codsoc,
                        ROW_NUMBER() OVER (PARTITION BY ADR.num_personne
                                            ORDER BY
                                              CASE ADR.role_adresse WHEN 'ADSO' THEN 1 ELSE 2 END ASC,
                                            ADR.date_deb_effet DESC,
                                            ADR.datchg DESC) AS Rang
                    FROM tsac.ldco_personne_adresses_explicites ADR
                    LEFT JOIN tlrf.r_tran_pays RTP ON RTP.codiso3 = ADR.pays
                                                    AND CURRENT_DATE BETWEEN RTP.datdebvld AND RTP.datfinvld
                                                    AND CURRENT_DATE BETWEEN RTP.datdebeff AND NVL(RTP.datfineff, '2999-12-31')
                    WHERE 1=1
                    AND CURRENT_DATE BETWEEN date_deb_effet AND NVL(date_fin_effet, '2999-12-31')
                    AND ADR.role_adresse = 'ADSO'
                ) ADRR WHERE ADRR.Rang = 1
            ),
            --===========================================
            -- Sous-CTE : AdresseImp
            -- Extrait les adresses implicites depuis TSAC.
            -- Filtre sur les adresses actives, priorise temporaire, domicile, puis professionnel.
            --===========================================
            AdresseImp AS (
                SELECT * FROM (
                    SELECT DISTINCT
                        TO_CHAR(ADR.num_personne) AS RefSrcPer,
                        'Implicite' AS Type_adresse,
                        ADR.pays AS Pays_adresse,
                        DECODE(num_voie,NULL,'',num_voie) AS num_voie,
                        DECODE(lib_voie,NULL,'',lib_voie) AS lib_voie,
                        DECODE(lib_lieudit,NULL,'',lib_lieudit) AS lib_lieudit,
                        DECODE(boite_postale,NULL,'',boite_postale) AS boite_postale,
                        DECODE(code_postal,'@','',code_postal) AS code_postal,
                        DECODE(insee_localite,NULL,'',insee_localite) AS insee_localite,
                        DECODE(lib_localite,NULL,'',lib_localite) AS lib_localite,
                        DECODE(libelle_acheminement,NULL,'',libelle_acheminement) AS lib_localite_cedex,
                        DECODE(complement_geo,NULL,'',complement_geo) AS complement_geo,
                        DECODE(pt_remise_courrier,NULL,'',pt_remise_courrier) AS pt_remise_courrier,
                        RTP.libcog AS Pays,
                        RTP.codiso2 AS codiso2,
                        ADR.type_adresse AS usage_adresse,
                        ADR.date_deb_effet AS date_deb_effet,
                        ADR.date_fin_effet AS date_fin_effet,
                        ADR.datcso AS date_chgt,
                        ADR.code_soc_appart AS codsoc,
                        ROW_NUMBER() OVER (PARTITION BY ADR.num_personne
                                            ORDER BY
                                              CASE ADR.type_adresse WHEN 'TEMP' THEN 1 WHEN 'DOM' THEN 2 ELSE 3 END ASC,
                                            ADR.date_deb_effet DESC,
                                            ADR.date_maj_dwh DESC) AS Rang
                    FROM tsac.ldco_personne_adresse ADR
                    LEFT JOIN tlrf.r_tran_pays RTP ON RTP.codiso3 = ADR.pays
                                                    AND CURRENT_DATE BETWEEN RTP.datdebvld AND RTP.datfinvld
                                                    AND CURRENT_DATE BETWEEN RTP.datdebeff AND NVL(RTP.datfineff, '2999-12-31')
                    WHERE 1=1
                    AND CURRENT_DATE BETWEEN ADR.date_deb_effet AND NVL(ADR.date_fin_effet, '2999-12-31')
                ) ADRR WHERE ADRR.Rang = 1
            )
            -- Union des adresses explicites et implicites.
            SELECT * FROM AdresseExp
            UNION
            SELECT * FROM AdresseImp
        ) A
    ) B WHERE B.RangGlobal = '1'
)

--===========================================
-- Sélection finale : Compte les ID AI dont l'adresse ne correspond pas à l'adresse KPEP.
--===========================================
SELECT COUNT(DISTINCT(ACC.refsrcper))
FROM AdrKPEP KPEP
INNER JOIN AdrAI ACC ON ACC.RefSrcPer = KPEP.num_acc
INNER JOIN (
    --===========================================
    -- Sous-requête : Identifie les ID AI dont l'adresse ne correspond PAS à l'adresse KPEP.
    -- Utilise une clause NOT EXISTS avec des comparaisons détaillées sur les composants de l'adresse.
    -- La logique de comparaison des adresses est complexe, incluant le nettoyage des chaînes (retrait de "cedex" et des chiffres).
    --===========================================
    SELECT B.refsrcper
    FROM AdrAI B
    WHERE NOT EXISTS (
        SELECT 1 FROM AdrKPEP A
        WHERE A.num_acc = B.refsrcper
        -- Comparaisons des composants d'adresse : libellé de voie, code postal, localité (avec nettoyage)
        AND LOWER(TRIM(B.lib_voie)) = LOWER(TRIM(NVL(A.libvoi,'')))
        AND B.code_postal = NVL(A.codptl,'')
        AND TRIM(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(DECODE(B.lib_localite_cedex,'',B.lib_localite,B.lib_localite_cedex)),'cedex',''),'9',''),'8',''),'7',''),'6',''),'5',''),'4',''),'3',''),'2',''),'1',''),'0',''))
            =
            TRIM(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(NVL(A.loc,'')),'cedex',''),'9',''),'8',''),'7',''),'6',''),'5',''),'4',''),'3',''),'2',''),'1',''),'0',''))
    )
) AI2 ON AI2.refsrcper = KPEP.num_acc;
