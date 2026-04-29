--===========================================
-- Sélection finale : Compte les parcours d'opportunité/besoin fermés par date.
--===========================================
SELECT
    ldob.hrdtsttoppbesn::date AS datopb,          -- Date d'horodatage du statut de l'opportunité/besoin
    COUNT(DISTINCT ldob.ideoppbesn) AS nb_parcours -- Nombre de parcours d'opportunité/besoin uniques
FROM tlak_rad.lak_dco_opportunite_besoin ldob     -- Table des opportunités/besoins
WHERE
    codsttoppbesn = 'OPB-STT-FER'                 -- Filtre sur les parcours dont le statut est 'FER' (Fermé)
    AND avenant = TRUE                            -- Filtre sur les enregistrements qui sont des avenants
    AND ldob.hrdtsttoppbesn >= CURRENT_DATE - INTERVAL '30 days' -- Filtre sur les 30 derniers jours
GROUP BY datopb                                   -- Regroupe par date du statut
ORDER BY datopb DESC;                             -- Trie par date décroissante
