-- Chaque crime doit être dans sa case de la grille (centre ± 0,0025°, demi-case).
-- Vérifie que zone_id / zone_lat / zone_lon de Gold sont cohérents avec la position.
select f.event_id, f.latitude, f.longitude, z.zone_lat, z.zone_lon
from {{ ref('fact_crime') }} f
join {{ ref('dim_zone') }} z on z.zone_id = f.zone_id
where abs(f.latitude - z.zone_lat) > 0.0025 + 1e-6
   or abs(f.longitude - z.zone_lon) > 0.0025 + 1e-6
