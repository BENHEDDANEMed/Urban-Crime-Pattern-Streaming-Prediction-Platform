-- Dimension zone : une case de ~500 m (grille de 0,005°) et son profil.
select
    zone_id,
    city,
    zone_lat,
    zone_lon,
    crime_count,
    active_days,
    first_date,
    last_date,
    top_category,
    share_of_city_pct,
    rank_in_city
from {{ ref('stg_zone_stats') }}
