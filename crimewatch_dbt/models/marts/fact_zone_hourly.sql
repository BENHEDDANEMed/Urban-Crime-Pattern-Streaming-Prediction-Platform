-- Table de faits agrégée : crimes par zone, jour et heure (pour la heatmap heure par heure).
select
    zone_id || '|' || cast(occurred_date as varchar) || '|' || cast(hour_of_day as varchar) as zone_hour_key,
    zone_id,
    city,
    occurred_date,
    hour_of_day,
    crime_count
from {{ ref('stg_zone_hourly') }}
