-- Table de faits : 1 ligne = 1 crime.
-- Clés vers les dimensions : city, zone_id, crime_category, occurred_date, hour_of_day.
select
    event_id,
    city,
    zone_id,
    crime_category,
    occurred_date,
    hour_of_day,            -- NULL si l'heure n'est pas fiable
    crime_type,
    occurred_at_local,
    hour_known,
    is_weekend,
    latitude,
    longitude,
    temperature_c,
    precipitation_mm,
    wind_speed_kmh,
    is_raining,
    is_night,
    weather_level
from {{ ref('stg_crimes') }}
