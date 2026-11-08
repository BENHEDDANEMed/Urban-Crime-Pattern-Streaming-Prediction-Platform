-- Un crime par ligne, colonnes renommées et typées.
-- L'heure n'est gardée que si elle est fiable (hour_known = 1) : sinon NULL,
-- pour qu'aucune analyse "par heure" ne compte par erreur les crimes de Londres ou de 00:00.

select
    cast(event_id as varchar)                                   as event_id,
    cast(city as varchar)                                       as city,
    cast(zone_id as varchar)                                    as zone_id,
    cast(crime_category as varchar)                             as crime_category,
    cast(crime_type as varchar)                                 as crime_type,
    cast(occurred_ts as timestamp)                              as occurred_at_local,
    cast(occurred_date as date)                                 as occurred_date,
    case when hour_known = 1 then cast(occurred_hour as integer) end as hour_of_day,
    cast(hour_known as integer)                                 as hour_known,
    cast(day_of_week as integer)                                as day_of_week,   -- 1 = dimanche ... 7 = samedi
    cast(is_weekend as integer)                                 as is_weekend,
    cast(latitude as double)                                    as latitude,
    cast(longitude as double)                                   as longitude,
    cast(temperature_c as double)                               as temperature_c,
    cast(precipitation_mm as double)                            as precipitation_mm,
    cast(wind_speed_kmh as double)                              as wind_speed_kmh,
    cast(is_raining as integer)                                 as is_raining,
    cast(is_night as integer)                                   as is_night,
    cast(weather_level as varchar)                              as weather_level
from {{ source('gold', 'crime_features') }}
