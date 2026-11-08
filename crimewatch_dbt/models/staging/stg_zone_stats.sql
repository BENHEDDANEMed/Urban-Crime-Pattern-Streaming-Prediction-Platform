select
    cast(zone_id as varchar)             as zone_id,
    cast(city as varchar)                as city,
    cast(zone_lat as double)             as zone_lat,
    cast(zone_lon as double)             as zone_lon,
    cast(crime_count as integer)         as crime_count,
    cast(active_days as integer)         as active_days,
    cast(first_date as date)             as first_date,
    cast(last_date as date)              as last_date,
    cast(top_category as varchar)        as top_category,
    cast(share_of_city_pct as double)    as share_of_city_pct,
    cast(rank_in_city as integer)        as rank_in_city
from {{ source('gold', 'zone_stats') }}
