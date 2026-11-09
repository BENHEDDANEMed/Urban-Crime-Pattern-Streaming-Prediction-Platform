select
    cast(zone_id as varchar)          as zone_id,
    cast(city as varchar)             as city,
    cast(occurred_date as date)       as occurred_date,
    cast(occurred_hour as integer)    as hour_of_day,
    cast(crime_count as integer)      as crime_count
from {{ source('gold', 'zone_hourly') }}
