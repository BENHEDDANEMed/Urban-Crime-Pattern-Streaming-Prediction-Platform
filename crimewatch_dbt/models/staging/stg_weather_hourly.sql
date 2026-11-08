select
    cast(city as varchar)                         as city,
    cast(weather_ts as timestamp)                 as weather_hour_local,
    cast(temperature_c as double)                 as temperature_c,
    cast(precipitation_mm as double)              as precipitation_mm,
    cast(wind_speed_kmh as double)                as wind_speed_kmh,
    cast(is_day as integer)                       as is_day
from {{ source('silver', 'weather_hourly') }}
where temperature_c is not null
