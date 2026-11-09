select
    cast(city as varchar)                as city,
    cast(zone_id as varchar)             as zone_id,
    cast(day_type as varchar)            as day_type,        -- 'semaine' ou 'week-end'
    cast(hour_of_day as integer)         as hour_of_day,
    cast(prediction_rank as integer)     as prediction_rank, -- 1, 2, 3
    cast(crime_category as varchar)      as crime_category,
    cast(probability as double)          as probability,
    cast(model_run_id as varchar)        as model_run_id,
    cast(scored_at as timestamp)         as scored_at
from {{ source('gold', 'zone_predictions') }}
