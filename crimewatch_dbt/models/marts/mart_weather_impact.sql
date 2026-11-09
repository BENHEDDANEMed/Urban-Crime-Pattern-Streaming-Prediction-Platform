-- Météo et criminalité : y a-t-il plus de crimes par HEURE quand il pleut / fait chaud ?
--
-- On ne peut pas juste compter les crimes "sous la pluie" : il pleut moins souvent qu'il fait sec.
-- On compte donc aussi les heures de chaque condition (Silver weather_hourly) et on divise :
--     crimes_per_hour = crimes pendant cette condition / heures de cette condition
-- index_vs_city > 1 : plus de crimes que d'habitude dans la ville pendant cette condition.
--
-- Piège : il fait plus chaud l'après-midi, et il y a aussi plus de crimes l'après-midi.
-- index_vs_same_hour corrige cet effet : on compare aux crimes "attendus" à ces mêmes heures
-- de la journée (moyenne de la ville pour chaque heure 0-23). C'est l'indicateur à lire.
--
-- Seulement les jours où la ville a des crimes avec une heure fiable (pas Londres).
-- hours_observed, crime_count et expected_crime_count s'additionnent : on peut regrouper (ex. pluie seule) dans le dashboard.

with crime_days as (
    select distinct city, occurred_date
    from {{ ref('fact_crime') }}
    where hour_of_day is not null
),

weather as (
    select
        w.city,
        w.weather_hour_local,
        case when w.precipitation_mm > 0 then 'pluie' else 'sec' end as rain_condition,
        case
            when w.temperature_c < 0  then '1. < 0 °C'
            when w.temperature_c < 10 then '2. 0-10 °C'
            when w.temperature_c < 20 then '3. 10-20 °C'
            when w.temperature_c < 30 then '4. 20-30 °C'
            else '5. ≥ 30 °C'
        end as temperature_band
    from {{ ref('stg_weather_hourly') }} w
    join crime_days d
      on d.city = w.city
     and d.occurred_date = cast(w.weather_hour_local as date)
),

crimes_per_hour as (
    select city, date_trunc('hour', occurred_at_local) as crime_hour, count(*) as n
    from {{ ref('fact_crime') }}
    where hour_of_day is not null
    group by city, date_trunc('hour', occurred_at_local)
),

hours as (
    select w.*, coalesce(c.n, 0) as crimes
    from weather w
    left join crimes_per_hour c
      on c.city = w.city
     and c.crime_hour = w.weather_hour_local
),

with_expected as (
    -- crimes attendus = moyenne de la ville à cette heure de la journée
    select
        h.*,
        avg(h.crimes * 1.0) over (partition by h.city, extract(hour from h.weather_hour_local)) as expected_crimes
    from hours h
)

select
    city,
    rain_condition,
    temperature_band,
    count(*)                                        as hours_observed,
    sum(crimes)                                     as crime_count,
    round(sum(expected_crimes), 2)                  as expected_crime_count,
    round(sum(crimes) * 1.0 / count(*), 3)          as crimes_per_hour,
    round(
        (sum(crimes) * 1.0 / count(*))
        / nullif(sum(sum(crimes)) over (partition by city) * 1.0
                 / sum(count(*)) over (partition by city), 0)
    , 3)                                            as index_vs_city,
    round(sum(crimes) * 1.0 / nullif(sum(expected_crimes), 0), 3) as index_vs_same_hour
from with_expected
group by city, rain_condition, temperature_band
