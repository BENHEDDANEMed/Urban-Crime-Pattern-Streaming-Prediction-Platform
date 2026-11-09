-- Gold zone_hourly doit compter exactement les crimes de fact_crime qui ont une heure fiable.
-- Le test échoue s'il renvoie des lignes (ici : les villes où les deux totaux diffèrent).
with from_fact as (
    select city, count(*) as n
    from {{ ref('fact_crime') }}
    where hour_of_day is not null
    group by city
),

from_hourly as (
    select city, sum(crime_count) as n
    from {{ ref('fact_zone_hourly') }}
    group by city
)

select coalesce(f.city, h.city) as city, f.n as fact_count, h.n as hourly_count
from from_fact f
full outer join from_hourly h on h.city = f.city
where coalesce(f.n, 0) <> coalesce(h.n, 0)
