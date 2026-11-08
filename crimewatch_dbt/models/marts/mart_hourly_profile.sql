-- Profil horaire : nombre MOYEN de crimes par heure, en semaine et le week-end.
-- (moyenne = total / nombre de jours de ce type, sinon les semaines "pèsent" 5/2 fois plus)
with crimes as (
    select
        city,
        occurred_date,
        hour_of_day,
        case when is_weekend = 1 then 'week-end' else 'semaine' end as day_type
    from {{ ref('fact_crime') }}
    where hour_of_day is not null
),

days as (
    select city, day_type, count(distinct occurred_date) as day_count
    from crimes
    group by city, day_type
)

select
    c.city,
    c.day_type,
    c.hour_of_day,
    h.time_slot,
    count(*)                                         as crime_count,
    round(count(*) * 1.0 / max(d.day_count), 2)      as avg_crimes_per_day
from crimes c
join days d on d.city = c.city and d.day_type = c.day_type
join {{ ref('dim_hour') }} h on h.hour_of_day = c.hour_of_day
group by c.city, c.day_type, c.hour_of_day, h.time_slot
