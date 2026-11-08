-- Comparaison multi-villes : une ligne par ville (cas d'usage "Chicago vs LA vs NYC vs Londres").
with crimes as (
    select f.*, cat.is_violent
    from {{ ref('fact_crime') }} f
    join {{ ref('dim_category') }} cat on cat.crime_category = f.crime_category
),

top_category as (
    select city, crime_category as top_category
    from (
        select
            city,
            crime_category,
            row_number() over (partition by city order by count(*) desc, crime_category) as rn
        from crimes
        group by city, crime_category
    ) ranked
    where rn = 1
)

select
    c.city,
    d.city_name,
    count(*)                                                    as crime_count,
    count(distinct c.zone_id)                                   as zone_count,
    count(distinct c.occurred_date)                             as day_count,
    round(count(*) * 1.0 / count(distinct c.occurred_date), 1)  as crimes_per_day,
    round(100.0 * sum(c.hour_known) / count(*), 1)              as pct_hour_known,
    round(100.0 * sum(case when c.is_night = 1 then 1 else 0 end)
          / nullif(sum(case when c.is_night is not null then 1 else 0 end), 0), 1) as pct_night,
    round(100.0 * sum(c.is_weekend) / count(*), 1)              as pct_weekend,
    round(100.0 * sum(c.is_violent) / count(*), 1)              as pct_violent,
    t.top_category
from crimes c
join {{ ref('dim_city') }} d on d.city = c.city
join top_category t on t.city = c.city
group by c.city, d.city_name, t.top_category
