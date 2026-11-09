-- Dimension ville : la description (seed) + la période couverte par les données.
with coverage as (
    select
        city,
        min(occurred_date)   as first_crime_date,
        max(occurred_date)   as last_crime_date,
        count(*)             as crime_count
    from {{ ref('stg_crimes') }}
    group by city
)

select
    c.city,
    c.city_name,
    c.country,
    c.timezone,
    c.source_name,
    c.source_dataset,
    c.hour_reliable,
    cov.first_crime_date,
    cov.last_crime_date,
    coalesce(cov.crime_count, 0) as crime_count
from {{ ref('cities') }} c
left join coverage cov on cov.city = c.city
