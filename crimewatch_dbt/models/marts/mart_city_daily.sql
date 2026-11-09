-- Évolution dans le temps : crimes par ville et par jour.
select
    f.city,
    f.occurred_date,
    count(*)                                      as crime_count,
    sum(cat.is_violent)                           as violent_count
from {{ ref('fact_crime') }} f
join {{ ref('dim_category') }} cat on cat.crime_category = f.crime_category
group by f.city, f.occurred_date
