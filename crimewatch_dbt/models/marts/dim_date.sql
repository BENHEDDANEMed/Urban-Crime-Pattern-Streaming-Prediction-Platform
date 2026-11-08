-- Dimension date : une ligne par jour présent dans les données.
-- extract(...) existe avec la même syntaxe dans DuckDB et Snowflake.
with days as (
    select distinct occurred_date as date_day
    from {{ ref('stg_crimes') }}
)

select
    date_day,
    extract(year from date_day)                     as year,
    extract(quarter from date_day)                  as quarter,
    extract(month from date_day)                    as month,
    extract(day from date_day)                      as day_of_month,
    extract(dow from date_day) + 1                  as day_of_week,      -- 1 = dimanche ... 7 = samedi (comme Spark)
    case extract(dow from date_day)
        when 0 then 'dimanche' when 1 then 'lundi' when 2 then 'mardi' when 3 then 'mercredi'
        when 4 then 'jeudi' when 5 then 'vendredi' else 'samedi'
    end                                             as day_name_fr,
    case when extract(dow from date_day) in (0, 6) then 1 else 0 end as is_weekend,
    case
        when extract(month from date_day) in (12, 1, 2) then 'hiver'
        when extract(month from date_day) in (3, 4, 5) then 'printemps'
        when extract(month from date_day) in (6, 7, 8) then 'été'
        else 'automne'
    end                                             as season
from days
