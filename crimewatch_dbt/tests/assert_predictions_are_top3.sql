-- Pour chaque zone / type de jour / heure : exactement 3 prédictions, de probabilités
-- décroissantes (rang 1 >= rang 2 >= rang 3), et une somme <= 1.
with grouped as (
    select
        zone_id, day_type, hour_of_day,
        count(*)                                                        as n,
        sum(probability)                                                as total_proba,
        max(case when prediction_rank = 1 then probability end)         as p1,
        max(case when prediction_rank = 2 then probability end)         as p2,
        max(case when prediction_rank = 3 then probability end)         as p3
    from {{ ref('mart_zone_predictions') }}
    group by zone_id, day_type, hour_of_day
)

select *
from grouped
where n <> 3 or total_proba > 1.0001 or p1 < p2 or p2 < p3
