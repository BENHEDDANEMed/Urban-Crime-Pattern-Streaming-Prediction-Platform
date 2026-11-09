-- Prédictions prêtes pour le dashboard : "dans cette zone, à cette heure, attendez-vous surtout à..."
select
    p.city,
    p.zone_id,
    z.zone_lat,
    z.zone_lon,
    z.rank_in_city,
    p.day_type,
    p.hour_of_day,
    p.prediction_rank,
    p.crime_category,
    c.category_label_fr,
    p.probability,
    p.model_run_id,
    p.scored_at
from {{ ref('stg_zone_predictions') }} p
join {{ ref('dim_zone') }} z on z.zone_id = p.zone_id
join {{ ref('dim_category') }} c on c.crime_category = p.crime_category
