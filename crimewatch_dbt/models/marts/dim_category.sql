select
    crime_category,
    category_label_fr,
    crime_family,
    is_violent
from {{ ref('crime_categories') }}
