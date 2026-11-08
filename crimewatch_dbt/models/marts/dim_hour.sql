select
    hour_of_day,
    hour_label,
    time_slot,
    time_slot_order
from {{ ref('hours') }}
