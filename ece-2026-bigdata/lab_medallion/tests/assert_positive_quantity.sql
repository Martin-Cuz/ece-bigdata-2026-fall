select order_id, quantity
from {{ ref('stg_orders') }}
where quantity <= 0
