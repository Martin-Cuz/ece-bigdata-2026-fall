select
    o.order_id,
    o.user_id
from {{ ref('stg_orders') }} o
left join {{ ref('stg_users') }} u
    on o.user_id = u.user_id
where u.user_id is null
