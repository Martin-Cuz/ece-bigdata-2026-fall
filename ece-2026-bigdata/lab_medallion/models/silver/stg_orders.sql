with source as (
    select * from {{ source('bronze', 'orders') }}
),

typed as (
    select
        cast(uuid as uuid) as order_id,
        cast(user_uuid as uuid) as user_id,
        cast(date as timestamptz) as ordered_at,
        cast(quantity as integer) as quantity,
        lower(trim(product)) as product
    from source
)

select *
from typed
qualify row_number() over (partition by order_id order by ordered_at desc) = 1
