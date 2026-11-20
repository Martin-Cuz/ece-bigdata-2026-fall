with source as (
    select * from {{ source('bronze', 'users') }}
),

typed as (
    select
        cast(uuid as uuid) as user_id,
        trim(username) as username,
        lower(trim(username)) as username_normalized,
        trim(name) as name,
        upper(trim(sex)) as sex,
        lower(trim(mail)) as email,
        cast(birthdate as date) as birthdate,
        split_part(address, chr(10), 1) as street,
        split_part(address, chr(10), 2) as address_line_2
    from source
),

first_orders as (
    select user_id, min(ordered_at) as first_order_at
    from {{ ref('stg_orders') }}
    group by user_id
)

select
    t.user_id,
    t.username,
    t.username_normalized,
    t.name,
    t.sex,
    t.email,
    case
        when f.first_order_at is null or t.birthdate <= f.first_order_at::date then t.birthdate
    end as birthdate,
    coalesce(f.first_order_at is null or t.birthdate <= f.first_order_at::date, true) as birthdate_is_valid,
    t.street,
    nullif(regexp_extract(t.address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1), '') as city,
    regexp_extract(t.address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
    regexp_extract(t.address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
from typed t
left join first_orders f on t.user_id = f.user_id
