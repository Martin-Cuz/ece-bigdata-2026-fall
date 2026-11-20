{{ config(severity='warn') }}

-- The zip code of a user must belong to the range of its state
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
join {{ ref('state_zip_ranges') }} r on u.state = r.state
where cast(u.zip_code as integer) not between r.zip_min and r.zip_max
