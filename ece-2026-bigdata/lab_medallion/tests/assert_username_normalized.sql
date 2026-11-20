select user_id, username, username_normalized
from {{ ref('stg_users') }}
where username_normalized != lower(trim(username))
   or username_normalized != trim(username_normalized)
