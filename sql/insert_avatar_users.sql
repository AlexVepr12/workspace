with avatar_id as (
select id
from files
where type = 'avatar' AND name = {}
)
update  users
set avatar = avatar_id
where id = {};
