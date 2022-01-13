with owner_id AS (
select id
from users
where user_id_temp = {}
)
insert into file (name, owner_id, type)
values
({}, owner_id, 'avatar');
