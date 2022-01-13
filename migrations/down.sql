alter table groups 
drop column group_id_temp;

alter table users 
drop column group_id_temp;

alter table users 
drop column department;

alter table users 
add column position varchar(255);

alter table groups
drop column cover;