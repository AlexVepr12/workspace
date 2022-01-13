-- +migrate Up
alter table users
    drop column position;

-- +migrate Down
alter table users
    add column position varchar(255);

-- +migrate Up
alter table users
    add column workspace_id varchar(255) unique,
    add column department   varchar(255),
    add column about        text;

-- +migrate Down
alter table users
    drop column workspace_id,
    drop column department,
    drop column about;


-- NEW
alter table users
    rename column workspace_id to workplace_id;
alter table groups
    add column workplace_id varchar(255) unique;
alter table group_member
    add administrator bool default false;
create unique index file_name_uindex
    on file (name);

-- NEW v2
alter table post
    add column workplace_id varchar(255) unique;

create unique index users_email_uindex
    on users (email);

alter table file
    add column description text;

