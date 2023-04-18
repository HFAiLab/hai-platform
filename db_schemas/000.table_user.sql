create type user_role as enum ('internal', 'external');
create table "user" (
    "user_id" integer not null,
    "user_name" varchar(255) not null,
    "nick_name" varchar(255) default null,
    "token" varchar(255) not null,
    "role" user_role not null default 'internal',
    "active" boolean not null default true,
    "last_activity" timestamp default CURRENT_TIMESTAMP not null,
    "shared_group" varchar(255) not null default 'default',
    constraint "pri-user-user_id" primary key ("user_id"),
    constraint "unq-user-token" unique ("token"),
    constraint "unq-user-user_name" unique ("user_name")
);
comment on table "user" is '用户表';
comment on column "user"."user_id" is '用户使用的linux user id';
comment on column "user"."user_name" is '用户名';
comment on column "user"."token" is '用户验证身份的 token';
comment on column "user"."role" is '用户角色：内部用户 or 外部用户';
comment on column "user"."active" is '用户身份是否可用';
comment on column "user"."last_activity" is '用户上次活动时间，pod/task 表发生改动时自动修改';

-- 注意，这里有风险，比如谁加了个内部用户，uid 是 100000，那就烂了
create or replace function add_user_id()
returns trigger as $$
begin
    if new."user_id" is null then
        if new."role" = 'internal' then
            new."user_id" = (select max("user_id") + 1 from "user" where "role" = 'internal');
        else
            new."user_id" = (select coalesce(max("user_id"), 100000) + 1 from "user" where "role" = 'external' and "user_id" > 100000);
        end if;
    end if;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_add_user_id before insert on "user" for each row execute procedure add_user_id();

create or replace function avoid_default()
returns trigger as $$
begin
    if new."shared_group" = 'default' and new."role" = 'external' then  -- 不允许写default
        raise exception '外部用户的shared_group不允许设成default';
    end if;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_avoid_default before insert on "user" for each row execute procedure avoid_default();

create or replace function update_user_nick_name()
returns trigger as $$
begin
    if new."nick_name" is null then
        new."nick_name" = new."user_name";
    end if;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_user_nick_name before insert on "user" for each row execute procedure update_user_nick_name();
