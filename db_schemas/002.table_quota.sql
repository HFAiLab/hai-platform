create table "quota" (
    "user_name" varchar(255) not null,
    "resource" varchar(255) not null,
    "quota" bigint not null default 0,
    "expire_time" timestamp default null,
    "created_at" timestamp default current_timestamp,
    "updated_at" timestamp default current_timestamp,
    constraint "pri-quota-user_name-resource" primary key ("user_name", "resource")
);
comment on table "quota" is '用户 quota 表';
comment on column "quota"."user_name" is '用户名';
comment on column "quota"."resource" is '资源';
comment on column "quota"."quota" is 'quota';

create index "idx-quota-expire_time" on "quota" ("expire_time");


create or replace function update_quota_updated_at()
returns trigger as $$
begin
    new."updated_at" = current_timestamp;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_quota_updated_at before update on "quota" for each row execute procedure update_quota_updated_at();

