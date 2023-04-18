create table "user_access_token" (
    "from_user_name" varchar(255) not null,
    "access_user_name" varchar(255) not null,
    "access_token" varchar(255) default null,
    "access_scope" varchar(255) not null,
    "expire_at" timestamp default CURRENT_TIMESTAMP not null,
    "created_at" timestamp default CURRENT_TIMESTAMP not null,
    "updated_at" timestamp default CURRENT_TIMESTAMP not null,
    "created_by" varchar(255) not null,
    "deleted_by" varchar(255) null default null,
    "active" boolean null default true,
    constraint "pri-user_access_token-user_name-access_user_name" unique ("from_user_name", "access_user_name", "access_scope", "active"),
    constraint "unq-user_access_token-access_token" unique ("access_token")
);
comment on table "user_access_token" is '用户准入 token 表';
comment on column "user_access_token"."from_user_name" is '这个 token 是给谁用的';
comment on column "user_access_token"."access_user_name" is '这个 token 对应的登录用户';
comment on column "user_access_token"."access_token" is 'token';
comment on column "user_access_token"."access_scope" is 'token 的准入范围';
comment on column "user_access_token"."expire_at" is 'token 的过期时间';
comment on column "user_access_token"."created_at" is 'token 的创建时间';
comment on column "user_access_token"."updated_at" is 'token 的更新时间';
comment on column "user_access_token"."created_by" is '这个 token 是谁创建的';
comment on column "user_access_token"."deleted_by" is '这个 token 是谁删除的';
comment on column "user_access_token"."active" is '这条记录有没有效';

create or replace function update_user_access_token_updated_at()
returns trigger as $$
begin
    new."updated_at" = current_timestamp;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_user_access_token_updated_at before update on "user_access_token" for each row execute procedure update_user_access_token_updated_at();

