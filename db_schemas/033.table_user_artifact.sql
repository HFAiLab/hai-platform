create table "user_artifact" (
    "user_name" varchar(255) not null,
    "name" varchar(255) not null,
    "version" varchar(2047) not null default 'default',
    "type" varchar(255) not null default '',
    "location" varchar(2047) not null default '',
    "description" varchar(2047) not null default '',
    "extra" varchar(2047) not null default '',
    "shared_group" varchar(255) not null,
    "updated_at" timestamp not null default current_timestamp,
    "created_at" timestamp not null default current_timestamp,
    constraint "user_artifact-check-name" CHECK ("name" != ''),
    constraint "pri-user_artifact-user_name-name-version" primary key ("user_name", "name", "version")
);
create index "idx-user_artifact-name-version" on "user_artifact" ("name", "version");
comment on table "user_artifact" is 'user_artifact 表';
comment on column "user_artifact"."user_name" is '归属用户名';
comment on column "user_artifact"."name" is 'artifact name';
comment on column "user_artifact"."version" is 'artifact 版本';
comment on column "user_artifact"."type" is '类型，如dataset, pretrain, benchmark';
comment on column "user_artifact"."location" is '对应数据实体所在位置';
comment on column "user_artifact"."description" is 'artifact 描述';
comment on column "user_artifact"."extra" is '预留字段';
comment on column "user_artifact"."shared_group" is '共享的组名';
comment on column "user_artifact"."updated_at" is '更新时间';
comment on column "user_artifact"."created_at" is '创建时间';

create or replace function update_user_artifact_updated_at()
returns trigger as $$
begin
    new."updated_at" = current_timestamp;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_user_artifact_updated_at before update on "user_artifact" for each row execute procedure update_user_artifact_updated_at();

create or replace function delete_user_artifact_update_mapping()
returns trigger as $$
begin
    -- 置空对应mapping表里的引用
    update "task_artifact_mapping"
    set "in_artifact" = ''
    where ("user_name" = old.user_name or "shared_group" = old.shared_group)
        and "in_artifact" like format('%%:%s:%s', old.name, old.version);
    update "task_artifact_mapping"
    set "out_artifact" = ''
    where ("user_name" = old.user_name or "shared_group" = old.shared_group)
        and "out_artifact" like format('%%:%s:%s', old.name, old.version);
    -- 如input, output artifact 都不存在, 则删除对应mapping表里的记录
    delete from "task_artifact_mapping"
    where ("user_name" = old.user_name or "shared_group" = old.shared_group)
        and "in_artifact" = '' and "out_artifact" = '';
    return null;
end;
$$ language 'plpgsql';

create trigger trigger_delete_user_artifact_update_mapping after delete on "user_artifact" for each row execute procedure delete_user_artifact_update_mapping();
