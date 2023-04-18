create table "user_sync_status" (
    "user_name" varchar(255) not null,
    "user_role" varchar(255) not null,
    "file_type" file_type not null,
    "name" varchar(2047) not null,
    "pull_status" sync_status,
    "last_pull" timestamp,
    "push_status" sync_status,
    "last_push" timestamp,
    "local_path" varchar(2047) not null,
    "cluster_path" varchar(2047) not null,
    "updated_at" timestamp not null default current_timestamp,
    "created_at" timestamp not null default current_timestamp,
    "deleted_at" timestamp,
    constraint "pri-user_sync_status-user_name-file_type-name" primary key ("user_name", "file_type", "name")
);
create index "idx-user_sync_status-user_name" on "user_sync_status" ("user_name");
comment on table "user_sync_status" is '用户文件同步状态列表';
comment on column "user_sync_status"."user_name" is '用户名';
comment on column "user_sync_status"."user_role" is '用户身份类型';
comment on column "user_sync_status"."file_type" is '文件类型';
comment on column "user_sync_status"."name" is 'workspace/env/dataset 名字';
comment on column "user_sync_status"."pull_status" is 'pull状态';
comment on column "user_sync_status"."last_pull" is '上次pull时间';
comment on column "user_sync_status"."push_status" is 'push状态';
comment on column "user_sync_status"."last_push" is '上次push时间';
comment on column "user_sync_status"."local_path" is '本地目录';
comment on column "user_sync_status"."cluster_path" is '集群侧目录';
comment on column "user_sync_status"."updated_at" is '更新时间';
comment on column "user_sync_status"."created_at" is '创建时间';
comment on column "user_sync_status"."deleted_at" is '删除时间';

create or replace function update_user_sync_status_updated_at()
returns trigger as $$
begin
    new."updated_at" = current_timestamp;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_user_sync_status_updated_at before update on "user_sync_status" for each row execute procedure update_user_sync_status_updated_at();
