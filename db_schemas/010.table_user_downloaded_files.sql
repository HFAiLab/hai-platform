create type sync_status as enum ('running', 'finished', 'failed', 'init', 'stage1_running', 'stage2_running', 'stage1_finished', 'stage1_failed', 'stage2_failed');
create type file_type as enum ('workspace', 'dataset', 'env', 'doc', 'pypi', 'website');

create table "user_downloaded_files" (
    "user_name" varchar(255) not null,
    "user_role" varchar(255) not null,
    "file_type" file_type not null,
    "file_path" varchar(2047) not null,
    "file_size" bigint not null,
    "file_mtime" varchar(255) not null,
    "file_md5" varchar(255) not null,
    "status" sync_status not null,
    "updated_at" timestamp not null default current_timestamp,
    "created_at" timestamp not null default current_timestamp,
    "deleted_at" timestamp,
    constraint "pri-user_downloaded_files-file_path-file_mtime" primary key ("file_path", "file_md5")
);
create index "idx-user_downloaded_files-user_name" on "user_downloaded_files" ("user_name");
comment on table "user_downloaded_files" is '用户上传到外部文件列表';
comment on column "user_downloaded_files"."user_name" is '用户名';
comment on column "user_downloaded_files"."user_role" is '用户身份类型';
comment on column "user_downloaded_files"."file_type" is '文件类型';
comment on column "user_downloaded_files"."file_path" is '文件路径';
comment on column "user_downloaded_files"."file_size" is '文件大小';
comment on column "user_downloaded_files"."file_mtime" is '文件修改时间';
comment on column "user_downloaded_files"."file_md5" is '文件md5 hash值';
comment on column "user_downloaded_files"."status" is '文件同步状态';
comment on column "user_downloaded_files"."updated_at" is '更新时间';
comment on column "user_downloaded_files"."created_at" is '创建时间';
comment on column "user_downloaded_files"."deleted_at" is '删除时间';

create or replace function update_user_downloaded_files_updated_at()
returns trigger as $$
begin
    new."updated_at" = current_timestamp;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_user_downloaded_files_updated_at before update on "user_downloaded_files" for each row execute procedure update_user_downloaded_files_updated_at();
