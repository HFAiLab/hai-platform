create table "task_artifact_mapping" (
    "user_name" varchar(255) not null,
    "chain_id" varchar(255) not null,
    "nb_name" varchar(511) not null,
    "task_ids" integer[] not null,
    "in_artifact" varchar(1023) not null default '',
    "out_artifact" varchar(1023) not null default '',
    "shared_group" varchar(255) not null,
    "updated_at" timestamp not null default current_timestamp,
    "created_at" timestamp not null default current_timestamp,
    constraint "pri-task_artifact_mapping-chain_id" primary key ("chain_id")
);
create index "idx-task_artifact_mapping-user_name" on "task_artifact_mapping" ("user_name");
create index "idx-task_artifact_mapping-in_artifact" on "task_artifact_mapping" ("in_artifact");
create index "idx-task_artifact_mapping-out_artifact" on "task_artifact_mapping" ("out_artifact");
comment on table "task_artifact_mapping" is 'task_artifact_mapping 表';
comment on column "task_artifact_mapping"."user_name" is 'task归属用户名';
comment on column "task_artifact_mapping"."chain_id" is 'task的chain_id';
comment on column "task_artifact_mapping"."nb_name" is 'task的nb_name';
comment on column "task_artifact_mapping"."task_ids" is 'task的id';
comment on column "task_artifact_mapping"."in_artifact" is 'task的输入artifact信息, artifact_shared_group:name:version';
comment on column "task_artifact_mapping"."out_artifact" is 'task的输出artifact信息, artifact_shared_group:name:version';
comment on column "task_artifact_mapping"."shared_group" is '任务共享的组名';
comment on column "task_artifact_mapping"."updated_at" is '更新时间';
comment on column "task_artifact_mapping"."created_at" is '创建时间';

create or replace function update_task_artifact_mapping_updated_at()
returns trigger as $$
begin
    new."updated_at" = current_timestamp;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_task_artifact_mapping_updated_at before update on "task_artifact_mapping" for each row execute procedure update_task_artifact_mapping_updated_at();
