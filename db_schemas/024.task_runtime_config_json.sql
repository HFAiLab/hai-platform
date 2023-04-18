create table "task_runtime_config"
(
    "task_id" integer null default null,
    "chain_id" varchar(255) null default null,
    "config_json" jsonb not null,
    "source" varchar(255) not null,
    "created_at" timestamp not null default current_timestamp,
    "updated_at" timestamp not null default current_timestamp,
    constraint "unq-task_id-source" unique ("task_id", "source"),
    constraint "unq-chain_id-source" unique ("chain_id", "source"),
    check (not ("task_id" is null and "chain_id" is null)),
    check (not ("task_id" is not null and "chain_id" is not null))
);
create index "idx-task_runtime_config-source" on "task_runtime_config" ("source");
comment on table "task_runtime_config" is '任务额外 config_json 表';
comment on column "task_runtime_config"."task_id" is 'task_id';
comment on column "task_runtime_config"."chain_id" is 'chain_id';
comment on column "task_runtime_config"."source" is '来源';
comment on column "task_runtime_config"."config_json" is 'runtime config json 主体';
comment on column "task_runtime_config"."created_at" is 'created_at';
comment on column "task_runtime_config"."updated_at" is 'updated_at';
