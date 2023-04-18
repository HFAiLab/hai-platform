create table "task_ng" (
    "id" serial,
    "nb_name" varchar(511) not null,
    "user_name" varchar(255) not null,
    "code_file" varchar(2047) not null,
    "workspace" varchar(255) not null,
    "config_json" jsonb not null,
    "group" varchar(2048) not null,
    "nodes" integer not null,
    "assigned_nodes" varchar[] not null default array[]::varchar[],
    "restart_count" integer not null default 0,
    "whole_life_state" integer not null default 0,
    "first_id" integer not null,
    "backend" varchar(255) not null,
    "task_type" varchar(255) not null,
    "queue_status" varchar(255) not null default 'finished',
    "notes" text null,
    "priority" integer not null default 0,
    "chain_id" varchar(255) not null,
    "stop_code" integer not null default 0,
    "suspend_code" integer not null default 0,
    "mount_code" integer not null default 2,
    "suspend_updated_at" timestamp not null default CURRENT_TIMESTAMP,
    "begin_at" timestamp not null default CURRENT_TIMESTAMP,
    "end_at" timestamp not null default CURRENT_TIMESTAMP,
    "created_at" timestamp not null default CURRENT_TIMESTAMP,
    "worker_status" varchar(255) not null default 'queued',
    "last_task" boolean default false,
    constraint "pri-task_ng-id" primary key ("id")
);
create index "idx-task_ng-user_name" on "task_ng" ("user_name");
create index "idx-task_ng-nb_name" on "task_ng" ("nb_name");
create index "idx-task_ng-chain_id" on "task_ng" ("chain_id");
create index "idx-task_ng-first_id" on "task_ng" ("first_id");
create index "idx-task_ng-suspend_updated_at" on "task_ng" ("suspend_updated_at");
create index "idx-task_ng-begin_at" on "task_ng" ("begin_at");
create index "idx-task_ng-end_at" on "task_ng" ("end_at");
create index "idx-task_ng-created_at" on "task_ng" ("created_at");
create index "idx-task_ng-chain_id-varchar" on "task_ng"("chain_id" varchar_pattern_ops);
create index "idx-task_ng-worker_status" on "task_ng" ("worker_status");
create index "idx-task_ng-backend" on "task_ng" ("backend");
create index "idx-task_ng-last_task" on "task_ng" ("last_task");
create index "idx-task_ng-queue_status" on "task_ng" ("queue_status");
create index "idx-task_ng-user_name-last_task-id-task_type-w" on "task_ng" ("user_name", "last_task", "id", "task_type", "worker_status");
create index "idx-task_ng-user_name-last_task-first_id-task_type-w" on "task_ng" ("user_name", "last_task", "first_id", "task_type", "worker_status");
create index "idx-task_ng-user_name-created_at" on "task_ng" ("user_name", "created_at");
comment on table "task_ng" is '记录任务的机器运行数据';
comment on column "task_ng"."id" is '任务的 id';
comment on column "task_ng"."nb_name" is '任务的 nb_name';
comment on column "task_ng"."user_name" is '谁的任务';
comment on column "task_ng"."code_file" is '任务运行的 code_file';
comment on column "task_ng"."workspace" is '任务运行的 workspace';
comment on column "task_ng"."group" is '申请哪个组的资源';
comment on column "task_ng"."nodes" is '申请的节点数';
comment on column "task_ng"."assigned_nodes" is '分配给这个任务的节点';
comment on column "task_ng"."restart_count" is '任务重启次数';
comment on column "task_ng"."whole_life_state" is '任务的 whole_life_state，32位整数，任务第一次启动的时候初始化为0，任务挂起不重置这个值';
comment on column "task_ng"."first_id" is '这条链上的第一个 id';
comment on column "task_ng"."backend" is '集群运行后端';
comment on column "task_ng"."task_type" is '任务类型';
comment on column "task_ng"."queue_status" is '任务队列状态';
comment on column "task_ng"."notes" is '备注';
comment on column "task_ng"."priority" is '优先级';
comment on column "task_ng"."chain_id" is '任务的 chain_id';
comment on column "task_ng"."stop_code" is '任务结束码';
comment on column "task_ng"."suspend_code" is '中断：0-不需要；1-需要；2-需要且知晓；3-可以 suspend 我';
comment on column "task_ng"."mount_code" is '经过编码的 mount 路径';
comment on column "task_ng"."suspend_updated_at" is 'suspend_code 更新时间';
comment on column "task_ng"."begin_at" is '任务开始时间';
comment on column "task_ng"."end_at" is '任务结束时间';
comment on column "task_ng"."created_at" is '任务创建时间';
comment on column "task_ng"."worker_status" is '任务结束时的状态';

create or replace function update_task_ng_queue_status()
returns trigger as $$
begin
    if new."queue_status" in ('queued', 'scheduled') then
        raise exception '不可以将 task_ng 中的任务改为运行态 / 提交运行态的任务';
    end if;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_task_ng_queue_status before insert or update of "queue_status" on "task_ng" for each row execute procedure update_task_ng_queue_status();
