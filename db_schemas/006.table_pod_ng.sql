create table "pod_ng" (
    "task_id" integer not null,
    "pod_id" varchar(255) not null,
    "job_id" integer not null,
    "status" varchar(255) not null,
    "exit_code" varchar(255) not null default 'nan',
    "node" varchar(255) not null,
    "assigned_gpus" int[] not null default array[]::int[],
    "memory" bigint not null,
    "cpu" integer not null,
    "role" varchar(255) not null,
    "created_at" timestamp not null default current_timestamp,
    "begin_at" timestamp not null default current_timestamp,
    "end_at" timestamp not null default current_timestamp,
    constraint "pri-pod_ng-task_id-pod_id" primary key ("task_id", "pod_id"),
    constraint "unq-pod_ng-node-task_id" unique ("node", "task_id")
);
create index "idx-pod_ng-pod_id" on "pod_ng" ("pod_id");
comment on table "pod_ng" is 'pod 表';
comment on column "pod_ng"."task_id" is '对应的 task_id';
comment on column "pod_ng"."pod_id" is 'pod 的 id，对应 k8s 的 pod_name';
comment on column "pod_ng"."job_id" is 'job 的 id，与 rank 一致';
comment on column "pod_ng"."status" is 'pod 中的 job_container 状态';
comment on column "pod_ng"."exit_code" is '容器 shell 结束码';
comment on column "pod_ng"."node" is '分配的节点名';
comment on column "pod_ng"."assigned_gpus" is '分配的 gpu 编号';
comment on column "pod_ng"."memory" is '内存限制，单位 bytes';
comment on column "pod_ng"."cpu" is 'cpu 限制，单位 milicpu';
comment on column "pod_ng"."role" is 'master/worker';
comment on column "pod_ng"."created_at" is 'pod 记录的创建时间';
comment on column "pod_ng"."begin_at" is 'pod 变为 building 的时间';
comment on column "pod_ng"."end_at" is 'pod 结束的时间';

create or replace function update_pod_status()
returns trigger as $$
begin
    if new."status" = 'building' then
        new."begin_at" = current_timestamp;
        new."end_at" = new."begin_at";
    elsif new."status" in ('succeeded', 'failed', 'stopped') then
        new."end_at" = current_timestamp;
    end if;
    if old."status" in ('succeeded', 'failed', 'stopped') then
        raise exception '已到达终态';
    end if;
    if new."status" in ('succeeded', 'failed', 'stopped') then
        return new;
    end if;
    if old."status" in ('succeeded_terminating', 'failed_terminating', 'stopped_terminating') then
        raise exception '已到达terminating态且新状态不是终态';
    end if;
    if new."status" in ('succeeded_terminating', 'failed_terminating', 'stopped_terminating') then
        return new;
    end if;
    if old."status" in ('running') then
        raise exception '已到达running态';
    end if;
    return new;
end;
$$ language 'plpgsql';
create trigger update_pod_ng_status before update of "status" on "pod_ng" for each row execute procedure update_pod_status();
