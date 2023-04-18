create table "task_metrics" (
    "id" serial,
    "task_id" integer not null,
    "gpu_rate" decimal(7, 4) null,
    "cpu_rate" decimal(7, 4) null,
    "mem_rate" decimal(7, 4) null,
    "ib_usage" decimal(7, 4) null,
    "created_at" timestamp not null default CURRENT_TIMESTAMP,
    "attempt_num" integer null,
    "success_num" integer null,
    "ib_recv_usage" decimal(7, 4) null,
    "ib_send_usage" decimal(7, 4) null,
    constraint "pri-task_metrics-id" primary key ("id")
);
comment on table "task_metrics" is '记录任务的机器运行数据';
comment on column "task_metrics"."id" is 'metric id';
comment on column "task_metrics"."task_id" is '对应 task 的 id';
comment on column "task_metrics"."gpu_rate" is '任务运行时的平均GPU rate(%)(source: nvsmi.get_gpus)';
comment on column "task_metrics"."cpu_rate" is '任务运行时的平均CPU rate(%)(source: psutil.cpu_percent)';
comment on column "task_metrics"."mem_rate" is '任务运行时的平均内存占用率(%)(source: psutil.virtual_memory().percent)';
comment on column "task_metrics"."ib_usage" is '任务运行时的IB使用率，暂时设置为空';
comment on column "task_metrics"."created_at" is 'created_at';
comment on column "task_metrics"."attempt_num" is '采样请求次数';
comment on column "task_metrics"."success_num" is '采样成功次数';
comment on column "task_metrics"."ib_recv_usage" is '任务运行时的IB recv使用率(%)(source: dstat --ib)';
comment on column "task_metrics"."ib_send_usage" is '任务运行时的IB send使用率(%)(source: dstat --ib)';
