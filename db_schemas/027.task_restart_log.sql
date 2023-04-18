create table "task_restart_log" (
    "task_id" integer not null,
    "rule" varchar(2047) not null,
    "reason" varchar(2047) not null,
    "result" varchar(2047) not null,
    constraint "pri-task_restart_slog-task_id" primary key ("task_id", "rule")
);
comment on table "task_restart_log" is '任务重启日志';
comment on column "task_restart_log"."task_id" is '任务 id';
comment on column "task_restart_log"."rule" is '重启规则';
comment on column "task_restart_log"."reason" is '原因';
comment on column "task_restart_log"."result" is '处理结果';
