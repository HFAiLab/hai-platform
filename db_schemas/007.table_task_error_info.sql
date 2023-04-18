create table "task_error_info" (
    "id" integer not null,
    "error_info" text null,
    constraint "pri-task_error_info-task_id_pod_id" primary key ("id")
);
comment on table "task_error_info" is '部分error task对应的错误信息';
comment on column "task_error_info"."id" is '对应 task 的 id';
comment on column "task_error_info"."error_info" is 'task对应的error信息';
