create table "node_health_check_report" (
    "node" varchar(255) not null,
    "source" varchar(255) not null,
    "err_msg" varchar(2047) not null,
    "raw_log" text null,
    "running_task_id" integer null,
    "last_task_id" integer null,
    "occur_time" timestamp default current_timestamp,
    "node_label" varchar(255) null,
    "get_pods_success" boolean not null default false,
    "stop_pods_success" boolean not null default false,
    "label_node_success" boolean not null default false,
    "created_at" timestamp default current_timestamp
);
comment on table "node_health_check_report" is '节点健康检查记录表';
comment on column "node_health_check_report"."node" is '节点名';
comment on column "node_health_check_report"."source" is '日志来源';
comment on column "node_health_check_report"."err_msg" is '错误原因';
comment on column "node_health_check_report"."raw_log" is '原始日志';
comment on column "node_health_check_report"."running_task_id" is '检查时还在运行的任务';
comment on column "node_health_check_report"."last_task_id" is '检查时最后一个运行的任务';
comment on column "node_health_check_report"."occur_time" is '发生时间';
comment on column "node_health_check_report"."node_label" is '节点移出分组';
comment on column "node_health_check_report"."get_pods_success" is '是否正确查询到运行中的 pods';
comment on column "node_health_check_report"."stop_pods_success" is '是否正确停止了 pods';
comment on column "node_health_check_report"."label_node_success" is '是否正确将节点移出';
comment on column "node_health_check_report"."created_at" is '创建时间';


create index "idx-node_health_check_report-nso" on "node_health_check_report" ("node", "source", "occur_time");
create index "idx-node_health_check_report-occur_time" on "node_health_check_report" ("occur_time");
