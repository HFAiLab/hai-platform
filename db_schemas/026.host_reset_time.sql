create table "host_reset_time" (
    "node" varchar(255) not null,
    "reset_time" timestamp default CURRENT_TIMESTAMP not null
);
create index "idx-host_reset_time-node" on "host_reset_time" ("node");
create index "idx-host_reset_time-reset_time" on "host_reset_time" ("reset_time");
comment on table "host_reset_time" is '节点 reset 时间表';
comment on column "host_reset_time"."node" is '节点名';
comment on column "host_reset_time"."reset_time" is 'reset 时间';
