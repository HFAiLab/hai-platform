create table "host" (
    "node" varchar(255) not null,
    "gpu_num" integer not null,
    "type" varchar(255) not null default 'gpu',
    "use" varchar(255) not null default 'training',
    "origin_group" varchar(255) not null default '',
    "room" varchar(255) not null default '',
    "schedule_zone" varchar(255) not null default '',
    constraint "host-check-origin_group" CHECK ("origin_group" != ''),
    constraint "pri-host-node" primary key ("node")
);
comment on table "host" is 'host 表';
comment on column "host"."node" is 'node name';
comment on column "host"."gpu_num" is 'node gpu_num';
comment on column "host"."type" is 'node 类型';
comment on column "host"."use" is 'node 用途';
