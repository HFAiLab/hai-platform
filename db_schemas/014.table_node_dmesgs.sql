create table "node_dmesgs" (
    "id" serial,
    "node" varchar(255) not null,
    "start_at" timestamp not null,
    "end_at" timestamp not null,
    "total_errors" integer not null default 0,
    "new_errors" integer not null default 0,
    "last_dmesg" varchar(255) not null default '',
    constraint "pri-node_dmesgs-id" primary key ("id")
);
comment on table "node_dmesgs" is '节点 dmesgs 表';
comment on column "node_dmesgs"."id" is '消息 id';
comment on column "node_dmesgs"."node" is '节点';
comment on column "node_dmesgs"."start_at" is 'start_at';
comment on column "node_dmesgs"."end_at" is 'end_at';
comment on column "node_dmesgs"."total_errors" is '总错误条数';
comment on column "node_dmesgs"."new_errors" is '新增错误条数';
comment on column "node_dmesgs"."last_dmesg" is '上一条 dmesg';
