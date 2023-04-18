create table "multi_server_config" (
    "module" varchar(255) not null,
    "key" varchar(255) not null,
    "value" jsonb not null,
    "notes" text default '',
    constraint "pri-multi_server_config-key" primary key ("key", "module")
);
comment on table "multi_server_config" is '平台配置表';
comment on column "multi_server_config"."module" is '配置属于哪个组件';
comment on column "multi_server_config"."key" is '配置 key';
comment on column "multi_server_config"."value" is '配置 value';
comment on column "multi_server_config"."notes" is '配置说明';
