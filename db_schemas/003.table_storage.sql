create table "storage" (
    "host_path" varchar(255) not null,
    "mount_path" varchar(255) not null,
    "owners" varchar[] not null,
    "conditions" varchar[] not null default array[]::varchar[],
    "mount_type" varchar(64)  not null default '',
    "read_only" boolean not null default true,
    "action" varchar(64) not null,
    "active" boolean not null default true
);
create index "idx-host_path-storage" on "storage" ("host_path");
create index "idx-mount_path-storage" on "storage" ("mount_path");
comment on table "storage" is '用户存储表';
comment on column "storage"."owners" is '用户、用户组的集合';
comment on column "storage"."host_path" is '挂载点的 host_path';
comment on column "storage"."mount_path" is '挂载点的 mount_path';
comment on column "storage"."mount_type" is '挂载类型';
comment on column "storage"."read_only" is '是否为只读挂载';
comment on column "storage"."conditions" is '挂载条件';
comment on column "storage"."action" is '暂定只有 add / remove';
comment on column "storage"."active" is '是否有效';
