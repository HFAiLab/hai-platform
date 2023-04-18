create table "storage_monitor_dir" (
    "type" varchar(255) not null,
    "host_path" varchar(2047) not null,
    "tag" varchar(255) not null default '',
    "user_name" varchar(255) not null default '',
    "inode_id" decimal(30, 0) default null,
    constraint "pri-storage_monitor_dir-type-path" primary key ("type", "host_path"),
    constraint "storage_monitor_dir-check-weka-inode_id" CHECK ( not ("type" = 'weka' and "inode_id" is null)  )
);
comment on table "storage_monitor_dir" is '配置要监控用量的目录的表';
comment on column "storage_monitor_dir"."type" is '目录属于哪一个文件系统';
comment on column "storage_monitor_dir"."host_path" is '目录路径';
comment on column "storage_monitor_dir"."tag" is '给 path 打一个标签';
comment on column "storage_monitor_dir"."user_name" is 'path 所属用户';
comment on column "storage_monitor_dir"."inode_id" is '用于 weka 目录, path 的 inode id';