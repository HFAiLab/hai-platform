create table "task_tag" (
    "chain_id" varchar(255) not null,
    "user_name" varchar(255) not null,
    "tag" varchar(255) not null,
    constraint "pri-task_tag-chain_id-tag" primary key ("chain_id", "tag")
);
create index "idx-task_tag-user_name" on "task_tag" ("user_name");
create index "idx-task_tag-tag" on "task_tag" ("tag");
comment on table "task_tag" is 'task tag 表';
comment on column "task_tag"."chain_id" is 'task 的 chain_id';
comment on column "task_tag"."user_name" is 'task 的用户名';
comment on column "task_tag"."tag" is 'task tag';
