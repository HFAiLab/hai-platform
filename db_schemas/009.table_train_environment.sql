create table "train_environment" (
    "env_name" varchar(255) not null,
    "image" varchar(255) not null,
    "schema_template" varchar(2047) not null,
    "config" jsonb not null default '{}',
    constraint "pri-train_environment-env_name" primary key ("env_name")
);
comment on table "train_environment" is '训练用的环境，含image/template';
comment on column "train_environment"."env_name" is 'environment 的名字';
comment on column "train_environment"."image" is '训练容器';
comment on column "train_environment"."schema_template" is '训练用的模板';
