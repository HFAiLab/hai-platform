create table "external_quota_change_log"
(
	"editor" varchar not null,
	"external_user" varchar not null,
	"resource" varchar not null,
	"original_quota" integer not null default 0,
	"quota" integer not null,
	"inserted_at" timestamp default CURRENT_TIMESTAMP,
	"expire_time" timestamp default null
);
comment on table "external_quota_change_log" is '修改外部用户 quota 的时候，保存用户操作的逻辑';
comment on column "external_quota_change_log"."editor" is '修改者';
comment on column "external_quota_change_log"."external_user" is '外部用户';
comment on column "external_quota_change_log"."resource" is '修改的目标';
comment on column "external_quota_change_log"."original_quota" is '原始的quota';
comment on column "external_quota_change_log"."quota" is '修改的quota';
comment on column "external_quota_change_log"."inserted_at" is '修改时间';
