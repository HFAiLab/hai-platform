alter table "host"
add column "flags" varchar[] not null default array[]::varchar[];

comment on column "host"."flags" is '节点的 flags';
