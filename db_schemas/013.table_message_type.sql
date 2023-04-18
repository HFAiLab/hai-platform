create type "message_type" as enum ('normal', 'success', 'warning', 'danger');
create table "message" (
    "messageId" serial,
    "important" boolean not null default false,
    "type"message_type not null default 'normal',
    "title" varchar(255) not null default '',
    "content" varchar(255) not null default '',
    "detailContent" text null,
    "date" timestamp not null,
    "detailText" varchar(255) null,
    "assigned_to" varchar(255) not null,
    "expiry" timestamp default CURRENT_TIMESTAMP not null,
    constraint "pri-message-messageId" primary key ("messageId")
);
comment on table "message" is '用户前端展示的消息';
comment on column "message"."messageId" is '消息 id';
comment on column "message"."important" is '消息是否重要';
comment on column "message"."type" is '消息类型，会有不同颜色';
comment on column "message"."title" is '标题';
comment on column "message"."content" is '消息内容，不推荐写太多';
comment on column "message"."detailContent" is '详情内容，或者url';
comment on column "message"."date" is '消息日期';
comment on column "message"."detailText" is '跳转链接的标题，默认为"Detail"，或者可以写"查看详情"等';
comment on column "message"."assigned_to" is '消息发给哪个分组或者用户';
comment on column "message"."expiry" is '消息过期时间';
