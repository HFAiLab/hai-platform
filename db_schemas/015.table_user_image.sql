create table "user_image" (
    "user_name" varchar(255) not null,
    "description" varchar(255) not null,
    "image_ref" varchar(255) not null,
    "updated_at" timestamp not null default current_timestamp,
    "created_at" timestamp not null default current_timestamp,
    constraint "pri-user_image-image_ref" primary key ("image_ref")
);
create index "idx-user_image-user_name" on "user_image" ("user_name");
comment on table "user_image" is '用户可用镜像列表';
comment on column "user_image"."user_name" is '用户名';
comment on column "user_image"."description" is '镜像的描述';
comment on column "user_image"."image_ref" is '镜像名';
comment on column "user_image"."updated_at" is '更新时间';
comment on column "user_image"."created_at" is '创建时间';

create or replace function update_user_image_updated_at()
returns trigger as $$
begin
    new."updated_at" = current_timestamp;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_user_image_updated_at before update on "user_image" for each row execute procedure update_user_image_updated_at();
