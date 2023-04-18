create table public.train_image
(
    image_tar varchar not null,
	image varchar default ''::character varying not null,
	path varchar default ''::character varying not null,
	shared_group varchar,
	registry varchar default 'registry.high-flyer.cn'::character varying,
	status varchar default 'processing'::character varying,
	task_id integer default 0,
	created_at timestamp not null default current_timestamp,
	updated_at timestamp not null default current_timestamp
);

comment on table public.train_image is '训练用镜像';

comment on column public.train_image.image is '镜像名字';

comment on column public.train_image.path is '镜像在 weka 上的路径';

comment on column public.train_image.shared_group is '哪个 group 可以共享这个镜像';

alter table public.train_image owner to root;

create unique index train_image_image_uindex
	on public.train_image (image_tar);

create or replace function update_train_image_updated_at()
returns trigger as $$
begin
    new."updated_at" = current_timestamp;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_train_image_updated_at before update on "train_image" for each row execute procedure update_train_image_updated_at();
