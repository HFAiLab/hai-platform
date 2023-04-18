create materialized view user_all_groups as
    select
           "user"."user_name",
           array_remove(array_cat(array["user"."role"::varchar, 'public'], array_agg(distinct "group")), null) as "user_groups"
    from "user"
    left join "user_group" on ("user"."user_name" = "user_group"."user_name" or "user"."shared_group" = "user_group"."user_name")
    group by "user"."user_name", "user"."role";

create unique index user_with_groups_user_name on "user_all_groups" ("user_name");

create or replace function update_user_with_groups_view()
returns trigger as $$
begin
    refresh materialized view user_all_groups;
    return new;
end;
$$ language 'plpgsql';

create trigger trigger_update_user_with_groups_view_on_user
    after delete or insert or update of "user_name", "role", "shared_group" on "user"
    for each statement execute procedure update_user_with_groups_view();
create trigger trigger_update_user_with_groups_view_on_user_group
    after delete or insert or update on "user_group"
    for each statement execute procedure update_user_with_groups_view();
