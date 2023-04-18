create table "unfinished_task_ng" (
    "queue_status" varchar(255) default 'queued',
    constraint "pri-unfinished_task_ng-id" primary key ("id"),
    constraint "unq-unfinished_task_ng-user_name-nb_name" unique ("user_name", "nb_name")
) inherits (task_ng);
create index "idx-unfinished_task_ng-user_name" on "unfinished_task_ng" ("user_name");
create index "idx-unfinished_task_ng-nb_name" on "unfinished_task_ng" ("nb_name");
create index "idx-unfinished_task_ng-chain_id" on "unfinished_task_ng" ("chain_id");
create index "idx-unfinished_task_ng-first_id" on "unfinished_task_ng" ("first_id");
create index "idx-unfinished_task_ng-suspend_updated_at" on "unfinished_task_ng" ("suspend_updated_at");
create index "idx-unfinished_task_ng-begin_at" on "unfinished_task_ng" ("begin_at");
create index "idx-unfinished_task_ng-end_at" on "unfinished_task_ng" ("end_at");
create index "idx-unfinished_task_ng-created_at" on "unfinished_task_ng" ("created_at");
create index "idx-unfinished_task_ng-chain_id-varchar" on "unfinished_task_ng"("chain_id" varchar_pattern_ops);
create index "idx-unfinished_task_ng-worker_status" on "unfinished_task_ng" ("worker_status");
create index "idx-unfinished_task_ng-backend" on "unfinished_task_ng" ("backend");
create index "idx-unfinished_task_ng-last_task" on "unfinished_task_ng" ("last_task");
create index "idx-unfinished_task_ng-queue_status" on "unfinished_task_ng" ("queue_status");


create or replace function insert_task()
returns trigger as $$
begin
    if new."first_id" is null then
        new."first_id" = new."id";
    end if;
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_insert_task before insert on "unfinished_task_ng" for each row execute procedure insert_task();
create trigger trigger_insert_task_ng before insert on "task_ng" for each row execute procedure insert_task();

create or replace function update_task_queue_status()
returns trigger as $$
begin
    if new."queue_status" = 'scheduled' then
        new."begin_at" = current_timestamp;
        new."end_at" = new."begin_at";
    elsif new."queue_status" = 'finished' then
        new."end_at" = current_timestamp;
        -- 可以这么判断，因为到这里的时候，pod 一定已经正确处理了
        if new."worker_status" = 'queued' then
            if new."task_type" = 'virtual' then  -- virtual任务改成stopped
                new."worker_status" = 'stopped';
            else
                new."worker_status" = 'canceled';
            end if;
        end if;
    end if;
    return new;
end;
$$ language 'plpgsql';
create trigger update_unfinished_task_ng_queue_status before update of "queue_status" on "unfinished_task_ng" for each row execute procedure update_task_queue_status();

create or replace function finish_task()
returns trigger as $$
begin
    if new."queue_status" = 'finished' then
        new."end_at" = current_timestamp;
        insert into "task_ng" (
            "id", "nb_name", "user_name", "code_file", "workspace", "group", "nodes", "assigned_nodes", "restart_count",
            "whole_life_state", "first_id", "backend", "task_type", "queue_status", "notes", "priority", "chain_id",
            "stop_code", "suspend_code", "mount_code", "suspend_updated_at", "begin_at", "end_at", "created_at", "config_json", "worker_status", "last_task"
        )
        values (
            new."id", new."nb_name", new."user_name", new."code_file", new."workspace", new."group", new."nodes",
            new."assigned_nodes", new."restart_count", new."whole_life_state", new."first_id", new."backend",
            new."task_type", new."queue_status", new."notes", new."priority", new."chain_id", new."stop_code",
            new."suspend_code", new."mount_code", new."suspend_updated_at", new."begin_at", new."end_at", new."created_at", new."config_json", new."worker_status", new."last_task"
        );
        delete from "unfinished_task_ng" where "id" = new."id";
    end if;
    return null;
end;
$$ language 'plpgsql';
create trigger trigger_finish_task after update of "queue_status" on "unfinished_task_ng" for each row execute procedure finish_task();

create or replace function update_task_suspend_at()
returns trigger as $$
begin
    if old."suspend_code" & 3 = new."suspend_code" & 3 then  -- 改的不是suspend标记位
        return new;
    end if;
    if new."suspend_code" & 3 < old."suspend_code" & 3 then  -- 不允许往小的改
        new."suspend_code" = old."suspend_code";
        return new;
    end if;
    new."suspend_updated_at" = current_timestamp;
    return new;
end;
$$ language 'plpgsql';
create trigger update_task_ng_suspend_at before update of "suspend_code" on "unfinished_task_ng" for each row execute procedure update_task_suspend_at();


create or replace function update_last_task()
returns trigger as $$
begin
    update "task_ng" set "last_task"=false where "chain_id"=new."chain_id";
    new."last_task" = true;
    return new;
end;
$$ language 'plpgsql';

create trigger trigger_update_last_task before insert on "unfinished_task_ng" for each row execute procedure update_last_task();

