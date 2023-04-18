create or replace function update_worker_status_task_ng()
returns trigger as $$
declare
    rank_arr text [] := array['failed', 'stopped', 'failed_terminating', 'stopped_terminating', 'created', 'building', 'running', 'succeeded_terminating', 'succeeded', 'queued'];
    st_arr text [];
    rank_idx int;
    st_idx int;
begin
    if new."worker_status" in ('canceled', 'stopped') then  -- canceled 和 stopped 的话立刻执行就行
        return new;
    end if;
    -- 否则去 pod_ng 里找
    st_arr := array(select "status" from "pod_ng" where "task_id"=new.id);
    for rank_idx in 1..array_length(rank_arr, 1) loop
        for st_idx in 1..array_length(st_arr, 1) loop
            if st_arr[st_idx] = rank_arr[rank_idx] then
                new."worker_status" = rank_arr[rank_idx];
                return new;
            end if;
        end loop;
    end loop;
-- 理论上不会到达这里，因为之前一定能遇到一个符合条件的
    new."worker_status" = 'unknown';
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_worker_status_task_ng before update of "worker_status" on "unfinished_task_ng" for each row execute procedure update_worker_status_task_ng();


create or replace function update_worker_status()
returns trigger as $$
begin
    update "task_ng" set "worker_status"='' where "id"=new."task_id";  -- 让 task_ng 的 worker_status trigger 去操作，避免并发引起的状态异常
    return new;
end;
$$ language 'plpgsql';
create trigger trigger_update_worker_status after insert or update of "status" on "pod_ng" for each row execute procedure update_worker_status();
