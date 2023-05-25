create or replace function update_task_queue_status()
returns trigger as $$
begin
    if new."queue_status" = 'scheduled' then
        new."begin_at" = current_timestamp;
        new."end_at" = new."begin_at";
    elsif new."queue_status" = 'finished' then
        new."end_at" = current_timestamp;
        -- 可以这么判断，因为到这里的时候，pod 一定已经正确处理了
        if new."task_type" = 'virtual' then  -- virtual任务改成stopped
            new."worker_status" = 'stopped';
        elsif new."worker_status" = 'queued' then
            new."worker_status" = 'canceled';
        end if;
    end if;
    return new;
end;
$$ language 'plpgsql';