import os
import re

import aiofiles
import ciso8601


async def get_timestamp_from_line(line):
    if len(line) <= 28:
        raise Exception()
    return ciso8601.parse_datetime(line[1:27].decode())


async def file_read_all(fp, timestamp, offset, mtime):
    await fp.seek(offset, 0)
    data = await fp.read()
    leng = len(data)
    for i in range(leng - 1, -1, -1):
        if data[i] == ord('[') and (i == 0 or data[i - 1] == ord('\n')):  # 避免找到一行中间的时间戳
            try:
                line_timestamp = await get_timestamp_from_line(data[i:])
                if timestamp and line_timestamp <= timestamp:  # 理论上不会出现这种情况，兜个底
                    return {
                        "data": "",
                        "success": 1,
                        "last_seen": {
                            "timestamp": timestamp,
                            "offset": offset,
                            "mtime": mtime
                        }
                    }
                return {
                    "data": data.decode(errors='replace'),  # 如果无法被decode，说明日志烂了
                    "success": 1,
                    "last_seen": {
                        "timestamp": line_timestamp,
                        "offset": i + offset,
                        "mtime": mtime
                    }
                }
            except:  # 无法被解析成timestamp
                pass
    return {  # 没有找到时间戳，比较尴尬，理论上到达这里意味着没有更多的数据
        "data": "",
        "success": 1,
        "last_seen": {
            "timestamp": timestamp,
            "offset": offset,
            "mtime": mtime
        }
    }


async def get_file_log(path, last_seen):
    # 判断是否要跳过该文件，如果该文件更新时间比上次看到的最晚更新时间要少，说明没必要再看
    mtime = os.path.getmtime(path)
    if last_seen and 'mtime' in last_seen and mtime < last_seen['mtime']:
        return {
            "data": "",
            "success": 1,
            "last_seen": last_seen
        }
    async with aiofiles.open(path, "rb") as fp:
        if not last_seen:
            return await file_read_all(fp, None, 0, mtime)
        # 先判断能否match上
        offset = last_seen['offset']
        timestamp = last_seen['timestamp']
        await fp.seek(offset, 0)
        line = await fp.readline()
        try:
            line_timestamp = await get_timestamp_from_line(line)
            if not timestamp == line_timestamp:
                raise Exception()
            # match上了，下一行就是要开始读取的数据
            return await file_read_all(fp, timestamp, await fp.tell(), mtime)
        except:
            pass
        # 然后判断是否需要全部输出
        await fp.seek(0, 0)
        line = await fp.readline()
        try:
            line_timestamp = await get_timestamp_from_line(line)
            if timestamp < line_timestamp:
                return await file_read_all(fp, timestamp, 0, mtime)
        except:
            # 存在一次试错机会，因为一行可能被分到两个文件中，第二行理应是正确的
            try:
                current_idx = await fp.tell()
                line = await fp.readline()
                line_timestamp = await get_timestamp_from_line(line)
                if timestamp < line_timestamp:
                    return await file_read_all(fp, timestamp, current_idx, mtime)
            except Exception as e:  # 第二次再失败，可能是因为输出的一行太长打爆导致的
                pass
        # 兜底，理论上不会到达这一段
        return await file_read_all(fp, timestamp, 0, mtime)


def check_file_match(file_name: str, idx: int):
    return f'#{idx}.' in file_name or file_name.endswith(f'#{idx}')


async def get_task_node_idx_log(task_id, user, node_idx: int, last_seen=None, suffix_filter=None, max_line_length=4096):
    """
    :param task_id:
    :param user:
    :param node_idx:
    :param last_seen:
    :param suffix_filter:
    :return:
    """
    log_dir = user.config.log_dir()
    path = os.path.join(log_dir, str(task_id))
    try:
        files = os.listdir(path)
        valid_files = [(file, os.path.getmtime(os.path.join(path, file)))
                       for file in files
                       if check_file_match(file, node_idx) and not file.endswith('error') \
                            and not file.startswith('events') and not file.startswith('debug') \
                            and (suffix_filter is None or
                                 re.search(re.escape(suffix_filter)+r'(\.\d+)*$', file) is not None)
                       ]
        sorted_valid_files = sorted(valid_files, key=lambda t: t[1])
        for file in files:
            if check_file_match(file, node_idx) and file.endswith('error'):
                sorted_valid_files.append((file, 0))
        data = []
        rst_last_seen = last_seen
        for file in sorted_valid_files:
            info = await get_file_log(os.path.join(path, file[0]), last_seen)
            if info['last_seen'] and info['last_seen']['timestamp']:
                if not rst_last_seen or info['last_seen']['timestamp'] > rst_last_seen['timestamp']:
                    rst_last_seen = info['last_seen']
            data.append(info['data'])
        data = "".join(data)
        cut_log = []
        for line_log in data.split('\n'):
            if len(line_log) > max_line_length and line_log[29:40] != '[HFAI_PRINT':
                line_log = line_log[0:max_line_length] + f'...(日志长度超过 {max_line_length}，已被截断)'
            cut_log.append(line_log)
        return {
            "data": '\n'.join(cut_log) if cut_log else "还没产生日志",
            "success": 1,
            "msg": "get log successfully",
            "last_seen": rst_last_seen
        }
    except Exception as exp:  # 共享盘里文件还没创建的情况
        if os.path.exists(path):  # path 存在的情况下，应该是出了异常了
            raise Exception
        return {
            "data": "还没产生日志",
            "success": 1,
            "msg": f'{task_id} 找不到相应日志, node_idx: {node_idx}, 错误编号: {exp}',
            "last_seen": None
        }

__all__ = ['get_task_node_idx_log']
