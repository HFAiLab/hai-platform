import threading
import time
import pickle
from db import redis_conn
from .data_processor import DataProcessor
from .backends import backend
from .attr_hooks import set_attr_hooks
from .utils import is_senator
from .mass import get_mass_info, record_mass
from conf.flags import PARLIAMENT_SOURCE_TYPE
from conf import CONF
from logm import logger


_registered = False


def monitor():
    backoff_exp = 0
    while True:
        try:
            for data in backend.watch():
                try:
                    DataProcessor.run(data['source'], data['data'])
                except Exception as e:
                    logger.exception(e)
                    logger.error(f'FATAL!!!! 处理数据{data} 出错: {e}')
        except Exception as e:
            logger.exception(e)
            logger.error(f'监听backend过程中出现问题: {e}，尝试重新监听', flush=True)
            time.sleep(1 << backoff_exp)
            backoff_exp = min(backoff_exp+1, 5)


def register_parliament():
    global _registered
    _registered = True
    set_attr_hooks()
    th = threading.Thread(target=monitor)
    th.daemon = True
    th.start()
    for retried_times in range(10):
        try:
            if not is_senator():  # 如果是群众要告知其它议员加入
                key_list, mass_name = get_mass_info()
                assert [key_list, mass_name] != [[], None], '群众加入会议前需要先调set_mass_info接口'
                data = {
                    'key_list': key_list,
                    'mass_name': mass_name
                }
                backend.set({'source': PARLIAMENT_SOURCE_TYPE.REGISTER_MASS, 'data': data})
                redis_conn.sadd(CONF.parliament.mass_set, pickle.dumps(data))
            else:  # 获取当前所有群众
                mass_set = redis_conn.smembers(CONF.parliament.mass_set)
                for mass in mass_set:
                    data = pickle.loads(mass)
                    try:
                        record_mass(key_list=data['key_list'], mass_name=data['mass_name'])
                    except:  # 简单兼容下以前的协议
                        pass
            break
        except Exception as e:
            logger.exception(e)
            logger.error(f'注册议会失败, 已失败 {retried_times} 次 {e}')
            time.sleep(retried_times * 10)


def withdraw_parliament(mass_name):
    data = {
        'mass_name': mass_name
    }
    backend.set({'source': PARLIAMENT_SOURCE_TYPE.CANCEL_MASS, 'data': data})
    mass_list = redis_conn.smembers(CONF.parliament.mass_set)
    for mass in mass_list:
        data = pickle.loads(mass)
        if data['mass_name'] == mass_name:
            redis_conn.srem(CONF.parliament.mass_set, mass)


def has_registered():
    return _registered
