

import pickle
from logm import logger
from conf import CONF
from db import MarsDB
import multiprocessing as mp
from scheduler.base_model import ProcessConnection
from roman_parliament import register_parliament


def import_from_str(s):
    split_modules = s.rsplit('.', 1)
    if len(split_modules) == 2:
        module = __import__(split_modules[0], fromlist=[split_modules[1]])
        return getattr(module, split_modules[1])
    else:
        return __import__(split_modules[0])


scheduler_modules = {}


if __name__ == '__main__':
    global_config_conn = ProcessConnection(init_obj={k: v for k, v in MarsDB().execute('''
        select "key", "value"
        from "multi_server_config"
        where "module" = 'scheduler'
    ''')}, dumps=pickle.dumps, loads=pickle.loads)
    MarsDB.dispose()
    scheduler_modules = {}
    # 初始化除了 monitor 以外的 modules
    for suffix in ['beater', 'assigner', 'matcher', 'feedbacker', 'subscriber']:
        for name, config in CONF.scheduler.get(suffix, {}).items():
            name = f'{name}_{suffix}'
            scheduler_modules[name] = {
                'conn': ProcessConnection(),
                'class': import_from_str(config['class']),
                'kwargs': config.get('kwargs', {})
            }
            scheduler_modules[name]['instance'] = scheduler_modules[name]['class'](
                name=name,
                conn=scheduler_modules[name]['conn'],
                global_config_conn=global_config_conn,
                **scheduler_modules[name]['kwargs']
            )
    # 指定 relation
    for relation_name, relations in CONF.scheduler.relations.items():
        for k, v in relations.items():
            for item in v:
                upstream = f"{k.split('.')[1]}_{k.split('.')[0]}"
                upstream_name = 'default'
                dot_downstream = item
                if item.find('[') > 0:
                    dot_downstream = item.split('[')[0]
                    upstream_name = item.split('[')[1].split(']')[0]
                downstream = f"{dot_downstream.split('.')[1]}_{dot_downstream.split('.')[0]}"
                scheduler_modules[downstream]['instance'].add_upstream(
                    upstream_name,
                    scheduler_modules[upstream]['conn']
                )
                logger.info(f'[{relation_name}] {upstream} -[{upstream_name}]-> {downstream}')
    # 启动
    for name, module in scheduler_modules.items():
        module['process'] = mp.Process(target=module['instance'].start, name=name)
        module['process'].start()
        logger.info(f'启动 {name}')

    # 自身化为 monitor
    register_parliament()
    monitor_name = list(CONF.scheduler.monitor.keys())[0]
    monitor_config = CONF.scheduler.monitor[monitor_name]
    monitor = import_from_str(monitor_config['class'])(
        name=monitor_name,
        conn=ProcessConnection(),
        global_config_conn=global_config_conn,
        scheduler_modules=scheduler_modules,
        **monitor_config.get('kwargs', {})
    )
    # 初始化 monitor 的 global config
    monitor.global_config = monitor.global_config_conn.get()
    logger.info(f'启动 {monitor_name}')
    monitor.start()
