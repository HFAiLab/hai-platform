import os
import sys
from functools import wraps, partial

from loguru import logger
from loguru._logger import context as log_context

import fetion
from conf.flags import TASK_PRIORITY

logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "{extra[uuid]} | <level>{message}</level>"
)
logger.configure(extra={"uuid": ""})
logger.remove()
logger.add(sys.stdout, format=logger_format, level=os.environ.get('LOGM_LV', "INFO"))


def bind_logger_task(task):
    """
    把当前的logger和task进行绑定，fetion时会根据task发送到对应的群中，一般只在manager中进行绑定
    :param task:
    :return:
    """
    for func in ['error', 'warning', 'info', 'debug']:
        setattr(logger, f'f_{func}', partial(getattr(logger, f'f_{func}'), task=task))


def log_stage(stage):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with logger.contextualize(uuid=f'{stage}.{func.__name__}'):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def fetion_log(f):
    def log(*args, **kwargs):
        """
        不指定 fetion = False 那么会发送 fetion
        :param args:
        :param kwargs:
        :return:
        """
        if kwargs.pop('fetion', True):
            # 添加日志上下文
            context_dict = log_context.get()
            uuid_var = ''
            if context_dict:
                uuid_var = ';'.join(
                    [f'{context_dict[k]}' for k in context_dict])
                uuid_var += ' | '
            task = kwargs.pop('task', None)
            task_info = '' if task is None else f'[{task.user_name}][{task.nb_name}][{task.id}][{TASK_PRIORITY.value_key_map().get(task.priority, "AUTO")}] | '
            rst = fetion.Fetion.alert(f'{uuid_var}{task_info}{args[0]}', task=task, **kwargs)
            if rst is not None:
                logger.info(rst)
        f(*args, **kwargs)

    return log


for func in ['error', 'warning', 'info', 'debug']:
    setattr(logger, f'f_{func}', fetion_log(getattr(logger, func)))


class ExceptionWithoutErrorLog(Exception):
    pass


def __log_exception_with_filter(exception):
    """ 用于打印异常 stack trace 时忽略一些不想输出 ERROR 级别日志的异常, 比如因用户传错参数导致的校验失败等 """
    if not isinstance(exception, ExceptionWithoutErrorLog):
        logger.__vanilla_exception(exception)


logger.__vanilla_exception = logger.exception
logger.exception = __log_exception_with_filter
