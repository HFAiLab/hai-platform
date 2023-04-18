
from functools import wraps

import ujson
from prometheus_client import Summary

from db import redis_conn, a_redis
from logm import logger


def redis_cached(ttl_in_sec=None, enable_fallback=True):
    """ 需要被修饰的方法参数是 ujson 可序列化的 """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = f'redis_cache_{func.__module__}.{func.__name__}_{ujson.dumps(args)}_{ujson.dumps(kwargs)}'
            fallback_key = key + '_fallback'
            if (result := redis_conn.get(key)) is not None:
                result = ujson.loads(result)
            else:
                try:
                    result = func(*args, **kwargs)
                    redis_conn.set(key, (dumped := ujson.dumps(result)), ex=ttl_in_sec)
                    if enable_fallback: redis_conn.set(fallback_key, dumped)
                except Exception as e:
                    logger.exception(e)
                    if enable_fallback and (fallback_data := redis_conn.get(fallback_key)):
                        logger.warning(f'{func.__name__} 查询最新数据失败: {e}, 使用 redis 中的旧数据')
                        result = ujson.loads(fallback_data)
            return result
        return wrapper

    return decorator


def async_redis_cached(ttl_in_sec=None, enable_fallback=True):
    """ 需要被修饰的方法参数是 ujson 可序列化的 """
    def decorator(coro_func):
        @wraps(coro_func)
        async def wrapper(*args, **kwargs):
            key = f'redis_cache_{coro_func.__module__}.{coro_func.__name__}_{ujson.dumps(args)}_{ujson.dumps(kwargs)}'
            fallback_key = key + '_fallback'
            if (result := await a_redis.get(key)) is not None:
                result = ujson.loads(result)
            else:
                try:
                    result = await coro_func(*args, **kwargs)
                    await a_redis.set(key, (dumped := ujson.dumps(result)), ex=ttl_in_sec)
                    if enable_fallback: await a_redis.set(fallback_key, dumped)
                except Exception as e:
                    logger.exception(e)
                    if enable_fallback and (fallback_data := await a_redis.get(fallback_key)):
                        logger.warning(f'{coro_func.__name__} 查询最新数据失败: {e}, 使用 redis 中的旧数据')
                        result = ujson.loads(fallback_data)
            return result
        return wrapper

    return decorator


def record_latency(func):
    """ 记录执行时间到 prometheus 的 decorator """
    summary = Summary(f'platform_func_latency_{func.__name__}', f'execution latency of method {func.__name__}')
    return summary.time()(func)


def async_record_latency(coro_func):
    """ 记录执行时间到 prometheus 的 decorator """
    summary = Summary(f'platform_func_latency_{coro_func.__name__}', f'execution latency of method {coro_func.__name__}')
    @wraps(coro_func)
    async def wrapper(*args, **kwargs):
        with summary.time():
            return_value = await coro_func(*args, **kwargs)
        return return_value
    return wrapper
