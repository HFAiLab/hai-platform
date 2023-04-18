

from .default import *
from .custom import *
import pickle
import pandas as pd
from k8s import get_corev1_api
from logm import logger
from utils import asyncwrap
from db import a_redis
from k8s_watcher.node_watcher import NODES_DF_COLUMNS

k8s_corev1_api = get_corev1_api()


async def async_get_nodes_df(monitor=False):
    nodes_df_pickle = await a_redis.get('nodes_df_pickle')
    nodes_df = pd.DataFrame(columns=NODES_DF_COLUMNS) if nodes_df_pickle is None else pickle.loads(nodes_df_pickle)
    if len(nodes_df) == 0:
        return nodes_df
    if monitor:
        await monitor_info(nodes_df)
    return nodes_df


async def async_set_node_label(node: str, key: str, value: str):
    try:
        async_func = asyncwrap(k8s_corev1_api.patch_node_with_retry)
        await async_func(node, body=dict(metadata=dict(labels={key: value})), _request_timeout=5)
        return True
    except Exception as e:
        logger.exception(e)
        return False


async def async_read_node(node: str):
    async_func = asyncwrap(k8s_corev1_api.read_node)
    return await async_func(node)
