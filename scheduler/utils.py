

import os
import zlib
import pickle
import pandas as pd
import multiprocessing as mp


def assign_nodes_safely(df: pd.DataFrame, index: int, assigned_nodes_list: list, memory_list: list, cpu_list: list, assigned_gpus_list: list):
    # assigned_nodes
    tmp_series = df.assigned_nodes.copy()
    tmp_series.loc[index] = assigned_nodes_list
    df.assigned_nodes = tmp_series
    # memory
    tmp_series = df.memory.copy()
    tmp_series.loc[index] = memory_list
    df.memory = tmp_series
    # cpu
    tmp_series = df.cpu.copy()
    tmp_series.loc[index] = cpu_list
    df.cpu = tmp_series
    # assigned_gpus
    tmp_series = df.assigned_gpus.copy()
    tmp_series.loc[index] = assigned_gpus_list
    df.assigned_gpus = tmp_series
    return df


def apply_process(func) -> mp.Queue:
    que = mp.Queue()
    p = mp.Process(target=lambda q: q.put(func()), args=(que, ))
    p.start()
    return que
