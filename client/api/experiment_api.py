# 萤火2号 api
import os
from abc import ABC
from io import StringIO
from typing import Tuple, List, Union
import datetime

import munch
from hfai.base_model.base_task import BasePod
from hfai.base_model.training_task import TrainingTask, ITrainingTaskImpl
# client
from hfai.conf.flags import STATUS_COLOR_MAP, EXP_PRIORITY, TASK_TYPE
from rich import box
from rich.table import Table
import urllib

from .api_config import get_mars_token as mars_token
from .api_config import get_mars_url as mars_url
from .api_utils import async_requests, RequestMethod


# ==============================================================================
class Experiment(TrainingTask):
    """
    任务类

    包含如下属性：

    - id (int): 任务 id
    - nb_name (str): 任务名
    - user_name (str): 用户名
    - code_file (str): 训练任务代码的路径
    - workspace (str): 训练任务代码的 workspace
    - config_json (dict): 任务的配置信息，包括：priority (`int`)，environment (`dict[str, str]`)，whole_life_state (`int`)
    - group (str): 任务所在组
    - nodes (int): 任务占用节点数量
    - assigned_nodes (list[str]): 分配的节点
    - whole_life_state (int): 当前设置的 whole_life_state
    - star(bool): 是否是星标任务
    - first_id (int): 整个 chain_id 中最小的 id
    - backend (str): 任务所在环境
    - task_type (str): 任务类型
    - queue_status (str): 任务当前运行状态
    - priority (int): 任务当前的优先级
    - chain_id (str): 任务 chain_id
    - stop_code (int): 任务退出情况
    - worker_status (str): 任务结束时的状态
    - begin_at (str): 任务开始时间
    - end_at (str): 任务结束时间
    - created_at (str): 任务创建时间
    - id_list (list[int]): 整个 chain_id 的所有 id
    - begin_at_list (list[str]): 整个 chain_id 所有 id 的启动时间
    - end_at_list (list[str]): 整个 chain_id 所有 id 的结束时间
    - stop_code_list (list[int]): 整个 chain_id 所有 id 的退出情况
    - whole_life_state_list (list[int]): 整个 chain_id 所有 id 的最新 whole_life_state
    - _pods_ (list[Pod]): 该任务每个 pod 的各项参数


    Examples:

    .. code-block:: python

        from hfai.client import get_experiment
        import asyncio
        experiment: Experiment = asyncio.run(get_experiment(id=1))
        log = asyncio.run(experiment.log_ng(rank=0))  # 获取 rank0 的日志
        asyncio.run(experiment.stop())  # 结束该任务

    """

    experiment_columns = [
        'id', 'nb_name', 'nodes', 'chain_status', 'task_type', 'suspend_count', 'created_at'
    ]

    def __init__(self, implement_cls=None, **kwargs):
        super(Experiment, self).__init__(implement_cls, **kwargs)
        self.suspend_count = self.restart_count
        self._pods_ = [BasePod(**pod) for pod in self._pods_]
        self.last_seen = None

    async def set_priority(self, priority: int, **kwargs):
        return await self.update(('priority', ), (priority, ))

    async def rerun(self, *args, **kwargs):
        task = self
        token = kwargs.get('token', mars_token())
        url = f'{mars_url()}/operating/rerun_task?token={token}&chain_id={task.chain_id}'
        result = await async_requests(RequestMethod.POST, url, [1, 2])
        return Experiment(ExperimentImpl, **result['task'])

    def row(self):
        values = []
        for k in self.experiment_columns:
            v = self.__getattribute__(k)
            color = STATUS_COLOR_MAP.get(v, 'white')
            values.append(f'[{color}]{v}[/{color}]')
        return values

    def tables(self):
        experiment_table = Table(show_header=True, box=box.ASCII_DOUBLE_HEAD)
        for k in self.experiment_columns:
            experiment_table.add_column(k)
        experiment_table.add_row(*self.row())

        job_table = Table(show_header=True, box=box.ASCII_DOUBLE_HEAD)
        job_columns = ['rank', 'status', 'node', 'begin_at']
        for k in job_columns:
            job_table.add_column(k)
        for p in self.pods:
            values = []
            if isinstance(p, dict):
                p = BasePod(**p)
            p.rank = p.job_id
            for c in job_columns:
                v = p.__getattribute__(c)
                color = STATUS_COLOR_MAP.get(v, 'white')
                values.append(f'[{color}]{v}[/{color}]')
            job_table.add_row(*values)

        return experiment_table, job_table


class ExperimentImpl(ITrainingTaskImpl, ABC):

    def select_pods(self, *args, **kwargs):
        # pods 应该是 server 端直接返回的，所以这里是不需要的
        pass
    async def update(self, fields: Tuple[str], values: Tuple, *args, **kwargs):
        assert fields[0] == 'priority', 'client 只能更新 priority'
        task = self.task
        token = kwargs.get('token', mars_token())
        url = f'{mars_url()}/operating/task/priority/update?token={token}&chain_id={task.chain_id}&priority={values[0]}'
        await async_requests(RequestMethod.POST, url, [1, 2])
        return True

    async def stop(self, op='stop', *args, **kwargs):
        task = self.task
        token = kwargs.get('token', mars_token())
        url = f'{mars_url()}/operating/task/stop?token={token}&chain_id={task.chain_id}&op={op}'
        await async_requests(RequestMethod.POST, url, [1, 2])
        return True

    async def suspend(self, restart_delay: int = 0, *args, **kwargs):
        task = self.task
        token = kwargs.get('token', mars_token())
        url = f'{mars_url()}/operating/task/suspend?token={token}&chain_id={task.chain_id}&restart_delay={restart_delay}'
        await async_requests(RequestMethod.POST, url, [1])
        return True

    async def log(self, rank: int = 0, last_seen: str = 'null', with_code=False, *args, **kwargs):
        """
        查看日志, 获取日志的时候，同样会返回状态，这样就可以一同刷新掉了
        @param rank:
        @param last_seen:
        @param with_code: True, 返回 (log, exit_code, stop_code) 这个 tuple
        @return:
        """
        task = self.task
        token = kwargs.get('token', mars_token())
        url = f'{mars_url()}/query/task/log?token={token}&chain_id={task.chain_id}&rank={rank}&last_seen={last_seen}'
        res = await async_requests(RequestMethod.POST, url, [1])
        self.task.last_seen = res['last_seen']
        if with_code:
            return res['data'], res["exit_code"], res["stop_code"]
        else:
            return res['data']

    async def log_ng(self, rank: int = 0, last_seen: str = 'null', *args, **kwargs):
        """
         查看日志, 获取日志的时候，同样会返回状态，这样就可以一同刷新掉了
        @param rank:
        @param last_seen:
        @return:
        """
        task = self.task
        token = kwargs.get('token', mars_token())
        url = f'{mars_url()}/query/task/log?token={token}&chain_id={task.chain_id}&rank={rank}&last_seen={last_seen}'
        res = await async_requests(RequestMethod.POST, url, [1])
        self.task.last_seen = res['last_seen']
        return res

    async def sys_log(self, *args, **kwargs):
        """
        查看系统错误日志
        :return:
        """
        task = self.task
        token = kwargs.get('token', mars_token())
        url = f'{mars_url()}/query/task/sys_log?token={token}&chain_id={task.chain_id}'
        res = await async_requests(RequestMethod.POST, url, [1])
        return res['data']

    async def search_in_global(self, content, *args, **kwargs):
        """
        全局搜索该任务每个rank包含content的次数
        :param content:
        :param args:
        :param kwargs:
        :return: 返回一个list，表示每个rank包含content的次数
        """
        task = self.task
        token = kwargs.get('token', mars_token())
        url = f'{mars_url()}/query/task/log/search?token={token}&chain_id={task.chain_id}&{urllib.parse.urlencode({"content": content})}'
        res = await async_requests(RequestMethod.POST, url, [1])
        return res['data']

    async def tag_task(self, tag: str, *args, **kwargs):
        """
        给当前任务添加标签
        :param tag:
        :param args:
        :param kwargs:
        :return:
        """
        task = self.task
        token = kwargs.get('token', mars_token())
        url = f'{mars_url()}/operating/task/tag?token={token}&chain_id={task.chain_id}&tag={tag}'
        res = await async_requests(RequestMethod.POST, url, [1])
        return res['msg']

    async def untag_task(self, tag: str, *args, **kwargs):
        """
        给当前任务删除标签
        :param tag:
        :param args:
        :param kwargs:
        :return:
        """
        task = self.task
        token = kwargs.get('token', mars_token())
        url = f'{mars_url()}/operating/task/untag?token={token}&chain_id={task.chain_id}&tag={tag}'
        res = await async_requests(RequestMethod.POST, url, [1])
        return res['msg']

    async def get_latest_point(self, *args, **kwargs):  # get_experiment_perf_current
        """获取任务当前的性能监控"""
        task = self.task
        token = kwargs.get('token', mars_token)
        url = f'{mars_url()}/monitor_v2/task_perf_api?token={token}&chain_id={task.chain_id}'
        result = await async_requests(RequestMethod.POST, url)
        return result['data']

    async def get_chain_time_series(self, query_type: str, rank: int = None, *args, **kwargs):
        """获取整条chain的时序性能数据"""
        task = self.task
        data_interval = kwargs.get('data_interval', '5min')
        assert query_type in ('gpu', 'cpu', 'mem', 'every_card', 'every_card_mem')
        token = kwargs.get('token', mars_token)
        url = f'{mars_url()}/monitor/task/chain_perf_series?token={token}&chain_id={task.chain_id}&typ={query_type}&rank={rank}&data_interval={data_interval}'
        result = await async_requests(RequestMethod.POST, url)
        return result['data']


# ==============================================================================
async def get_experiments(
        page: int,
        page_size: int,
        only_star=False,
        select_pods=True,
        nb_name_pattern=None,
        task_type_list=['training', 'virtual', 'background'],
        worker_status_list=[],
        queue_status_list=[],
        tag_list=[],
        **kwargs
):
    """
    获取自己最近提交的任务

    Args:
        page (int): 第几页
        page_size (int): 每一页的任务个数
        only_star (bool): 只考虑 ``star`` 的任务（默认为 ``False``）
        select_pods(bool): 是否查询 pod
        nb_name_pattern (str): 查询 nb_name 带有这个字符串的任务
        task_type_list (list[str]): 查询 task_type，默认拿 training 和 validation
        worker_status_list (list[str]): 查询 worker_status
        queue_status_list (list[str]): 查询 queue_status
        tag_list: 查询 tag

    Returns:
         int, list[Experiment]: 符合条件的任务总数，返回的任务列表

    Examples:

        >>> from hfai.client import get_experiments
        >>> import asyncio
        >>> asyncio.run(get_experiments(page=1, page_size=10))  # python3.8以下可能不支持asyncio.run的用法，需要用其它异步调用接口

    """
    token = kwargs.get('token', mars_token())
    url = f'{mars_url()}/query/task/list?page={page}&page_size={page_size}&token={token}'
    for task_type in task_type_list:
        url += f'&task_type={task_type}'
    if nb_name_pattern is not None:
        url += f'&nb_name_pattern={nb_name_pattern}'
    for worker_status in worker_status_list:
        url += f'&worker_status={worker_status}'
    for queue_status in queue_status_list:
        url += f'&queue_status={queue_status}'
    if only_star:
        tag_list.append('start')
    for tag in tag_list:
        url += f'&tag={tag}'
    url += f'&select_pods={select_pods}'
    result = (await async_requests(RequestMethod.POST, url))['result']

    total = result['total']
    tasks = result['tasks']

    return total, [Experiment(ExperimentImpl, **t) for t in tasks]


async def get_experiment(name: str = None, id: int = None, chain_id: str = None, **kwargs):
    """
    通过 name、id 或 chain_id 获取训练任务，不能都为空，只能获取自己的任务

    Args:
        name (str): 任务名
        id (int): 任务 id
        chain_id (str): 任务 chain_id

    Returns:
         Experiment: 返回的任务

    Examples:

        >>> from hfai.client import get_experiment
        >>> import asyncio
        >>> asyncio.run(get_experiment(id=1))  # python3.8以下可能不支持asyncio.run的用法，需要用其它异步调用接口

    """
    nb_name = name
    assert not(nb_name is None and id is None and chain_id is None), '必须设置一个 nb_name/id/chain_id'
    token = kwargs.get('token', mars_token())
    url = f'{mars_url()}/query/task?token={token}&'
    if id is not None:
        url += f'id={id}'
    elif nb_name is not None:
        url += f'nb_name={nb_name}'
    else:  # chain_id is not None:
        url += f'chain_id={chain_id}'

    result = await async_requests(RequestMethod.POST, url)
    return Experiment(ExperimentImpl, **result['result']['task'])


async def create_experiment(config: Union[str, StringIO, munch.Munch], **kwargs) -> Experiment:
    """
    根据 v2 配置文件创建任务

    配置文件示例:

    .. code-block:: yaml

        version: 2
        name: test_create_experiment
        priority: 20 # 可选，内部用户 50 40 30 20, 外部用户 0, 不填为 -1
        spec: # 任务定义，根据定义，将在集群上做下面的运行
          # cd /xxx/xxx; YOUR_ENV_KEY=YOUR_ENV_KEY python xxx.py --config config
          workspace: /xxx/xxx               # 必填
          entrypoint: xxx.py                # 必填, 若 entrypoint_binary 为 False 或者不填，那么支持 .py 或者 .sh, .sh 则使用 bash xxx.sh 运行；
                                            #      若 entrypoint_binary 为 True，那么认为 entrypoint 是可执行文件，直接使用 <entrypoint> 运行
          parameters: --config config       # 可选
          environments:                     # 可选
            YOUR_ENV_KEY: YOUR_ENV_VALUE
          entrypoint_executable: False      # 可选，不填则默认为 False，若为 True，那么认为 entrypoint 是可执行文件
        resource:
          image: registry.high-flyer.cn/hfai/docker_ubuntu2004:20220630.2   # 可选，不指定，默认 default，通过 hfai 上传的 image，或者集群内建的 template
          group: jd_a100#heavy                                              # 可选, jd_a100, jd_a100#heavy, jd_a100#light, jd_a100#A, jd_a100#B
          node_count: 1                                                     # 必填
        options: # 可选
          whole_life_state: 1   # hfai.get_whole_life_state() => 1
          mount_code: 2         # use 3fs prod mount
          py_venv: 202111 # 会在运行脚本前，source 一下 python 环境，根据输入不同选择 hf_env 或 hfai_env。
                          # 分为两类：1. 202111 => source haienv 202111; 2.1 hfai_env_name[hfai_env_owner] => source haienv hfai_env_name -u hfai_env_owner
                          #                                            2.2 hfai_env_name => source haienv hfai_env_name
                          # hf_env 可选: 202105, 202111, 202207, 其中202111会根据镜像选择py3.6或者py3.8
          override_node_resource: # 覆盖默认的resource选项
            cpu: 0
            memory: 0

    Args:
        config (str, StringIO, munch.Munch): 配置路径，yaml 的 string，或 Munch

    Returns:
        Experiment: 生成的任务

    Examples:

    .. code-block:: python

        from hfai.client import create_experiment
        import asyncio
        asyncio.run(create_experiment('config/path'))  # python3.8以下可能不支持asyncio.run的用法，需要用其它异步调用接口

        await create_experiment('''
                version: 2
                name: test_create_experiment
                priority: 20
                ... yaml file
        ''')

    """
    if isinstance(config, str):
        config_file = os.path.expanduser(config)
        if os.path.exists(config_file):
            config = munch.Munch.fromYAML(open(config_file))
        else:
            config = munch.Munch.fromYAML(StringIO(config))
    elif isinstance(config, StringIO):
        config = munch.Munch.fromYAML(config)
    elif isinstance(config, munch.Munch):
        config = config
    else:
        assert 0, '非法输入'

    # 校验, 这边就做最简单的检查
    def check_exist(key):
        value = config.copy()
        ks = key.split('.')
        for _k in ks[:-1]:
            value = value.get(_k, {})
        assert value.get(ks[-1], None) is not None, f'配置项 {key} 必须存在'
    keys = ['version', 'name']
    for k in keys:
        check_exist(k)

    assert int(config.version) == 2, '版本出错，v2 接口需要 v2 配置文件'
    assert len(config.name) <= 511, f'name 长度不应超过 511'

    # 检查 profile 参数
    profile = config.get('options', {}).get('profile')
    if profile:
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        profile['log_dir'] = '${MARSV2_LOG_DIR}/haiprof/' + now
        seconds = profile.get('time', 0)
        assert isinstance(seconds, int) and seconds >= 0, "profile 的时间必须是 >= 0 的整数"
        for k in profile.get('interval', {}):
            interval = profile['interval'][k]
            assert isinstance(interval, int) and interval >= 1, "采样周期必须是 >= 1 的整数"

    token = kwargs.get('token', mars_token())
    result = await async_requests(RequestMethod.POST, url=f'{mars_url()}/operating/task/create?token={token}',
                                  assert_success=[1, 2], json=config.__dict__)
    return Experiment(ExperimentImpl, **result['task'])


async def _post_validate(url):
    result = await async_requests(RequestMethod.POST, url)
    if result['created']:
        return {
            'success': 1,
            'msg': f"{result['msg']}，任务编号: {result['task']['id']}"
        }
    else:
        return {
            'success': 0,
            'msg': result['msg']
        }


async def validate_nodes(nodes: List[str], file: str = '/marsv2/scripts/validation/validate.sh', backend: str = 'cuda_11', **kwargs):
    token = kwargs.get('token', mars_token())
    url = f'{mars_url()}/operating/node/validate?token={token}&file={file}&backend={backend}&nodes={",".join(nodes)}'
    job_table = await _post_validate(url)
    return job_table


async def validate_experiment(name: str = None, id: int = None, chain_id: str = None, ranks: Tuple = (), file: str = '/marsv2/scripts/validation/validate.sh', backend: str = 'cuda_11', **kwargs):
    nb_name = name
    assert not(nb_name is None and id is None and chain_id is None), '必须设置一个 nb_name/id/chain_id'
    all_rank = any([rank == 'all' for rank in ranks]) or not ranks
    if not all_rank:
        for rank in ranks:
            assert isinstance(rank, int) or rank.isnumeric(), '输入的rank必须是个整数'
        chosen_ranks = ','.join([str(rank) for rank in ranks])
    else:
        chosen_ranks = 'all'
    token = kwargs.get('token', mars_token())
    url = f'{mars_url()}/operating/task/validate?chosen_ranks={chosen_ranks}&token={token}&file={file}&backend={backend}&'
    if id is not None:
        url += f'id={id}'
    elif nb_name is not None:
        url += f'nb_name={nb_name}'
    else:  # chain_id is not None:
        url += f'chain_id={chain_id}'
    job_table = await _post_validate(url)
    return job_table


async def get_task_container_log(id, rank, **kwargs):
    token = kwargs.get('token', mars_token())
    url = f'{mars_url()}/query/task/container_log?id={id}&token={token}&rank={rank}'
    result = await async_requests(RequestMethod.POST, url=url, assert_success=[1])
    return result['result']['data']
