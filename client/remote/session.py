import datetime
import importlib
import os
import os.path as path
import shlex
import subprocess
import sys
import types
import uuid
from contextlib import contextmanager
from typing import Tuple, List

from .data_package import save, load
from .multiprocess import AsyncResult, Pool


class GlobalValuesRecorder:
    def __init__(self, main_globals):
        self.main_globals = main_globals
        self.enter_globals_keys = []
        self.global_keys = []

    @property
    def global_values(self):
        return {k: self.main_globals[k] for k in self.global_keys if k in self.main_globals}

    def enter_record(self):
        self.enter_globals_keys = list(self.main_globals.keys())

    def exit_record(self, *modules):
        for k in self.main_globals:
            if k not in self.enter_globals_keys:
                self.global_keys.append(k)
        self.update(*modules)

    @contextmanager
    def record(self, *modules):
        try:
            self.enter_record()
            yield
        finally:
            self.exit_record(*modules)

    def update(self, *modules):
        for module in modules:
            # 不管这个 item 是不是在 module 中，都设置上去
            global_values = self.global_values
            for k in global_values:
                setattr(module, k, global_values[k])


class SessionConfig:
    """
    配置Session，本地还是远程，远程用哪个分组

    Args:
        local: 本地运行, 默认 True
        inline: 在本地运行时候，不使用 python module.py 的模式，默认 True
        group: 远程运行时候提交的分组, 默认 jd_dev_alpha
        nb_auto_reload: 在运行 cell 之前自动 reload module
        process_limit: 后台运行的子进程数量, 0 表示和 cpu_count 一样
        priority: 以什么优先级来提交任务，不设置则用最高优先级运行
        image: 使用什么镜像运行，不填则用用户默认的镜像
    """
    def __init__(self, local=True, inline=True, group='jd_dev_alpha', nb_auto_reload=True,
                 process_limit=0, priority=50, image=None):
        self.local = local
        self.inline = True
        self.group = group
        self.nb_auto_reload = nb_auto_reload
        self.process_limit = process_limit
        self.priority = priority
        self.image = image

    @property
    def dict(self):
        return {k: getattr(self, k) for k in ['local', 'inline', 'group', 'auto_reload_interval', 'process_limit']}


class GlobalSession:
    """
    远程运行的Session

    在主进程构造一个 session，用于管理用户开发的 modules，并且，比如设置 modules 中的 global values
    逻辑上和 multiprocess.pool 有点接近，可以以此类别，帮助理解
    详细配置见 SessionConfig

    Examples:

    .. code-block:: python

        # demo.py
        foo = 'bar'
        def func1(param1, param2):
            print(foo, param1, param2)
            return f'{foo}_{param1}_{param2}'

    .. code-block:: python

        # main.ipynb
        # 构造 session 来运行
        import demo as demo2
        session = GlobalSession(globals(), (demo2, ), session_config=SessionConfig())
        with session.modules_globals():
            foo = 1
        session.apply(demo2.func1, (1, ), {'param2': 'b'}) # 使用 foo = 1 且 demo 修改了会 自动 reload
        session.apply(demo2.func1, (1, ), {'param2': 'b'}, local=False) # 远程运行

        # 在 local + inline 运行模式中，等价与：
        importlib.reload(demo2)
        session.update_values(demo2)
        demo2.func1('a', 'b')

    """

    def __init__(self, main_globals, modules: Tuple, session_config: SessionConfig = None):
        """
        构造 GlobalSession，一般而言，一个入口只有一个 session

        Args:
            main_globals: 构造函数传入 `globals()`
            modules: 注册需要 remote call 的 module 列表，用以之后 module.func; 注意：现在只支持使用一个文件作为一个 module，即 `demo.py` 对应 `demo` 这个模块
            session_config: 不指定则为默认的: local=True, inline=False, group=jd_dev_alpha, auto_reload_interval=2， process_limit=0
        """
        super().__init__()
        self.session_id = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M')}_{str(uuid.uuid4())[0:8]}"

        self.recorder = GlobalValuesRecorder(main_globals)
        self.session_config = session_config or SessionConfig()

        self.module_names = [module.__name__ for module in modules]

        # 在 main 函数中会有 import demo as demo2 的情况，demo2 为 global_name，demo 为 module_name
        self.global_name_to_module_names = {}
        self.module_name_to_global_names = {}
        for global_name in self.recorder.main_globals:
            module = self.recorder.main_globals[global_name]
            if isinstance(module,
                          types.ModuleType) and module.__name__ in self.module_names:
                self.global_name_to_module_names[global_name] = module.__name__
                self.module_name_to_global_names[module.__name__] = global_name

        self.saved_global_var_dp = None

        # notebook 注册，在 cell 运行前 auto reload
        if self.session_config.nb_auto_reload:
            self.register_auto_reload()

        self.pool = Pool(process_limit=self.session_config.process_limit)

    @property
    def modules(self):
        mn2gn = self.module_name_to_global_names
        return {name: self.recorder.main_globals[mn2gn[name]] for name in
                self.module_names}

    @contextmanager
    def modules_globals(self):
        """
        记录，并且更新 modules 中的 global_var(若有)

        注意：

        1. 对于 [引用] 的变量，在 modules_globals 记录之后，可以直接赋值，会进行更新
            .. code-block:: python

                with session.modules_globals():
                    a = []
                a.append(1)    # module.a -> [1]
                a = [2, 3, 4]  # module.a -> [2, 3, 4]

        2. 对于 [赋值] 的变量，如 int、str，在 modules_globals 记录之后，直接赋值不会进行更新，
            2.1 在同一个notebook cell中需要 reload 或者 with 重新更新；

            2.2 不同 cell 之间，我们注册了 auto reload 机制，会在 cell 运行之前设置变量、更新代码

            2.3 建议在赋值完成之后，启动一个新的 cell 运行，这样也比较清晰

            .. code-block:: python

                # session = GlobalSession(auto_reload_interval=2) # 构造的时候输入也可以
                # session.auto_reload(2)  # 显示调用，一般不用，在构造的时候可以传入，让他进行自动 reload

                with session.modules_globals():
                    a = 1
                # 同一个 cell
                a = 2
                print(module.a) # -> 1, 赋值的不会变;
                session.reload() # 或者直接调用
                print(module.a) # -> 2, reload 之后就变了

                #[] cell 0
                # a = 3
                #[] cell 1
                print(module.a) # -> 3, 新启动 cell 会进行 auto reload

                # 另外重新记录的话，会立刻改变
                with session.modules_globals():
                    a = 3      # module.a -> 3



        Examples:

        .. code-block:: python

            # main.ipynb
            # 构造 session 来运行
            import demo as demo2
            demo2.foo = 123
            session = GlobalSession(globals(), (demo2, ), session_config=SessionConfig())
            with session.modules_globals():
                foo = 1  # 如果 demo2 中定义了 doo，会更新demo2 中的 foo 为 1

        """
        try:
            self.recorder.enter_record()
            yield
        finally:
            self.recorder.exit_record(*list(self.modules.values()))

    def reload(self):
        for module in self.modules.values():
            sys.path.insert(0, os.path.dirname(os.path.abspath(module.__file__)))
            importlib.reload(module)
            del sys.path[0]
        # reset global var
        self.recorder.update(*list(self.modules.values()))

    def update_values(self, module):
        self.recorder.update(module)

    def save_global_vars(self):
        """
        手动触发保存 global var，这样不用每次调用的时候都存一遍了
        :return:
        """
        workspace = path.realpath('.')
        io_path = path.join(workspace, '.hfai_session')
        os.makedirs(io_path, exist_ok=True)
        global_values_pkl = path.join(io_path, f'global_values_{self.session_id}.dp')
        save(self.recorder.global_values, global_values_pkl)
        self.saved_global_var_dp = global_values_pkl
        return global_values_pkl

    def load_global_vars(self, saved_global_values_path):
        """
        从指定路径中加载上次保存的 global var

        :param saved_global_values_path:
        :return:
        """
        global_vars = load(saved_global_values_path)
        for k in global_vars:
            self.recorder.main_globals[k] = global_vars[k]

    def set_process_limit(self, process_limit=0):
        """
        动态调整，限制运行时候的 process pool 进程数量，注意，已经启动的 process 不会被关闭

        :param process_limit: 0，表示不限制，将使用 cpu 的 process 数量
        :return:
        """
        self.pool.set_process_limit(process_limit)

    def apply(self, func, args: Tuple = (), kwargs: dict = {},
              local=None, inline=None, group=None, blocking=True,
              stdout=True,
              renew_global_var=False,
              packaging_func: str = None,
              priority: int = None,
              image: str = None):
        """
            运行注册了的 module 中的 func 函数，运行 `module.func(*args, **kwargs)`

            根据配置有以下三种核心模式(直接模式)与 packaging 特殊模式(建议使用 package 接口)：

            1. local = True and inline = True:

            .. code-block:: python

                # 相当于下面的代码
                importlib.reload(module)
                session.update_values(module)
                pool.apply(module.func, args, kwargs)

            2. local = True and inline = False

            .. code-block:: python

                # 注意，会带来 pkl 的开销，建议在使用 remote 之前 inline False 跑一跑
                # 一般而言，这个模式能跑的，remote 模式也能跑
                # inline = False 可以通过，async_result.stdout() 来打印输出
                python remote_call.py --module module.py --func function

            3. local = False and inline = False:

            .. code-block:: python

                # 提交任务远程运行
                hfai python remote_call.py --module module.py --func function -- --nodes 1

            4. packaging 模式： packaging_options:

            .. code-block:: python

                # 把多进程任务打包到一个任务上运行
                # pool.map(func, iterable=args)
                # pool.starmap(func, iterable=args)

        :param func: 模块中的函数，输入中需要带上 `module.`，如 `demo.func1` 这样
        :param args: func 的 args 参数
        :param kwargs: func 的 kwargs 参数
        :param local: 本地运行；不指定的话，会使用 session 构造中传入的 session_config
        :param inline: local 下的 inline 模式, 不建议使用；不指定的话，会使用 session 构造中传入的 session_config
        :param group: 远程运行到哪个分组的机器上
        :param blocking: True 同步接口，False 异步接口，返回 AsyncResult
        :param stdout: 是否在 outline 任务打印 stdout，apply 接口先不要用
        :param renew_global_var: 在调用函数的时候，自动更新 global var
        :param packaging_func: None, 表示不起用 outline 时候使用，此时，args 是一个 iterable，会启动一个 pool 来运行 func 把多个任务打包提交上去跑
        :param priority: 设置远程提交任务的优先级，不填则使用 Session Config 中的设置
        :param image: 使用哪个镜像运行，不填则用 Session Config 中的设置

        :return: blocking: `func(*args, **kwargs)` 的结果；not blocking，返回 AsyncResult
        """
        # 如果 func 是新加的，那么在 remote 运行的时候，module.func 会找不到，所以要 reload 起来
        self.reload()

        local = self.session_config.local if local is None else local
        inline = self.session_config.inline if inline is None else inline
        module_name = func.__module__
        module = self.modules[module_name]
        func_name = func.__name__
        stamp = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M')}_{str(uuid.uuid4())[0:8]}"
        job_name = f'{"local" if local else "remote"}_{"inline" if (inline and local) else "outline"}_call_{stamp}'

        if packaging_func:
            assert inline is not None, 'packaging 模式不支持 inline'
            assert not kwargs, '在 packaging 模式下，不能指定 kwargs'

        if local and inline:
            # 之所以在这里构建了一个 pool，是因为这样 notebook 才会把 func 中的 stdout 打印到 cell 中
            async_result = self.pool.apply_async(name=job_name, target=getattr(module, func_name), args=args, kwargs=kwargs)
            if blocking:
                return async_result.get()
            else:
                return async_result

        # create each calls workspace
        workspace = path.realpath(path.dirname(module.__file__))
        io_path = path.join(workspace, module_name, func_name, job_name)
        os.makedirs(io_path, exist_ok=False)  # 加了 uuid 就不应该存在

        group = self.session_config.group if group is None else group
        if renew_global_var or self.saved_global_var_dp is None:
            self.save_global_vars()
        # workspace/[module]/[func]/[job_name]/[params.pkl, global_values.pkl, output.pkl]
        #    现在假设 workspace 是用户的 notebook，之后再考虑是也能过户本地的情况
        # 可以把 remote call 和他们的 io python 记录到 .hfai/remote_call.sqlite 下，方便日后追查，不过目前没有看到这个需求
        args_params = path.join(io_path, 'args_params.dp')
        kwargs_params = path.join(io_path, 'kwargs_params.dp')
        output_pkl = path.join(io_path, 'output.pkl')
        save(args, args_params)
        save(kwargs, kwargs_params)
        # note: 保存这次调用的参数，不应该做，在循环里面这个性能就炸了
        remote_call_py = path.join(path.realpath(path.dirname(__file__)), 'remote_call.py')
        python_cmd = f'python {remote_call_py} --job {job_name} ' \
                     f'--workspace {workspace} ' \
                     f'--module {module_name} --function {func_name} ' \
                     f'--args {args_params} --kwargs {kwargs_params} ' \
                     f'--global_values {self.saved_global_var_dp} ' \
                     f'--output {output_pkl} ' \
                     f'--pool {self.pool.process_limit if packaging_func else 1} ' \
                     f'--pool_func {packaging_func}'
        priority = self.session_config.priority if priority is None else priority
        image = self.session_config.image if image is None else image
        hfai_python = f'hfai {python_cmd} -- --follow --nodes 1 --priority {priority} --group {group} --name {job_name}'
        if image:
            hfai_python += f' --image {image}'
        command = python_cmd if local else hfai_python

        def outline_func():
            process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
            ar = AsyncResult(job=job_name, process=process, output_pkl=output_pkl)
            return ar.get(stdout=stdout)  # stdout 给 outline

        async_result = self.pool.apply_async(name=job_name, target=outline_func)
        async_result.output_pkl = output_pkl
        async_result.stdout_pkl = output_pkl + '.stdout.pkl'

        if blocking:
            return async_result.get(stdout=stdout)
        else:
            return async_result

    def apply_async(self, func, args=(), kwargs={}, local=None, inline=None, group=None,
                    renew_global_var=False, stdout=True, priority: int = None,
                    image: str = None) -> AsyncResult:
        """
        一个异步的接口，返回一个 AsyncResult;
        """
        return self.apply(func=func, args=args, kwargs=kwargs, local=local,
                          inline=inline, group=group, blocking=False,
                          renew_global_var=renew_global_var, stdout=stdout,
                          priority=priority, image=image)

    def map(self, func, iterable=None, local=None, inline=None, group=None,
            blocking=True, stdout=False, star=False, priority: int = None,
            image: str = None) -> List:
        """
        多进程迭代运行，并且返回输出 list, 类比 pool.map, pool.starmap

        注意事项:
            1. 远程运行如果没有资源会一个个排队
            2. args_list 和 kwargs_list 不能同时存在

        Args:
            func: 需要引用的函数
            iterable: [(1, 2), (3, 4)] -> start = True: func(1, 2), func(3, 4); star = False: func((1, 2), ), func((3, 4), )
            local: 本地运行
            inline:
            group: 远程运行到哪个分组的机器上
            blocking: 是否放后台运行
            stdout: 要不要 stdout 输出内容，默认 False 防止 map 太多打爆；outline 会把 stdout pkl 下来方便日后查看
            star: True 的时候相当于 pool 的 starmap
            priority: 设置优先级，不设置则为 session config 中的
            image: 设置运行的镜像，不设置则为 session config 中的

        Return:
            blocking True 返回对应的输出；False 返回List[AsyncResult]
        """
        async_results = []
        for args in iterable:
            if not star:
                args = (args, )
            a_res = self.apply_async(func, args=args, local=local, inline=inline,
                                     group=group, stdout=stdout, priority=priority,
                                     image=image)  # 这个 stdout 传给 outline
            async_results.append(a_res)

        if blocking:
            return [a_res.get(stdout=stdout) for a_res in async_results]
        else:
            return [a_res for a_res in async_results]

    def map_async(self, func, iterable=None, local=None, group=None) -> List[AsyncResult]:
        """
        map 的异步调用

        :return: List[Async_result]
        """
        return self.map(func, iterable, local, group, blocking=False)

    def starmap(self, func, iterable=None, local=None, inline=None, group=None,
                blocking=True, stdout=False) -> List[AsyncResult]:
        """
        iterable: [(1, 2), (3, 4)] -> start = True: func(1, 2), func(3, 4)


        :return: List[Async_result]
        """
        return self.map(func, iterable, local, group, blocking=blocking, stdout=stdout, star=True)

    def starmap_async(self, func, iterable=None, local=None, inline=None, group=None,
                      stdout=False) -> List[AsyncResult]:
        """
        iterable: [(1, 2), (3, 4)] -> start = True: func(1, 2), func(3, 4)


        :return: List[Async_result]
        """
        return self.map(func, iterable, local, group, blocking=False, stdout=stdout, star=True)

    def package(self, func, iterable=None, local=None, group=None,
                blocking=True, stdout=False, star=False):
        """
        将多进程任务打包成一个任务运行，主要适用于我们想要提交远程任务的时候，方便在一个机器上启动一个多进程任务，而不是启动一堆任务

        实现上调用的是 pool.map/pool.starmap，所以使用 iterable 作为接口名字，不是 inline 模式

        .. code-block:: python

            session.package(demo.func, [0, 1, 2], star=False)

            # 等同于
            # demo_p.py
            def package_func():
                pool = multiprocessing.Pool(process_limit)
                return pool.map(demo.func, [(0, 1), (2, 3)])
                # return pool.starmap(demo.func, [(0, 1), (2, 3)])  若 star = True
            session.apply(demo_p.package_func(), [0, 1, 2], local=False)

        Args:
            func: 需要引用的函数
            iterable: [(1, 2), (3, 4)] -> star = True: func(1, 2), func(3, 4) else func((1, 2)), func((3, 4))
            local: 本地运行
            group: 远程运行到哪个分组的机器上
            blocking: 是否放后台运行
            stdout: 要不要 stdout 输出内容，默认 False 防止进程太多打爆；outline 会把 stdout pkl 下来方便日后查看
            star: 是否使用 starmap 来处理，默认使用 map

        Return:
            blocking True 返回对应的输出；False 返回List[AsyncResult]
        """
        return self.apply(func=func, args=iterable, local=local,
                          inline=False, group=group, blocking=blocking,
                          packaging_func='starmap' if star else 'map', stdout=stdout)

    def package_async(self, func, iterable=None, local=None, group=None,
                      stdout=False):
        """
        starpackage, 类比 pool.starmap

        iterable: [(1, 2), (3, 4)] -> func(1, 2), func(3, 4)

        """
        return self.package(func=func, iterable=iterable, local=local, group=group,
                            stdout=stdout, blocking=False)

    def starpackage(self, func, iterable=None, local=None, group=None, stdout=False):
        """
        starpackage 的异步调用，返回 async result

        :param func:
        :param iterable:
        :param local:
        :param group:
        :param stdout:
        :return:
        """
        return self.package(func=func, iterable=iterable, local=local, group=group,
                            stdout=stdout, blocking=True, star=True)

    def starpackage_async(self, func, iterable=None, local=None, group=None,
                      stdout=False):
        """
        package 的异步调用，返回 async result

        :param func:
        :param iterable:
        :param local:
        :param group:
        :param stdout:
        :return:
        """
        return self.package(func=func, iterable=iterable, local=local, group=group,
                            stdout=stdout, blocking=False, star=True)

    def register_auto_reload(self):
        def hfai_remote_session_auto_reload():  # 一个长一点的名字，好删除
            self.reload()
        try:
            ip = get_ipython()  # jupyter
            for func in ip.events.callbacks['pre_run_cell']:
                if func.__name__ == 'hfai_remote_session_auto_reload':
                    ip.events.unregister('pre_run_cell', func)
            ip.events.register('pre_run_cell', hfai_remote_session_auto_reload)
        except:
            print('not run in jupyter, pass register auto reload')
