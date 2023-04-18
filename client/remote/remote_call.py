import pickle as pkl
import sys
from argparse import ArgumentParser
import setproctitle
from data_package import load as dp_load
import traceback
from multiprocessing import Pool

parser = ArgumentParser('hfai remote func parser')
parser.add_argument('--job', help='任务进程名')
parser.add_argument('--workspace', help='模块文件所在的位置')
parser.add_argument('--module', help='模块文件')
parser.add_argument('--function', help='指定要运行的函数')
parser.add_argument('--args', default=None, help='函数需要的 args 输入, 是一个 hfai dp 文件')
parser.add_argument('--kwargs', default=None, help='函数需要的 kwargs 输入, 是一个 hfai dp 文件')
parser.add_argument('--global_values', default=None,
                    help='函数文件需要更新的全局变量， 是一个 pkl 文件')
parser.add_argument('--output', default=None, help='函数运行输出的位置, 是一个 pkl 文件')
parser.add_argument('--star_pool', default=1, type=int, help='启动一个 process pool 来运行代码')
parser.add_argument('--pool', default=1, type=int, help='启动一个 process pool 来运行代码')
parser.add_argument('--pool_func', default='map', type=str, help='pool 处理 args 的方法')

options, _ = parser.parse_known_args()

# 设置进程名字
setproctitle.setproctitle(options.job)

sys.path.insert(0, options.workspace)

module_name = options.module
module = __import__(module_name)

global_values = {}
if options.global_values:
    global_values = dp_load(options.global_values)

for k in global_values:
    setattr(module, k, global_values[k])

params = {}
if options.args:
    params['args'] = dp_load(options.args)
if options.kwargs:
    params['kwargs'] = dp_load(options.kwargs)

func = getattr(module, options.function)
try:
    if options.pool == 1:
        output = func(*params.get('args', []), **params.get('kwargs', {}))
    else:
        pool = Pool(options.pool)
        output = getattr(pool, options.pool_func)(func=func, iterable=params.get('args', []))
except Exception as e:
    output = ('remote_call_exception', e, traceback.format_exc())

# None 也 pkl 下去
pkl.dump(output, open(options.output, 'wb+'))
