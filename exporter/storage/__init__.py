
import os
import importlib
from pathlib import Path

# 自动导入目录下的 collector
for f in os.listdir(os.path.dirname(__file__)):
    if f.endswith('_collector.py'):
        importlib.import_module('.' + str(Path(f).with_suffix('')), package=__name__)

from .utils import collectors
