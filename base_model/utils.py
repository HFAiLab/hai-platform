
import sys
import os

from importlib.abc import MetaPathFinder, FileLoader
from importlib.util import spec_from_file_location, spec_from_loader


class EmptyFileLoader(FileLoader):

    def get_source(self, fullname: str):
        return ''


class CustomFinder(MetaPathFinder):

    def __init__(self, custom_file_name: str):
        super().__init__()
        self.custom_file_name = custom_file_name

    def find_spec(self, fullname, path, target=None):
        if path is None or path == "":
            path = [os.getcwd()]
        *parents, name = fullname.split('.')
        for entry in path:
            if not os.path.isdir(entry):
                continue
            if os.path.isdir(os.path.join(entry, name)):
                filename = os.path.join(entry, name, "__init__.py")
            else:
                filename = os.path.join(entry, name + ".py")
            # 对于平台代码目录的 import，找对应的 custom_file，找不到就提供一个空文件
            if \
                    (
                        entry.startswith(os.environ.get('SERVER_CODE_DIR', '/high-flyer/code/multi_gpu_runner_server')) or
                        'hfai/client' in entry or 'hfai/base_model' in entry
                    ) and \
                    len(set(os.listdir(entry)) & {'implement.py', 'default.py'}) == 2 and \
                    name == 'custom':
                filename = os.path.join(entry, self.custom_file_name + ".py")
                if os.path.exists(filename):
                    return spec_from_file_location(fullname, filename)
                return spec_from_loader(name, EmptyFileLoader(fullname=fullname, path=entry))
            if not os.path.exists(filename):
                continue
            return spec_from_file_location(fullname, filename)
        return None


def setup_custom_finder():
    """
    提供了一个方便地测试 custom.py 的方法
    指定 CUSTOM_FILE_NAME 就可以将 implement.py / default.py 结构中的 custom.py 改为去找指定的文件
    不需要指定到 .py 结尾，只用不带后缀的文件名就行
    如果指定了不走 custom（例如设置 CUSTOM_FILE_NAME=xxx），如果不存在 xxx.py，会 fallback 到 default（提供一个空文件来 import）
    """
    default_custom_file_name = 'custom'
    custom_file_name = os.environ.get('CUSTOM_FILE_NAME', default_custom_file_name)
    if custom_file_name != default_custom_file_name or custom_file_name == 'custom':
        sys.meta_path = [m for m in sys.meta_path if not isinstance(m, CustomFinder)]
        sys.meta_path.insert(0, CustomFinder(custom_file_name=custom_file_name))
