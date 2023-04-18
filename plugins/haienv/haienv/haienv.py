import os
import getpass
import sys
from haienv.client.model import Haienv

keys = list(sys.modules.keys())
for removed_key in [key for key in keys if 'pkg_resources' in key]:
    sys.modules.pop(removed_key)

path_prefix = os.environ.get('HAIENV_PATH', os.environ['HOME'])

try:
    if not os.path.exists(path_prefix):
        os.makedirs(path_prefix)
    db_path = os.path.join(path_prefix, 'venv.db')
except:
    pass


def set_env(haienv_name, user: str = None):
    if user is None:
        user = haienv_name.split('[')[-1].split(']')[0] if '[' in haienv_name else None
    haienv_name = haienv_name.split('[')[0]

    if not db_path:
        raise Exception('获取haienv_path出错')
    root_path = os.path.realpath(os.path.join(db_path, '../..'))
    results = []
    for _user in sorted(os.listdir(root_path)):
        if user is not None and user != _user:
            continue
        _db_path = os.path.realpath(os.path.join(db_path, f'../../{_user}/venv.db'))
        if not os.path.exists(_db_path):
            continue
        try:
            result = Haienv.select(outside_db_path=_db_path, haienv_name=haienv_name)
            if result is not None:
                results.append((_user, result))
                if _user == getpass.getuser():
                    results = [(user, result)]
                    break
        except Exception as e:
            pass
    if len(results) == 0:
        raise Exception(f'未找到该虚拟环境 {haienv_name}，当前虚拟环境目录为{path_prefix}，如需更改请设置环境变量HAIENV_PATH')
    haienv_config = results[0][1]

    if len(haienv_config.extra_search_bin_dir):
        os.environ['PATH'] = f'{":".join(haienv_config.extra_search_bin_dir)}:{os.environ["PATH"]}'
    for var in haienv_config.extra_environment:
        try:
            key, value = var.split('=')
            os.environ[key] = value
        except:
            pass

    p_dir_list = [p_dir for p_dir in os.listdir(os.path.join(haienv_config.path, 'lib')) if p_dir.startswith('python')]
    assert len(p_dir_list) == 1, f'未找到python或有多个python'
    p_dir = p_dir_list[0]

    sys.path = ['.'] + haienv_config.extra_search_dir
    sys.path.extend([
        os.path.join(haienv_config.path, f'lib/{p_dir}/site-packages'),
        os.path.join(haienv_config.path, f'lib/{p_dir}'),
        os.path.join(haienv_config.path, f'lib/{p_dir}/lib-dynload')
    ])

    if haienv_config.extend == 'True':
        try:
            site_packages_path = os.path.join(haienv_config.path, f'lib/{p_dir}/site-packages')
            with open(os.path.join(site_packages_path, f'easy-install.pth'), 'r') as f:
                easy_install_list = f.read()
                for easy_install in easy_install_list.strip().split('\n'):
                    sys.path.append(os.path.realpath(os.path.join(site_packages_path, easy_install)))
        except:
            pass

    os.environ['PYTHONPATH'] = os.pathsep.join(sys.path)
    os.environ['HF_ENV_NAME'] = haienv_name
    for k in [k for k in sys.modules.keys() if 'pkg_resources' in k]:
        del sys.modules[k]


def get_envs( user: str = None):
    """
    :param user: None 为输出所有的
    :return:
    """
    if not db_path:
        raise Exception('获取venv_path出错')
    root_path = os.path.realpath(os.path.join(db_path, '../..'))
    results = []
    for _user in sorted(os.listdir(root_path)):
        if user is not None and user != _user:
            continue
        _db_path = os.path.realpath(os.path.join(db_path, f'../../{_user}/venv.db'))
        try:
            for haienv_name, item in Haienv.select(outside_db_path=_db_path).items():
                results += [(_user, haienv_name, item.path, item.extend, item.extend_env, item.py)]
        except:
            pass
    return results


__all__ = ['set_env', 'get_envs']
