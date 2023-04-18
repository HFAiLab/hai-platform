import os
import aiofiles
import shutil
import munch
import sys
import sysconfig
import getpass
from stat import S_IWUSR
from .script import ACTIVATE
from .model import Haienv, HaienvConfig, get_haienv_path, get_db_path


HAIENV_PLACEHOLDER = 'haienv_placeholder'


async def create_haienv(haienv_name, extend, py, extra_search_dir, extra_search_bin_dir, extra_environment, **kwargs):
    assert extend in ['True', 'False']
    current_pyconfig = sysconfig.get_paths()
    current_pyconfig_scripts = os.environ.get('HAIENV_SCRIPTS', current_pyconfig['scripts'])
    current_pyconfig_purelib = os.environ.get('HAIENV_PURELIB', current_pyconfig['purelib'])
    current_env = os.environ.get('HAIENV_NAME', os.environ.get('HF_ENV_NAME', None))  # 兼容HF_ENV_NAME
    if not current_env:
        current_env = 'base'
    while True:
        notes = f"确认您要构建的虚拟环境版本为：{py}；"
        notes += f"扩展{current_env}环境" if extend == 'True' else f"不扩展当前python环境；"
        if extra_search_dir:
            notes += f'\n额外的PYTHONPATH: {extra_search_dir}'
        if extra_search_bin_dir:
            notes += f'\n额外的PATH: {extra_search_bin_dir}'
        if extra_environment:
            notes += f'\n额外的环境变量: {extra_environment}'
        notes += '\nY/N: '
        check = input(notes)
        if check in ['Y', 'N']:
            break
    if check == 'N':
        return {
            'success': 0,
            'msg': '放弃创建虚拟环境'
        }

    if os.environ.get("TASK_NAME", "NO_CLUSTER") != "NO_CLUSTER":  # 集群环境
        channels = os.popen('conda config --show channels').read()
        if not ('defaults' in channels and channels.count('\n') == 2):
            return {
                'success': 0,
                'msg': '集群环境下创建haienv请指定唯一的defaults channel（通过conda config --show channels看）'
            }

    result = get_haienv_path(haienv_name=haienv_name)
    if result['success'] == 0:
        return result
    path = result['msg']
    if extend == 'True':  # 继承上个环境的额外环境变量
        if os.environ.get('HFAI_ENV_EXTEND_PATH', ''):
            extra_search_dir = list(extra_search_dir) + os.environ['HFAI_ENV_EXTEND_PATH'].strip(':').split(':')
        if os.environ.get('HFAI_ENV_EXTEND_BIN_PATH', ''):
            extra_search_bin_dir = list(extra_search_bin_dir) + os.environ['HFAI_ENV_EXTEND_BIN_PATH'].strip(':').split(':')
        if os.environ.get('HFAI_ENV_EXTEND_ENVIRONMENT', ''):
            extra_environment = list(extra_environment) + os.environ['HFAI_ENV_EXTEND_ENVIRONMENT'].strip(':').split(':')
    haienv_config = HaienvConfig(path=path, extend=extend, extend_env=current_env if extend == 'True' else '', py=py, extra_search_dir=extra_search_dir, extra_search_bin_dir=extra_search_bin_dir, extra_environment=extra_environment)
    Haienv.insert(haienv_name=haienv_name, haienv_config=haienv_config)

    if os.system(f'conda create -p {path} -y python={py} -q -k'):
        await remove_haienv(haienv_name)
        return {
            'success': 0,
            'msg': '使用conda创建虚拟环境失败，请检查'
        }
    try:
        p_dir = [p_dir for p_dir in os.listdir(os.path.join(path, 'lib')) if p_dir.startswith('python')][0]
        activate_content = ACTIVATE

        # 创建pip.conf
        try:
            pip_configs = munch.Munch.fromYAML(open('/marsv2/scripts/pip_conf.yaml'))
        except:
            pip_configs = munch.Munch()
        if extend == 'True':  # 如果扩展之前的环境的话，为了避免pip在升级某个包的时候会先去卸载之前的包，就需要指定target
            pip_configs['user'] = 'yes'
        pip_conf_path = os.path.join(path, 'pip.conf')
        async with aiofiles.open(pip_conf_path, "w") as fp:
            await fp.write('[global]\n')
            for k, v in pip_configs.items():
                await fp.write(f'{k} = {v}\n')
        user_name = getpass.getuser()
        activate_content = activate_content.replace('__PIP_PATH__', pip_conf_path).replace('__HF_ENV_NAME__', haienv_name)\
            .replace('__PATH__', path).replace('__HF_ENV_OWNER__', user_name)

        if extend == 'True':
            # 创建 easy-install  如果不用继承原来环境就不装
            easy_install_lg_list = []
            easy_install_path = os.path.join(path, f'lib/{p_dir}/site-packages')
            dist_pakcage_list = [e for e in sys.path if e.endswith('dist-packages')]
            for package in [current_pyconfig_purelib] + dist_pakcage_list:
                try:
                    async with aiofiles.open(os.path.join(package, 'easy-install.pth')) as fp:
                        item_list = await fp.read()
                        for item in item_list.strip().split('\n'):
                            easy_install_lg_list.append(os.path.relpath(os.path.join(current_pyconfig_purelib, item), easy_install_path))
                except:
                    pass
            async with aiofiles.open(os.path.join(easy_install_path, 'easy-install.pth'), "w") as fp:
                await fp.write(os.path.relpath(current_pyconfig_purelib, easy_install_path) + '\n')
                for dist_package in dist_pakcage_list:
                    if not os.path.relpath(dist_package, easy_install_path) in easy_install_lg_list:
                        await fp.write(os.path.relpath(dist_package, easy_install_path) + '\n')
                await fp.write('\n'.join(easy_install_lg_list) + '\n')
            activate_content = activate_content.replace('__NAME__', f"[{haienv_name}] ({current_env})")
            path_list = os.environ['PATH'].split(':')
            injected_extended_path = f"{HAIENV_PLACEHOLDER}:{current_pyconfig_scripts}:"
            for extended_path in [path_list[i + 1] for i, path in enumerate(path_list[:-1]) if HAIENV_PLACEHOLDER == path]:
                injected_extended_path += f'{HAIENV_PLACEHOLDER}:{extended_path}:'
            activate_content = activate_content.replace('__EXTEND_HF_ENV__', f"PATH={injected_extended_path}${{PATH}}")
        else:
            activate_content = activate_content.replace('__NAME__', f"[{haienv_name}]")
            activate_content = activate_content.replace('__EXTEND_HF_ENV__', '')

        activate_path = os.path.join(path, 'activate')
        async with aiofiles.open(activate_path, "w") as fp:
            await fp.write(activate_content)
    except Exception as e:
        await remove_haienv(haienv_name)
        return {
            'success': 0,
            'msg': f'创建虚拟环境时发生{e}，请检查'
        }
    return {
        'success': 1,
        'msg': f'创建虚拟环境成功，使用 source haienv {haienv_name} 进入，或是在 python 中通过 import haienv; haienv.set_env(\'{haienv_name}\') 进入' +
        ('' if extend == 'True' else '\n注意新的环境下没有haienv或者hfai')
    }


async def list_haienv(user):
    try:
        if user is not None:
            outside_db_dir = os.path.realpath(os.path.join(get_db_path(), f'../../{user}'))
            outside_db_path = os.path.join(outside_db_dir, 'venv.db')
            assert os.path.exists(outside_db_path), '该用户不存在haienv'
            return Haienv.select(outside_db_path=outside_db_path)
        return Haienv.select()
    except:
        return []


def safe_delete(dest):
    def onerror(func, path, exc_info):
        if not os.access(path, os.W_OK):
            os.chmod(path, S_IWUSR)
            func(path)
        else:
            raise
    shutil.rmtree(dest, ignore_errors=True, onerror=onerror)


async def remove_haienv(haienv_name):
    item: HaienvConfig = Haienv.select(haienv_name=haienv_name)
    if item is None:
        return {
            'success': 0,
            'msg': f'未找到名为{haienv_name}的虚拟环境，请通过haienv list查看所有虚拟环境'
        }
    safe_delete(item.path)
    Haienv.delete(haienv_name=haienv_name)
    return {
        'success': 1,
        'msg': f'成功删除虚拟环境{haienv_name}'
    }
