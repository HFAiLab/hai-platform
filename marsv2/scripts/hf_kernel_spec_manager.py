

import os
import json
import psutil
import sysv_ipc
from jupyter_client.kernelspec import KernelSpecManager as BaseKernelSpecManager, KernelSpec
from jupyter_server.services.kernels.kernelmanager import AsyncMappingKernelManager as BaseKernelManager


VALID_ENVS = {}
INVALID_ENVS = set()
try:
    RELOAD_INVALID_ENVS_SHM = sysv_ipc.SharedMemory(6915405938, sysv_ipc.IPC_CREAT, mode=0o777, size=1)
    RELOAD_INVALID_ENVS_SHM.write(b'0')
except sysv_ipc.ExistentialError:
    RELOAD_INVALID_ENVS_SHM = sysv_ipc.SharedMemory(6915405938)


class HFKernelSpecManager(BaseKernelSpecManager):

    def get_all_specs(self):
        res = super(HFKernelSpecManager, self).get_all_specs()
        try:
            global VALID_ENVS, INVALID_ENVS
            current_envs = set()
            haienvs = json.loads(os.popen("haienv list -ojson").read().strip())
            if RELOAD_INVALID_ENVS_SHM.read() == b'1':
                INVALID_ENVS = set()
            for venv in haienvs.get('system', []) + haienvs.get('own', []):
                display_name = f'{venv["haienv_name"]}[{venv["py"]}]'
                if display_name in INVALID_ENVS:
                    continue
                if display_name not in VALID_ENVS:
                    if 0 == os.system(f'{os.path.join(venv["path"], "bin/python")} -c "import ipykernel_launcher"'):
                        VALID_ENVS[display_name] = {
                            'resource_dir': '',
                            'spec': {
                                'argv': ['bash', '-c',
                                         f'source haienv {venv["haienv_name"]} && python -m ipykernel_launcher -f {{connection_file}}'],
                                'env': {},
                                'display_name': display_name,
                                'language': 'python',
                                'interrupt_mode': 'signal',
                                'metadata': {}
                            }
                        }
                        current_envs.add(display_name)
                    else:
                        INVALID_ENVS.add(display_name)
                else:
                    current_envs.add(display_name)
            for n in set(VALID_ENVS.keys()) - current_envs:
                VALID_ENVS.pop(n, None)
            if RELOAD_INVALID_ENVS_SHM.read() == b'1':
                RELOAD_INVALID_ENVS_SHM.write(b'0')
            return {**res, **VALID_ENVS}
        except Exception as e:
            print('HFKernelSpecManager get_all_specs error:', e, flush=True)
            return res

    def get_kernel_spec(self, kernel_name):
        try:
            return super(HFKernelSpecManager, self).get_kernel_spec(kernel_name)
        except Exception:
            return KernelSpec(resource_dir='', **self.get_all_specs()[kernel_name]['spec'])


class HFKernelManager(BaseKernelManager):

    def kill_all_process_with_kernel_id(self, kernel_id):
        self.log.warning(f'an error occurred, kill all process with kernel_id: {kernel_id}')
        all_pids = psutil.pids()
        for pid in all_pids:
            process = psutil.Process(pid)
            process_cmd = process.cmdline()
            self.log.warning(f'{pid} {process.cmdline()}')
            if process.is_running() and 'ipykernel_launcher' in str(process_cmd) and kernel_id in str(process_cmd):
                self.log.warning(f'kill process: {pid}')
                try:
                    process.kill()
                except Exception as e:
                    self.log.warning(f'kill process failed: {pid} {e}')

    def _handle_kernel_died(self, kernel_id):
        self.kill_all_process_with_kernel_id(kernel_id)
        return super(HFKernelManager, self)._handle_kernel_died(kernel_id)
