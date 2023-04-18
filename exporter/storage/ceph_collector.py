
import os
from conf import CONF
from db import MarsDB
from utils import run_cmd_new
from .utils import register_collector


ROOT_PATH = CONF.exporter.ceph_root
USAGE_ATTR = 'ceph.dir.rbytes'
QUOTA_ATTR = 'ceph.quota.max_bytes'
SCRIPT_PATH = os.path.join(os.path.dirname(__file__), 'ceph_stat.sh')


def get_ceph_size():
    fs = os.statvfs(ROOT_PATH)
    return fs.f_frsize * fs.f_blocks


def collect_attr(attr, path_tags, key):
    script_cmd = f'/bin/bash {SCRIPT_PATH} {attr}'
    result = {}
    count_path = set()
    for path, tag in path_tags:
        stats = []
        if path.endswith('*'):
            stats = run_cmd_new(f'{script_cmd} "{path}"', timeout=10).decode().strip().split('\n')
            path = path[:-2]
        stats += run_cmd_new(f'{script_cmd} "{path}"', timeout=5).decode().strip().split('\n')
        stats = [stat.split() for stat in stats if len(stat) > 0]
        result.update({
            host_path: {'host_path': host_path, key: int(value), 'tag': tag }
            for host_path, value in stats if host_path not in count_path
        })
        count_path |= set(host_path for host_path, _ in stats)
    return result


@register_collector('cephfs_usage', columns=['used_bytes', 'limit_bytes'], labels=['tag'])
def get_ceph_usage():
    ceph_paths = list(MarsDB().execute(r''' select "host_path", "tag" from "storage_monitor_dir" where "type"='ceph' '''))
    path_usage = collect_attr(USAGE_ATTR, ceph_paths, key='used_bytes')
    path_quota = collect_attr(QUOTA_ATTR, ceph_paths, key='limit_bytes')
    for path, data in path_usage.items():
        data.update(path_quota.get(path, {}))
        if path == ROOT_PATH:
            data['limit_bytes'] = get_ceph_size()
    result = list(path_usage.values())
    return result
