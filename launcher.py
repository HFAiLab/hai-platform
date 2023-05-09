import os
import sys

import toml
import time
import ujson
from cachetools import cached, Cache
from kubernetes.client.rest import ApiException

from base_model.training_task import TrainingTask
from conf import CONF
from conf.flags import EXP_STATUS
from conf.flags import QUE_STATUS, TASK_TYPE
from db import MarsDB
from db import redis_conn
from k8s import K8sPreStopHook, get_corev1_api, get_appsv1_api
from k8s.v1_api import client
from k8s.v1_api import get_env_var
from logm import logger, log_stage
from roman_parliament import register_parliament, add_archive_trigger, archive_dict, add_archive_for_senators
from roman_parliament.archive_triggers.launcher_task_trigger import LauncherTaskTrigger
from server_model.auto_task_impl import AutoTaskSchemaWithDbImpl
from server_model.pod import Pod
from server_model.selector import TrainingTaskSelector, TrainImageSelector

register_parliament()
k8s_corev1_api = get_corev1_api()
k8s_appsv1_api = get_appsv1_api()
manager_mount_name = []
manager_host_path = []
manager_mount_path = []
manager_mount_ro = []
if manager_mounts := CONF.try_get('launcher.manager_mounts'):
    for k, v in manager_mounts.items():
        manager_mount_name.append(k)
        manager_host_path.append(v.split(':')[0])
        manager_mount_path.append(v.split(':')[1])
        manager_mount_ro.append(v.split(':')[-1] == 'ro')
RETRY_TIMES = 2

module = os.environ.get('POD_NAME', 'launcher')


class DuplicatedPods(Exception):
    """重复插入了 pods"""
    pass


def get_manager_resource(big):
    return client.V1ResourceRequirements(limits={'cpu': 1, 'memory': '4000Mi' if big else '200Mi'},
                                         requests={'cpu': 0, 'memory': '1000Mi' if big else '100Mi'})


# image info 不会变来变去的
@cached(cache=Cache(maxsize=1024))
def get_image_info(image_name):
    # 在这里查询数据库，获取 image 的信息，这样的好处是，以后可以和议会的集成起来
    return TrainImageSelector.find_one(os.path.basename(image_name))


@cached(cache=Cache(maxsize=1024))
def get_node_zone(node):
    schedule_zone = MarsDB().execute(f'''select "schedule_zone" from "host" where "node" = '{node}' ''').fetchall()[0][0]
    return schedule_zone


@log_stage(module)
def insert_pods(task: TrainingTask):
    if task.task_type != TASK_TYPE.VIRTUAL_TASK:
        task_memory = task.config_json['assigned_resource']['memory']
        task_cpu = task.config_json['assigned_resource']['cpu']
        task_assigned_gpus = task.config_json['assigned_resource']['assigned_gpus']
        try:
            with MarsDB() as conn:
                for i, node in enumerate(task.assigned_nodes):
                    Pod(
                        task_id=task.id, pod_id=f'{task.user_name.replace("_", "-")}-{task.id}-{i}', job_id=i,
                        xp_id=task.id,
                        status=EXP_STATUS.CREATED, node=node, role=['worker', 'master'][i == 0], memory=task_memory[i],
                        cpu=task_cpu[i], assigned_gpus=task_assigned_gpus[i]
                    ).insert(db_conn=conn)
        except Exception as e:
            if 'duplicate' in str(e):
                logger.exception(e)
                raise DuplicatedPods('有 launcher 已经插入任务了')
            raise Exception(f'task_id: {task.id} 插入 pod 失败, args: {(task_memory, task_cpu, task_assigned_gpus)} exception: {e}')


@log_stage(module)
def create_manager(task: TrainingTask, user_name):
    task_id = task.id
    # 先创建 configmap
    manager_name = f'{user_name.replace("_", "-")}-{task_id}-manager'
    env = [
        get_env_var(key='TASK_ID', value=task_id),
        get_env_var(key='MANAGER_NAME', value=manager_name),
        get_env_var(key='NAMESPACE', value=CONF.launcher.task_namespace),
        get_env_var(key='TZ', value='Asia/Shanghai'),
        get_env_var(key='MARSV2_SERVER', value=os.environ.get('MARSV2_SERVER', CONF.try_get('launcher.api_server'))),
        get_env_var(key='DEBUG', value=os.environ.get('DEBUG', '0')),
        get_env_var(key='MODULE_NAME', value='manager'),
        get_env_var(key='MARSV2_SCHEDULE_ZONE', value=get_node_zone(task.assigned_nodes[0])),
        get_env_var(key='MARSV2_TASK_SIDECARS', value=ujson.dumps(task.schema.get('options', {}).get('sidecar', [])))
    ]
    if 'CUSTOM_FILE_NAME' in os.environ:
        env.append(get_env_var(key='CUSTOM_FILE_NAME', value=os.environ.get('CUSTOM_FILE_NAME', '')))
    if manager_envs := CONF.try_get('launcher.manager_envs'):
        for k, v in manager_envs.items():
            env.append(get_env_var(k, value=v))
    # hfai image
    if task.backend.startswith('train_image:'):
        train_image_info = get_image_info(task.backend[len('train_image:'):])
        env.append(get_env_var(key='HFAI_IMAGE', value=train_image_info.image_url))
        env.append(get_env_var(key='HFAI_IMAGE_WEKA_PATH', value=train_image_info.path))
    manager_docker_image = os.environ.get('CURRENT_POD_IMAGE', CONF.try_get('launcher.manager_image'))
    capabilities = client.V1Capabilities(add=['IPC_LOCK'])
    security_context = client.V1SecurityContext(capabilities=capabilities)

    volume_mounts = [
        client.V1VolumeMount(
            name=mount_name,
            mount_path=mount_item,
            read_only=ro)
        for mount_name, mount_item, ro in zip(manager_mount_name, manager_mount_path, manager_mount_ro)
    ]
    volumes = [
        client.V1Volume(
            name=mount_name,
            host_path=client.V1HostPathVolumeSource(path=mount_item))
        for mount_name, mount_item in zip(manager_mount_name, manager_host_path)
    ]
    # 把manager和存放log的目录全部mount进去
    volume_mounts += [
        client.V1VolumeMount(
            name='config-map',
            mount_path='/etc/config'
        ),
        client.V1VolumeMount(
            name='log',
            mount_path='/var/log/experiment-manager',
        ),
    ]
    volumes += [
        client.V1Volume(
            name='config-map',
            config_map=client.V1ConfigMapVolumeSource(
                name=f'etc-configmap-{task_id}'
            )
        ),
        client.V1Volume(
            name='log',
            host_path=client.V1HostPathVolumeSource(
                path=f'/var/log/experiment-manager/{user_name.replace("_", "-")}-{task_id}-manager-0',
                type='DirectoryOrCreate'
            )
        ),
    ]
    if os.environ.get('server_path'):  # 不使用镜像里的server
        volume_mounts += [
            client.V1VolumeMount(
                name='server',
                mount_path='/high-flyer/code/multi_gpu_runner_server',
                read_only=True
            )]
        volumes += [
            client.V1Volume(
                name='server',
                host_path=client.V1HostPathVolumeSource(
                    path=os.environ['server_path'],
                    type='Directory'
                )
            )]

    containers = [client.V1Container(name=container.replace('_', '-'),
                                     image=manager_docker_image,
                                     image_pull_policy=CONF.try_get('launcher.image_pull_policy', default='IfNotPresent'),
                                     security_context=security_context,
                                     env=env,
                                     volume_mounts=volume_mounts,
                                     resources=get_manager_resource('init' not in container))
                  for container in ['init_manager', 'manager']]
    init_command = ['/bin/bash', '-c']
    init_args = ' && '.join([
        "cd /high-flyer/code/multi_gpu_runner_server",
        "PYTHONPATH=/high-flyer/code/multi_gpu_runner_server python -u experiment_manager/manager/init_manager.py"])
    init_command.append(init_args)
    containers[0].command = init_command
    containers[1].command = ['supervisord', '-c', '/high-flyer/code/multi_gpu_runner_server/experiment_manager/supervisord.conf']
    labels = {
        'task_id': str(task_id),
        'user_id': user_name,
        'type': 'manager'
    }
    podspec = client.V1PodSpec(
        init_containers=containers[0:1],
        containers=containers[1:],
        volumes=volumes,
        service_account_name='default',
        restart_policy='Always',
        affinity=client.V1Affinity(
            node_affinity=client.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=client.V1NodeSelector(
                    node_selector_terms=[client.V1NodeSelectorTerm(
                        match_expressions=[
                            client.V1NodeSelectorRequirement(
                                key='kubernetes.io/hostname',
                                operator='In',
                                values=CONF.launcher.manager_nodes
                            )
                        ]
                    )]
                )
            )
        ))
    metadata = client.V1ObjectMeta(name=manager_name, namespace=CONF.launcher.task_namespace, labels=labels)
    podtemplatespec = client.V1PodTemplateSpec(metadata=metadata,
                                               spec=podspec)
    stspec = client.V1StatefulSetSpec(
        replicas=1,
        template=podtemplatespec,
        selector=client.V1LabelSelector(match_labels=labels),
        service_name=f'{user_name.replace("_", "-")}-{task_id}-manager'
    )
    st = client.V1StatefulSet(metadata=metadata, spec=stspec)
    st_resp = k8s_appsv1_api.create_namespaced_stateful_set_with_retry(namespace=CONF.launcher.task_namespace, body=st)
    # 接下来所有的资源 owner_ref 都指向 manager
    owner_ref = client.V1OwnerReference(api_version='apps/v1', kind='StatefulSet', name=st_resp.metadata.name, uid=st_resp.metadata.uid, controller=False, block_owner_deletion=True)

    # 创建 headless service
    metadata = client.V1ObjectMeta(
        name=f'{user_name.replace("_", "-")}-{task_id}-manager-0',
        namespace=CONF.launcher.task_namespace,
        owner_references=[owner_ref]
    )
    spec = client.V1ServiceSpec(selector={'statefulset.kubernetes.io/pod-name': f'{user_name.replace("_", "-")}-{task_id}-manager-0'}, cluster_ip='None')
    service = client.V1Service(api_version='v1', kind='Service', metadata=metadata, spec=spec)
    k8s_corev1_api.create_namespaced_service_with_retry(namespace=CONF.launcher.task_namespace, body=service)
    # 创建任务所需的 configmap，实际上可以先创建 manager 再创建 manager 需要的 configmap，这样所有资源的 ref 都能指向 manager
    k8s_corev1_api.create_namespaced_config_map_with_retry(
        namespace=CONF.launcher.task_namespace,
        body=client.V1ConfigMap(
            immutable=True,
            data={'override.toml': toml.dumps(CONF)},
            metadata=client.V1ObjectMeta(
                name=f'etc-configmap-{task_id}',
                namespace=CONF.launcher.task_namespace,
                owner_references=[owner_ref]
            )
        )
    )
    k8s_corev1_api.create_namespaced_config_map_with_retry(
        namespace=CONF.launcher.task_namespace,
        body=client.V1ConfigMap(
            immutable=True,
            data={
                file: open(os.path.join('marsv2/scripts', file), 'r').read()
                for file in os.listdir('marsv2/scripts') if os.path.isfile(os.path.join('marsv2/scripts', file))
            },
            metadata=client.V1ObjectMeta(
                name=f'marsv2-scripts-{task_id}', # 这里不像别的资源一样，加上用户名，因为 storage 表不支持 replace 字符串
                namespace=CONF.launcher.task_namespace,
                owner_references=[owner_ref]
            )
        )
    )
    k8s_corev1_api.create_namespaced_config_map_with_retry(
        namespace=CONF.launcher.task_namespace,
        body=client.V1ConfigMap(
            immutable=True,
            data={
                file: open(os.path.join('marsv2/entrypoints', file), 'r').read()
                for file in os.listdir('marsv2/entrypoints') if os.path.isfile(os.path.join('marsv2/entrypoints', file))
            },
            metadata=client.V1ObjectMeta(
                name=f'marsv2-entrypoints-{task_id}',
                namespace=CONF.launcher.task_namespace,
                owner_references=[owner_ref]
            )
        )
    )


def manual_make_task_finished(task: TrainingTask):
    for pod in task.pods:
        pod.update(('status', ), (EXP_STATUS.STOPPED, ))
    task.re_impl(AutoTaskSchemaWithDbImpl)
    task.update(('queue_status',), (QUE_STATUS.FINISHED,))


@log_stage(module)
def start_exp(task: TrainingTask):
    logger.info(f"收到消息，起 {task.job_info} 的节点")
    # 对于validation任务，如果最开始的虚拟任务停止，则对应的所有validation任务停止
    main_task = TrainingTaskSelector.find_one(None, chain_id=task.chain_id.split('_main')[0]) if task.task_type == TASK_TYPE.VALIDATION_TASK else task
    ban_name = f'ban:{main_task.user_name}:{main_task.nb_name}:{main_task.chain_id}'
    if redis_conn.get(ban_name):
        logger.info(f'{task.id}由于前置任务被停止，不启动')
        manual_make_task_finished(task)
        return
    for t in range(RETRY_TIMES):
        try:
            insert_pods(task)
            create_manager(task=task, user_name=task.user_name)
            return
        except ApiException as ae:
            if ae.status == 409:
                logger.info('已经存在这个任务的 manager 了，不用新建')
                return
            else:
                logger.exception(ae)
                logger.f_error(f'manager 第{t + 1}次失败了，请人工检查', task=task)
                continue
        except DuplicatedPods as dp:
            raise dp
        except Exception as e:
            logger.exception(e)
            logger.f_error(f'起 manager 失败， 错误编号为: {e}\n强制退出任务，请查看数据库', task=task)
            manual_make_task_finished(task)
            raise e
    logger.f_error(f'起 manager {RETRY_TIMES}次都失败了， 强制退出任务，请查看数据库', task=task)
    manual_make_task_finished(task)


if __name__ == '__main__':
    with logger.contextualize(uuid=f'{module}.setup'):
        logger.info(f'launcher python', sys.version)
        logger.info('开始订阅...')
        add_archive_trigger(LauncherTaskTrigger)
        # 启动过的任务记录一下
        started_archive_keys = set()
    with logger.contextualize(uuid=f'{module}.loop'):
        while True:
            if K8sPreStopHook.receive_stop_pod():
                MarsDB().dispose()
                logger.warning('收到了 stop launcher 的指令，退出自己')
                os.system("""ps -ef | grep -v PID | awk '{system("kill -KILL " $2)}'""")
            archive_keys = set(archive_dict.keys())
            started_archive_keys &= archive_keys
            for archive_key in filter(lambda x: TrainingTask.__name__ in x, archive_keys - started_archive_keys):
                if (task := archive_dict.get(archive_key, None)) is None:
                    continue
                try:
                    start_exp(task)
                    add_archive_for_senators(trigger_name='TrainingTaskTrigger', data=[task.id])
                except DuplicatedPods as de:
                    # 有别的 launcher 启动了这个任务，就不管了
                    logger.info('有其他 launcher 启动了这个任务, 跳过')
                    pass
                except Exception as e:
                    logger.exception(e)
                    logger.f_error(f'起 manager 出现了异常：{str(e)}', task=task)
                started_archive_keys.add(archive_key)
            time.sleep(0.001)
