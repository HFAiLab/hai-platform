import os
import shlex
import sys
import time
import json
from concurrent.futures import ThreadPoolExecutor, wait

import munch
import ujson
from kubernetes.client.rest import ApiException
from kubernetes.watch import Watch

from api.task_schema import TaskService
from base_model.training_task import TrainingTask
from conf import CONTAINER_NAME, CONF
from conf.flags import MOUNT_CODE, EXP_STATUS, QUE_STATUS, TASK_TYPE
from k8s.v1_api import client
from k8s.v1_api import get_node_resource, get_env_var
from logm import logger, log_stage, bind_logger_task
from server_model.auto_task_impl import AutoTaskSchemaWithDbImpl
from server_model.selector import TrainingTaskSelector
from server_model.task_impl import DbOperationImpl
from server_model.task_runtime_config import TaskRuntimeConfig
from server_model.user import User
from server_model.user_data import initialize_user_data_roaming
from k8s import get_corev1_api, get_networkv1beta1_api, get_appsv1_api
from roman_parliament import set_mass_info, register_parliament
from roman_parliament.utils import generate_key

from experiment_manager.manager.manager_utils import get_log_uuid
from utils import DatetimeEncoder

task_id = int(os.environ['TASK_ID'])
module = os.path.basename(__file__)
initialize_user_data_roaming(overwrite_enable_roaming=False)
set_mass_info(key_list=[generate_key(class_name=TrainingTask.__name__, sign='id', value=task_id)], mass_name=f'{task_id}_{module}')
register_parliament()

log_id = get_log_uuid(module)
logger.info(sys.version.replace('\n', ''), uuid=log_id)

task = TrainingTaskSelector.find_one(AutoTaskSchemaWithDbImpl, id=task_id)
bind_logger_task(task)
user: User = task.user

k8s_corev1_api = get_corev1_api()
k8s_networkv1beta1_api = get_networkv1beta1_api()
# 训练相关的所有资源 ref 到这个任务的 manager
k8s_appsv1_api = get_appsv1_api()
manager_name = os.environ['MANAGER_NAME']
manager_uid = k8s_appsv1_api.read_namespaced_stateful_set_with_retry(manager_name, os.environ['NAMESPACE']).metadata.uid
owner_ref = client.V1OwnerReference(api_version='apps/v1', kind='StatefulSet', name=manager_name, uid=manager_uid, controller=False, block_owner_deletion=True)


# worker会将pod状态都改为created再通知launcher启动，如果这个时候不是created，说明已经启动过init_manager了

if any([pod.status != EXP_STATUS.CREATED for pod in task.pods]):
    logger.warning(f'检测到所有pod状态都不为created，不启动任务', uuid=f'{log_id}.enter_exit')  # pod已创建
    exit(0)


def check_mount(mount_path, mount_code):
    for k, v in MOUNT_CODE.items():
        if not (k & mount_code) and mount_path.startswith(v):  # 没选中这个mount path就不把它mount进去
            return False
    return True


def create_tcp_service_nodeport(rank, node):
    if 'nodeports' not in node.service or len(node.service.nodeports) == 0:
        return
    for nodeport in node.service.nodeports:
        try:
            res = user.nodeport.create(task, namespace=node.namespace, alias=nodeport.name, dist_port=nodeport.port, rank=rank)
            logger.info(f'为 {node.pod_id} 创建 nodeport 成功，信息：{str(res)}')
        except Exception as e:
            logger.warning(f'为 {node.pod_id} 创建 nodeport 失败，跳过创建，{e}')


def create_headless_services(node):
    if 'headless_services' not in node.service or len(node.service.headless_services) == 0:
        return
    metadata = client.V1ObjectMeta(name=node.pod_id, labels=node.labels, namespace=node.namespace, owner_references=[owner_ref])
    spec = client.V1ServiceSpec(selector=node.labels,
                                ports=[
                                    client.V1ServicePort(port=service.port, target_port=service.port, name=service.name)
                                    for service in node.service.headless_services
                                ],
                                cluster_ip='None')
    service = client.V1Service(api_version='v1', kind='Service', metadata=metadata, spec=spec)
    try:
        k8s_corev1_api.create_namespaced_service_with_retry(namespace=node.namespace, body=service)
    except ApiException as ae:
        if ae.status == 409:  # conflict
            logger.info(f'[svc {node.pod_id} already exits')
        else:
            logger.exception(ae)
            logger.f_error(f'创建 headless 服务失败: {ae}')
            raise
    logger.info(f'为 {node.pod_id} 创建 headless 服务成功')  # 创建网络


def create_http_service_ingress(node, base_ingress_name):
    if 'ingress_rules' not in node.service or len(node.service.ingress_rules) == 0:
        return
    host = CONF.jupyter.ingress_host[base_ingress_name]
    ingress_name = f'{node.pod_id}-{base_ingress_name}'
    metadata = client.V1ObjectMeta(
        name=ingress_name, labels=node.labels, namespace=node.namespace,
        annotations={
            "nginx.ingress.kubernetes.io/proxy-buffering": "off",
            "nginx.ingress.kubernetes.io/proxy-read-timeout": "604800",
            "nginx.ingress.kubernetes.io/proxy-send-timeout": "604800"
        },
        owner_references=[owner_ref],
    )
    ingress_rules = [
        client.NetworkingV1beta1IngressRule(
            host=host, http=client.NetworkingV1beta1HTTPIngressRuleValue(
                paths=[client.NetworkingV1beta1HTTPIngressPath(
                    backend=client.NetworkingV1beta1IngressBackend(
                        service_name=node.pod_id,
                        service_port=rule.port,
                    ),
                    path=rule.path,
                    path_type='Prefix'
                )]
            )
        )
        for rule in node.service.ingress_rules
    ]
    spec = client.NetworkingV1beta1IngressSpec(ingress_class_name='nginx', rules=ingress_rules)
    ingress = client.NetworkingV1beta1Ingress(kind='Ingress', metadata=metadata, spec=spec)
    try:
        k8s_networkv1beta1_api.create_namespaced_ingress_with_retry(namespace=node.namespace, body=ingress)
    except ApiException as ae:
        if ae.status == 409:  # conflict
            logger.info(f'ingress {ingress_name} already exits')
        else:
            logger.exception(ae)
            logger.f_error(f'创建 ingress 失败: {ae}')
            raise

    # wait for ingress provision
    watch = Watch()
    for event in watch.stream(func=k8s_networkv1beta1_api.list_namespaced_ingress,
                              namespace=node.namespace,
                              field_selector=f'metadata.name={ingress_name}'):
        if event["object"].status.load_balancer.ingress is not None:
            logger.info(f'为 {node.pod_id} 创建 ingress 成功，信息：'
                         f'{event["object"].status.load_balancer.ingress}')
            watch.stop()


@log_stage(log_id)
def create_master_network(rank, node, is_internal):
    create_tcp_service_nodeport(rank, node)
    if rank == 0:  # 只有 master 才会创建
        create_headless_services(node)
        create_http_service_ingress(node, 'hfhub')
        if not is_internal:
            create_http_service_ingress(node, 'yinghuo')


@log_stage(log_id)
def create_node_in_k8s(rank, node_schema):
    """

    :param rank:
    :param node_schema: 这个节点上的 schema
    :return:
    """
    # 写在 pod spec 里的 env
    master_addr = 'localhost' if rank == 0 else f'{task.user_name.replace("_", "-")}-{task_id}-0'
    nvidia_visible_devices = ','.join(
        f'{int(str(gpu)[1:3])}:{int(str(gpu)[3:5])}' if len(str(gpu)) == 5 else str(gpu)  # mig
        for gpu in task.pods[rank].assigned_gpus)
    k8s_envs = {
        'MASTER_ADDR': master_addr,
        'MASTER_PORT': 2222,
        'NVIDIA_VISIBLE_DEVICES': nvidia_visible_devices,
        'MOUNT_LIST': ','.join(mount_item.mount_path for mount_item in node_schema.mounts),
        'MARSV2_SCHEDULE_ZONE': os.environ.get('MARSV2_SCHEDULE_ZONE', 'A'),
        'ROOM': os.environ.get('MARSV2_SCHEDULE_ZONE', 'A'),
    }
    # 目前只支持两个 NUMA 半节点调度
    if task.config_json.get('assigned_resource', {}).get('assigned_numa') in {'0', '1'}:
        k8s_envs['MARSV2_ASSIGNED_NUMA'] = task.config_json['assigned_resource']['assigned_numa']
    # 用于jupyter任务获取studio地址
    if task.task_type == TASK_TYPE.JUPYTER_TASK and "studio" in CONF.jupyter.ingress_host.keys():
        k8s_envs['MARSV2_STUDIO_ADDR'] = CONF.jupyter.ingress_host["studio"]
    logger.info(f'为任务 在 [{rank}] {node_schema.node} 创建运行脚本 configmap')
    start_scripts_configmap_prefix = f'start-scripts-{task.id}'
    start_scripts_configmap_id = f'{start_scripts_configmap_prefix}-{rank}'
    start_scripts_configmap = client.V1ConfigMap(
        api_version='v1',
        immutable=True,
        data={
            'task_run.sh': node_schema.task_run_script,
            'hf_envs.values': node_schema.hf_envs_values,
            'haiprof_envs.values': node_schema.haiprof_env_values,
            'grant_user_group.sh': node_schema.grant_user_group_script,
            'pod_env.values': '\n'.join(f'export {k}={v}' for k, v in k8s_envs.items()),
            'task.json': json.dumps({k: v for k, v in task._trait_values.items() if k != '_pods_'}, cls=DatetimeEncoder)
        },
        metadata=client.V1ObjectMeta(name=start_scripts_configmap_id, namespace=node_schema.namespace, owner_references=[owner_ref])
    )
    try:
        k8s_corev1_api.create_namespaced_config_map_with_retry(namespace=node_schema.namespace, body=start_scripts_configmap)
    except ApiException as ae:
        if ae.status == 409:  # conflict
            logger.info(f'configmap {start_scripts_configmap_id} already exits')
        else:
            logger.exception(ae)
            logger.f_error(f'configmap {start_scripts_configmap_id} init error: {ae}')
            raise
        # 出错了的话，需要处理
    capabilities = client.V1Capabilities(add=node_schema.caps)
    security_context = client.V1SecurityContext(capabilities=capabilities, privileged=node_schema.privileged)
    volumes = []
    volume_mounts = []
    # host_path 方式
    volumes += [
        client.V1Volume(
            name=mount_item.name,
            host_path=client.V1HostPathVolumeSource(path=mount_item.host_path, type=mount_item.mount_type)
        )
        for mount_item in node_schema.mounts
        if check_mount(mount_item.host_path, task.mount_code) and
            mount_item.mount_type in {'Directory', 'File', 'DirectoryOrCreate', 'FileOrCreate', 'Socket', ''}
    ]
    volume_mounts += [
        client.V1VolumeMount(
            name=mount_item.name,
            mount_path=mount_item.mount_path,
            read_only=mount_item.read_only if 'read_only' in mount_item else True
        )
        for mount_item in node_schema.mounts
        if check_mount(mount_item.host_path, task.mount_code) and
            mount_item.mount_type in {'Directory', 'File', 'DirectoryOrCreate', 'FileOrCreate', 'Socket', ''}
    ]
    # configmap 方式
    configmap_names = set()
    for mount_item in node_schema.mounts:
        configmap_name = mount_item.host_path.split(':')[0]
        if check_mount(mount_item.host_path, task.mount_code) and \
                mount_item.mount_type == 'configmap' and configmap_name not in configmap_names:
            configmap_names.add(configmap_name)
            volumes.append(client.V1Volume(
                name=configmap_name,
                config_map=client.V1ConfigMapVolumeSource(name=configmap_name)
            ))
    volume_mounts += [
        client.V1VolumeMount(
            name=mount_item.host_path.split(':')[0],
            mount_path=mount_item.mount_path,
            sub_path=None if len(mount_item.host_path.split(':')) == 1 else mount_item.host_path.split(':')[1]
        )
        for mount_item in node_schema.mounts
        if check_mount(mount_item.host_path, task.mount_code) and
            mount_item.mount_type == 'configmap'
    ]
    # pvc 方式
    volumes += [
        client.V1Volume(
            name=mount_item.name,
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=mount_item.host_path)
        )
        for mount_item in node_schema.mounts
        if check_mount(mount_item.host_path, task.mount_code) and
            mount_item.mount_type == 'pvc'
    ]
    volume_mounts += [
        client.V1VolumeMount(
            name=mount_item.name,
            mount_path=mount_item.mount_path,
            read_only=mount_item.read_only if 'read_only' in mount_item else True)
        for mount_item in node_schema.mounts
        if check_mount(mount_item.host_path, task.mount_code) and
            mount_item.mount_type == 'pvc'
    ]

    volume_mounts += [
        client.V1VolumeMount(
            name='start-scripts',
            mount_path='/marsv2/scripts/init'
        ),
        client.V1VolumeMount(
            name='log-dir',
            mount_path='/marsv2/log'
        )
    ]

    volumes += [
        client.V1Volume(
            name='start-scripts',
            config_map=client.V1ConfigMapVolumeSource(name=f'{start_scripts_configmap_prefix}-{rank}'),
        ),
        client.V1Volume(
            name='log-dir',
            host_path=client.V1HostPathVolumeSource(path=user.config.log_dir(),
                                                    type='DirectoryOrCreate')
        )
    ]
    logger.info(f'create_node - {rank} - {node_schema["node"]} - {node_schema["resources"].toJSON()}')

    resources = get_node_resource(node_schema['resources'], group=task.group)
    if task.config_json.get('override_node_resource', None):
        override_node_resource = task.config_json.get('override_node_resource')
        requests = {
            'cpu': override_node_resource.get('cpu', 0),
            'memory': override_node_resource.get('memory', 0)
        }
        limits = {
            'cpu': override_node_resource.get('cpu_limit', requests['cpu']),
            'memory': override_node_resource.get('memory_limit', requests['memory']),
        }
        resources = client.V1ResourceRequirements(limits=limits, requests=requests)
    logger.info(f'尝试创建计算节点: {node_schema.pod_id}')  # 创建pod

    init_containers = []
    if node_schema.link_hfai_image:
        # 使用从 launcher 传来的数据, 避免查询数据库
        envs = [get_env_var(key=ee, value=os.environ.get(ee)) for ee in ['HFAI_IMAGE', 'HFAI_IMAGE_WEKA_PATH']]
        init_containers = [client.V1Container(
            name=f'{CONTAINER_NAME}-load-image',
            image='registry.high-flyer.cn/google_containers/busybox:latest',
            image_pull_policy=CONF.try_get('manager.image_pull_policy', default='IfNotPresent'),
            env=envs,
            volume_mounts=volume_mounts + [client.V1VolumeMount(name='data-local', mount_path='/data_local')],
            resources=client.V1ResourceRequirements(limits={'cpu': 1, 'memory': '200Mi'}),
            command=['/bin/sh'],
            args=['/marsv2/scripts/link_hfai_image.sh'],
        )]
        volumes += [client.V1Volume(name='data-local', host_path=client.V1HostPathVolumeSource(path='/data_local'))]
    # note 这个由 launcher 传进来，init manager 的启动要快，不要走 io，类似我对 HFAI_IMAGE_WEKA_PATH 的处理
    # room = db_engine.execute(f'''select "room" from "host" where "node" = '{node.node}' ''').fetchall()[0][0]
    if node_schema.image is None:
        msg = f'用户 {user.user_name} 指定了非法的 image'
        raise Exception(msg)

    containers = [client.V1Container(
        name=CONTAINER_NAME,
        image=node_schema.image,
        image_pull_policy=CONF.try_get('manager.image_pull_policy', default='IfNotPresent'),
        security_context=security_context,
        command=['/bin/bash', '-c'],
        args=['cd /marsv2/entrypoints && bash entrypoint.sh'],
        env=[get_env_var(key=k, value=v) for k, v in k8s_envs.items()],
        resources=resources,
        volume_mounts=volume_mounts + [client.V1VolumeMount(name='shm', mount_path='/dev/shm')]
    )]
    for sidecar in node_schema.sidecars:
        containers += sidecar.containers
        for configmap in sidecar.configmaps:
            configmap.metadata.namespace = node_schema.namespace
            configmap.metadata.owner_references = [owner_ref]
            try:
                k8s_corev1_api.create_namespaced_config_map_with_retry(namespace=node_schema.namespace, body=configmap)
            except ApiException as ae:
                if ae.status == 409:  # conflict
                    logger.info(f'configmap {configmap.metadata.name} already exits')
                else:
                    logger.exception(ae)
                    logger.f_error(f'configmap {configmap.metadata.name} init error: {ae}')
                    raise
        for volume in sidecar.volumes:
            volumes.append(volume)
        for extra_training_mount in sidecar.extra_training_mounts:
            containers[0].volume_mounts.append(extra_training_mount)
    # 为了启动 sbin init 的任务
    # todo 可能有安全问题
    if task.code_file == '/sbin/init':
        containers[0].command = ['/sbin/init']
        containers[0].args = []
    podspec = client.V1PodSpec(
        init_containers=init_containers,
        containers=containers,
        volumes=volumes + [client.V1Volume(name='shm', empty_dir=client.V1EmptyDirVolumeSource(medium='Memory'))],
        node_selector=node_schema['node_selector'],
        restart_policy='Never',
        enable_service_links=False,
        automount_service_account_token=False,
        host_pid=node_schema.host_pid,
        host_ipc=node_schema.host_ipc,
        host_network=node_schema.host_network,
        share_process_namespace=node_schema.share_process_namespace,
        tolerations=[
            client.V1Toleration(effect='NoExecute', key='node.kubernetes.io/memory-pressure', operator='Exists',)
        ]
    )
    annotations = {
        f'container.seccomp.security.alpha.kubernetes.io/{CONTAINER_NAME}': 'localhost/operator/default/hfai-experiment.json',
        f'container.apparmor.security.beta.kubernetes.io/{CONTAINER_NAME}': 'localhost/hfai-experiment'
    } if not user.is_internal else {}
    metadata = client.V1ObjectMeta(
        name=node_schema.pod_id,
        namespace=node_schema.namespace,
        labels=node_schema.labels,
        annotations=annotations,
        owner_references=[owner_ref]
    )
    pod = client.V1Pod(spec=podspec, api_version="v1", kind="Pod", metadata=metadata)
    try:
        pod = k8s_corev1_api.create_namespaced_pod_with_retry(namespace=node_schema.namespace, body=pod)
    except ApiException as ae:
        if ae.status == 409:  # conflict
            logger.info(f'pod {node_schema.pod_id} already exits')
        else:
            logger.exception(ae)
            logger.f_error(f'pod {node_schema.pod_id} init error: {ae}')
            raise
    create_master_network(rank, node_schema, user.is_internal)


def _create_node_impl(rank, node_schema):
    retries = 3
    threshold = 180 if task.task_type == TASK_TYPE.JUPYTER_TASK else 10  # jupyter任务init可以3分钟
    for i in range(1, retries+1):
        try:
            if task.pods[rank].status == EXP_STATUS.CREATED:
                create_node_in_k8s(rank, node_schema)
                # 将job的created态转化为building态
                task.pods[rank].update(('status',), ('building',))
            return
        except ApiException as e:
            if i == retries:
                raise
            else:
                logger.warning(f'[{rank}][{node_schema}] 第{i}次请求失败: {str(e)}, 等待2s后尝试重试...')  # 请求失败
                time.sleep(2)


@log_stage(log_id)
def create_node():
    logger.info('执行created_node，尝试创建计算节点')
    schema = munch.Munch.fromDict(task.build_schemas())
    logger.info('build schema success')  # build schema
    with ThreadPoolExecutor(max_workers=10) as thread_pool:
        futures = [thread_pool.submit(_create_node_impl, rank, node_schema) for rank, node_schema in enumerate(schema)]
        wait(futures)
        err_msg = [str(future.exception()) for future in futures if future.exception()]
        if err_msg:
            raise Exception(';'.join(err_msg))


@log_stage(log_id)
def create_services():
    """ 初始化服务的参数 """
    if len(task.schema.get('services', [])) == 0:
        logger.info('未配置 services, 跳过')
        return
    services = {}
    environments = {}
    for service in task.schema.get('services', []):
        service: TaskService = TaskService.parse_obj(service)
        if (service_config := CONF.jupyter.builtin_services.get(service.name)) is not None:
            service.port = service_config.get('port')
            service.type = service_config['type']
            service.startup_script = service_config.get('startup_script')
            service_environments = service_config.get('environ', {}).get(user.role, {})
            for env_name, env_value_template in service_environments.items():
                environments[env_name] = env_value_template \
                    .replace('{user_name}', user.user_name) \
                    .replace('{service_port}', str(service.port))
        services[service.name] = service.dict()
        services[service.name]['alive'] = False
        logger.info(f'service name [{service.name}] {services[service.name]}')
    svcs_with_script = {k: v for k, v in services.items() if v.get('startup_script')}
    environments['SERVICES'] = shlex.quote(ujson.dumps(svcs_with_script))
    TaskRuntimeConfig(task).insert(source='service_task', config_json={'services': services, 'version': CONF.jupyter.current_version})
    # 这里直接修改 task 实例的 schema 加入启动各项服务需要的信息, 因为每次启动都会生成这些 environ, 所以不需要写数据库持久化,
    # 只改一下当前 task 的属性用于 build schema 即可
    task.schema.setdefault('spec', {'environments': {}, 'workspace': '', 'entrypoint': 'stub.sh'})
    task.schema['spec'].setdefault('environments', {})
    task.schema['spec']['environments'].update(environments)
    task.schema['services'] = list(services.values())


with logger.contextualize(uuid=f'{log_id}.create_node'):
    try:
        logger.info(f'start create_node: ')
        create_services()
        create_node()
    except Exception as e:
        # 有异常，关闭掉任务
        task.re_impl(DbOperationImpl)
        task.update(('queue_status',), (QUE_STATUS.FINISHED,))
        with logger.contextualize(uuid=f'{log_id}.finally_exception'):
            logger.exception(e)
            logger.error(f'起{task.job_info}的service或是pod失败，错误编号为：{e}\n'
                         f'理想情况下后续起来的manager会将已经起来的pod杀光')
