import os
import traceback

from kubernetes import client
from kubernetes.client import ApiException
from kubernetes.client.models.v1_container_image import V1ContainerImage

from conf import MARS_GROUP_FLAG
from .k8s import get_corev1_api


def names(self, names):
    self._names = names


V1ContainerImage.names = V1ContainerImage.names.setter(names)

corev1 = get_corev1_api()


def node_gpu_num(node):
    return 8  # 强制是8


def set_node_label(node: str, key: str, value: str):
    try:
        ret = corev1.patch_node_with_retry(name=node, body=dict(metadata=dict(labels={key: value})))
        return True
    except Exception as e:
        traceback.print_exc()
        return False


def set_node_groups(node: str, *groups) -> bool:
    """

    @param node:
    @param groups: 多层 group 如，*[‘jd_all’, 'jd_a100', 'jd_a200'] 等等
    @return:
    """
    assert len(groups) >= 2, '必须设置 lv0 和 lv1 的 group'
    return set_node_label(node, MARS_GROUP_FLAG, '.'.join(groups))


def get_node_resource(resources, group=''):
    if os.environ.get('HAS_RDMA_HCA_RESOURCE', '1') == '1':
        limits = {'rdma/hca': 1}
        requests = {'rdma/hca': 1}
    else:
        limits = {}
        requests = {}
    for resource_item in resources:
        if resource_item == 'cpu':
            if 'limits' in resources.cpu:
                if 'requests' not in resources.cpu or resources.cpu.limits >= resources.cpu.requests:
                    limits['cpu'] = resources.cpu.limits
            if 'requests' in resources.cpu:
                requests['cpu'] = resources.cpu.requests
        if resource_item == 'memory':
            if 'limits' in resources.memory:
                limits['memory'] = resources.memory.limits
            if 'requests' in resources.memory:
                requests['memory'] = resources.memory.requests
    return client.V1ResourceRequirements(limits=limits, requests=requests)


def get_env_var(key, value):
    return client.V1EnvVar(name=str(key), value=str(value))


def create_nodeport_service(service_name: str, namespace: str, dist_port: int, selector: dict, src_port=None):
    # 先查询端口是否之前已经暴露
    try:
        result = corev1.read_namespaced_service_with_retry(namespace=namespace, name=service_name)
        if result:
            return {'port': result.spec.ports[0].node_port, 'existed': True}
    except ApiException as ae:
        if ae.status == 404:
            # 端口不存在，继续创建
            pass
        else:
            raise Exception(f'{service_name} 查询端口是否存在时出错') from ae

    metadata = client.V1ObjectMeta(name=service_name, namespace=namespace)
    spec = client.V1ServiceSpec(selector=selector,
                                ports=[client.V1ServicePort(port=dist_port, target_port=dist_port, node_port=src_port)],
                                type='NodePort')
    service = client.V1Service(api_version='v1',
                               kind='Service',
                               metadata=metadata,
                               spec=spec)
    result = corev1.create_namespaced_service_with_retry(namespace=namespace, body=service)
    if result is None:
        raise Exception(f'{service_name} service 创建失败')
    if src_port is None:
        # 未指定 src_port 时，获取 k8s 自动指定的 node_port
        src_port = result.spec.ports[0].node_port
    return {'port': src_port, 'existed': False}
