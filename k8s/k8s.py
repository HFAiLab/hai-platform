
import json
import random
import subprocess
import sys
import time
import os
from datetime import datetime
from functools import wraps
from http import HTTPStatus

import six
import sysv_ipc
import ujson
from kubernetes import client, config
from kubernetes.client.api_client import ApiClient
from kubernetes.client.exceptions import ApiTypeError, ApiValueError
from kubernetes.client.rest import ApiException
from kubernetes.leaderelection import leaderelection, electionconfig
from logm import logger
from conf import CONF


KUBECLIENTS = dict()
CURRENT_CLUSTER_HOST = os.environ[config.incluster_config.SERVICE_HOST_ENV_NAME]


def once(func):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return func(*args, **kwargs)

    wrapper.has_run = False
    return wrapper


def load_api_client(config_file=None):
    client_config = type.__call__(client.Configuration)
    if config_file:
        config.load_kube_config(config_file=config_file, client_configuration=client_config)
    else:
        config.load_incluster_config(client_configuration=client_config)
    return ApiClient(configuration=client_config)


@once
def load_all_api_client_cached():
    k8s_configs = CONF.try_get('k8s.config', default=[{'config_file': ''}])
    for i in k8s_configs:
        config_file = i.get('config_file', '')
        client = load_api_client(config_file)
        if config_file:
            try:
                service = Corev1ApiWithRetry(api_client=client).read_namespaced_service_with_retry(name='kubernetes', namespace='default')
                cluster_host = service.spec.cluster_ip
            except Exception as e:
                # ignore single cluster error
                logger.error(f'cluster {config_file} is not available: {e}!')
                continue
        else:
            cluster_host = CURRENT_CLUSTER_HOST
        KUBECLIENTS[cluster_host] = client
        logger.info(f'initialized {cluster_host} kubeclient')
    if CURRENT_CLUSTER_HOST not in KUBECLIENTS.keys():
        KUBECLIENTS[CURRENT_CLUSTER_HOST] = load_api_client()
        logger.info(f'initialized {CURRENT_CLUSTER_HOST} kubeclient')


def get_corev1_api(cluster_host = CURRENT_CLUSTER_HOST):
    load_all_api_client_cached()
    if cluster_host == 'all':
        return {host: Corev1ApiWithRetry(api_client=client) for host, client in KUBECLIENTS.items()}
    return Corev1ApiWithRetry(api_client=KUBECLIENTS[cluster_host])


def get_custom_corev1_api(cluster_host = CURRENT_CLUSTER_HOST):
    load_all_api_client_cached()
    if cluster_host == 'all':
        return {host: CustomCorev1Api(api_client=client) for host, client in KUBECLIENTS.items()}
    return CustomCorev1Api(api_client=KUBECLIENTS[cluster_host])


def get_appsv1_api(cluster_host = CURRENT_CLUSTER_HOST):
    load_all_api_client_cached()
    if cluster_host == 'all':
        return {host: AppsV1ApiWithRetry(api_client=client) for host, client in KUBECLIENTS.items()}
    return AppsV1ApiWithRetry(api_client=KUBECLIENTS[cluster_host])


def get_networkv1beta1_api(cluster_host = CURRENT_CLUSTER_HOST):
    load_all_api_client_cached()
    if cluster_host == 'all':
        return {host: NetworkingV1beta1ApiWithRetry(api_client=client) for host, client in KUBECLIENTS.items()}
    return NetworkingV1beta1ApiWithRetry(api_client=KUBECLIENTS[cluster_host])


class Backoff:
    def __init__(self, initial_duration=1, factor=2, jitter=0.2, steps=5,
                 max_duration=15):
        # wait interval in seconds, it's scaled by factor, limited by max_duration
        self.duration = initial_duration
        # scaling factor
        self.factor = factor
        # randomness factor of each interval
        self.jitter = jitter
        # the remaining number of iterations in which the duration may scale
        self.steps = steps
        # the the maximum duration in seconds
        self.max_duration = max_duration

    def jitter_func(self, duration, max_factor):
        if max_factor <= 0.0:
            max_factor = 1.0
        return duration + random.random() * max_factor * duration

    def step(self):
        if self.steps < 1:
            return self.jitter_func(self.duration,
                                    self.jitter) if self.jitter > 0 else self.duration

        self.steps -= 1
        duration = self.duration
        if self.factor > 0:
            self.duration = self.duration * self.factor
            if self.max_duration > 0 and self.duration > self.max_duration:
                self.duration = self.max_duration
                self.steps = 0

        if self.jitter > 0:
            duration = self.jitter_func(duration, self.jitter)
        return duration


def retry(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        retries = 0
        max_retries = 5
        backoff = Backoff()
        last_result = None
        while True:
            try:
                retries += 1
                last_result = func(*args, **kwargs)
                break
            except Exception as e:
                if retries >= max_retries:
                    logger.error(
                        f"max retries exceeded with exception: {str(e)}.")
                    raise e
                wait = backoff.step()
                logger.error(
                    f'retries {retries} got exception: {str(e)}, retry after: {wait}s.')
                time.sleep(wait)
                continue
        return last_result

    return wrapper


class Corev1ApiWithRetry(client.CoreV1Api):
    def __init__(self, api_client=None):
        super().__init__(api_client)

    @retry
    def list_namespaced_pod_with_retry(self, namespace, **kwargs):
        return self.list_namespaced_pod(namespace, **kwargs)

    @retry
    def read_namespaced_pod_with_retry(self, name, namespace, **kwargs):
        try:
            return self.read_namespaced_pod(name, namespace, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae

    @retry
    def read_namespaced_pod_status_with_retry(self, name, namespace, **kwargs):
        try:
            return self.read_namespaced_pod_status(name, namespace, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae

    @retry
    def patch_namespaced_pod_with_retry(self, name, namespace, body, **kwargs):
        try:
            return self.patch_namespaced_pod(name, namespace, body, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae

    @retry
    def create_namespaced_pod_with_retry(self, namespace, body, **kwargs):
        try:
            return self.create_namespaced_pod(namespace, body, **kwargs)
        except ApiException as ae:
            if ae.status == 409:
                return None
            raise ae

    @retry
    def delete_namespaced_pod_with_retry(self, name, namespace, **kwargs):
        try:
            return self.delete_namespaced_pod(name, namespace, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae

    @retry
    def list_node_with_retry(self, **kwargs):
        return self.list_node(**kwargs)

    @retry
    def read_node_with_retry(self, name, **kwargs):
        try:
            return self.read_node(name, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae

    @retry
    def patch_node_with_retry(self, name, body, **kwargs):
        try:
            return self.patch_node(name, body, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae

    @retry
    def read_namespaced_service_with_retry(self, name, namespace, **kwargs):
        try:
            return self.read_namespaced_service(name, namespace, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae

    @retry
    def create_namespaced_service_with_retry(self, namespace, body, **kwargs):
        try:
            return self.create_namespaced_service(namespace, body, **kwargs)
        except ApiException as ae:
            if ae.status == 409:
                return None
            raise ae

    @retry
    def delete_namespaced_service_with_retry(self, name, namespace, **kwargs):
        try:
            return self.delete_namespaced_service(name, namespace, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae

    @retry
    def read_namespaced_config_map_with_retry(self, name, namespace, **kwargs):
        try:
            return self.read_namespaced_config_map(name, namespace, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae

    @retry
    def create_namespaced_config_map_with_retry(self, namespace, body, **kwargs):
        try:
            return self.create_namespaced_config_map(namespace, body, **kwargs)
        except ApiException as ae:
            if ae.status == 409:
                return None
            raise ae

    @retry
    def delete_namespaced_config_map_with_retry(self, name, namespace, **kwargs):
        try:
            return self.delete_namespaced_config_map(name, namespace, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae

    @retry
    def list_namespaced_event_with_retry(self, namespace, **kwargs):
        return self.list_namespaced_event(namespace, **kwargs)


class AppsV1ApiWithRetry(client.AppsV1Api):
    def __init__(self, api_client=None):
        super().__init__(api_client)

    @retry
    def create_namespaced_stateful_set_with_retry(self, namespace, body,
                                                  **kwargs):
        try:
            return self.create_namespaced_stateful_set(namespace, body, **kwargs)
        except ApiException as ae:
            if ae.status == 409:
                return None
            raise ae

    @retry
    def delete_namespaced_stateful_set_with_retry(self, name, namespace,
                                                  **kwargs):
        try:
            return self.delete_namespaced_stateful_set(name, namespace, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae

    @retry
    def read_namespaced_stateful_set_with_retry(self, name, namespace, **kwargs):
        try:
            return self.read_namespaced_stateful_set(name, namespace, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae


class NetworkingV1beta1ApiWithRetry(client.NetworkingV1beta1Api):
    def __init__(self, api_client=None):
        super().__init__(api_client)

    @retry
    def read_namespaced_ingress_with_retry(self, name, namespace, **kwargs):
        try:
            return self.read_namespaced_ingress(name, namespace, **kwargs)
        except ApiException as ae:
            if ae.status == 404:
                return None
            raise ae

    @retry
    def create_namespaced_ingress_with_retry(self, namespace, body, **kwargs):
        try:
            return self.create_namespaced_ingress(namespace, body, **kwargs)
        except ApiException as ae:
            if ae.status == 409:
                return None
            raise ae


class CustomCorev1Api(client.CoreV1Api):
    '''
    使用protobuf做编解码, 降低apiserver开销
    client端不解析response为实际的api object, 直接返回原始dict数据, 以提升响应时间
    注: python client目前不支持protobuf, 暂时通过调用binary做protobuf decode作为workaround;
        所有使用CustomCorev1Api返回的dict key均为默认camelcase, 与python默认的下划线格式不一致, 可使用get_k8s_dict_val兼容
    '''

    def __init__(self, api_client=None):
        super().__init__(api_client)

    @retry
    def list_namespaced_pod_with_retry(self, namespace, **kwargs):
        return self.list_namespaced_pod(namespace, **kwargs)

    @retry
    def list_node_with_retry(self, **kwargs):
        return self.list_node(**kwargs)

    def list_namespaced_pod_with_http_info(self, namespace,
                                           **kwargs):  # noqa: E501
        """list_namespaced_pod  # noqa: E501

        list or watch objects of kind Pod  # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.list_namespaced_pod_with_http_info(namespace, async_req=True)
        >>> result = thread.get()

        :param async_req bool: execute request asynchronously
        :param str namespace: object name and auth scope, such as for teams and projects (required)
        :param str pretty: If 'true', then the output is pretty printed.
        :param bool allow_watch_bookmarks: allowWatchBookmarks requests watch events with type \"BOOKMARK\". Servers that do not implement bookmarks may ignore this flag and bookmarks are sent at the server's discretion. Clients should not assume bookmarks are returned at any specific interval, nor may they assume the server will send any BOOKMARK event during a session. If this is not a watch, this field is ignored. If the feature gate WatchBookmarks is not enabled in apiserver, this field is ignored.
        :param str _continue: The continue option should be set when retrieving more results from the server. Since this value is server defined, clients may only use the continue value from a previous query result with identical query parameters (except for the value of continue) and the server may reject a continue value it does not recognize. If the specified continue value is no longer valid whether due to expiration (generally five to fifteen minutes) or a configuration change on the server, the server will respond with a 410 ResourceExpired error together with a continue token. If the client needs a consistent list, it must restart their list without the continue field. Otherwise, the client may send another list request with the token received with the 410 error, the server will respond with a list starting from the next key, but from the latest snapshot, which is inconsistent from the previous list results - objects that are created, modified, or deleted after the first list request will be included in the response, as long as their keys are after the \"next key\".  This field is not supported when watch is true. Clients may start a watch from the last resourceVersion value returned by the server and not miss any modifications.
        :param str field_selector: A selector to restrict the list of returned objects by their fields. Defaults to everything.
        :param str label_selector: A selector to restrict the list of returned objects by their labels. Defaults to everything.
        :param int limit: limit is a maximum number of responses to return for a list call. If more items exist, the server will set the `continue` field on the list metadata to a value that can be used with the same initial query to retrieve the next set of results. Setting a limit may return fewer than the requested amount of items (up to zero items) in the event all requested objects are filtered out and clients should only use the presence of the continue field to determine whether more results are available. Servers may choose not to support the limit argument and will return all of the available results. If limit is specified and the continue field is empty, clients may assume that no more results are available. This field is not supported if watch is true.  The server guarantees that the objects returned when using continue will be identical to issuing a single list call without a limit - that is, no objects created, modified, or deleted after the first request is issued will be included in any subsequent continued requests. This is sometimes referred to as a consistent snapshot, and ensures that a client that is using limit to receive smaller chunks of a very large result can ensure they see all possible objects. If objects are updated during a chunked list the version of the object that was present at the time the first list result was calculated is returned.
        :param str resource_version: When specified with a watch call, shows changes that occur after that particular version of a resource. Defaults to changes from the beginning of history. When specified for list: - if unset, then the result is returned from remote storage based on quorum-read flag; - if it's 0, then we simply return what we currently have in cache, no guarantee; - if set to non zero, then the result is at least as fresh as given rv.
        :param int timeout_seconds: Timeout for the list/watch call. This limits the duration of the call, regardless of any activity or inactivity.
        :param bool watch: Watch for changes to the described resources and return them as a stream of add, update, and remove notifications. Specify resourceVersion.
        :param _return_http_data_only: response data without head status code
                                       and headers
        :param _preload_content: if False, the urllib3.HTTPResponse object will
                                 be returned without reading/decoding response
                                 data. Default is True.
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :return: tuple(V1PodList, status_code(int), headers(HTTPHeaderDict))
                 If the method is called asynchronously,
                 returns the request thread.
        """

        local_var_params = locals()

        all_params = [
            'namespace',
            'pretty',
            'allow_watch_bookmarks',
            '_continue',
            'field_selector',
            'label_selector',
            'limit',
            'resource_version',
            'timeout_seconds',
            'watch'
        ]
        all_params.extend(
            [
                'async_req',
                '_return_http_data_only',
                '_preload_content',
                '_request_timeout'
            ]
        )

        for key, val in six.iteritems(local_var_params['kwargs']):
            if key not in all_params:
                raise ApiTypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method list_namespaced_pod" % key
                )
            local_var_params[key] = val
        del local_var_params['kwargs']
        # verify the required parameter 'namespace' is set
        if self.api_client.client_side_validation and (
                'namespace' not in local_var_params or  # noqa: E501
                local_var_params['namespace'] is None):  # noqa: E501
            raise ApiValueError(
                "Missing the required parameter `namespace` when calling `list_namespaced_pod`")  # noqa: E501

        collection_formats = {}

        path_params = {}
        if 'namespace' in local_var_params:
            path_params['namespace'] = local_var_params[
                'namespace']  # noqa: E501

        query_params = []
        if 'pretty' in local_var_params and local_var_params[
            'pretty'] is not None:  # noqa: E501
            query_params.append(
                ('pretty', local_var_params['pretty']))  # noqa: E501
        if 'allow_watch_bookmarks' in local_var_params and local_var_params[
            'allow_watch_bookmarks'] is not None:  # noqa: E501
            query_params.append(('allowWatchBookmarks', local_var_params[
                'allow_watch_bookmarks']))  # noqa: E501
        if '_continue' in local_var_params and local_var_params[
            '_continue'] is not None:  # noqa: E501
            query_params.append(
                ('continue', local_var_params['_continue']))  # noqa: E501
        if 'field_selector' in local_var_params and local_var_params[
            'field_selector'] is not None:  # noqa: E501
            query_params.append(('fieldSelector', local_var_params[
                'field_selector']))  # noqa: E501
        if 'label_selector' in local_var_params and local_var_params[
            'label_selector'] is not None:  # noqa: E501
            query_params.append(('labelSelector', local_var_params[
                'label_selector']))  # noqa: E501
        if 'limit' in local_var_params and local_var_params[
            'limit'] is not None:  # noqa: E501
            query_params.append(
                ('limit', local_var_params['limit']))  # noqa: E501
        if 'resource_version' in local_var_params and local_var_params[
            'resource_version'] is not None:  # noqa: E501
            query_params.append(('resourceVersion', local_var_params[
                'resource_version']))  # noqa: E501
        if 'timeout_seconds' in local_var_params and local_var_params[
            'timeout_seconds'] is not None:  # noqa: E501
            query_params.append(('timeoutSeconds', local_var_params[
                'timeout_seconds']))  # noqa: E501
        if 'watch' in local_var_params and local_var_params[
            'watch'] is not None:  # noqa: E501
            query_params.append(
                ('watch', local_var_params['watch']))  # noqa: E501

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = 'application/vnd.kubernetes.protobuf'
        # Authentication setting
        auth_settings = ['BearerToken']  # noqa: E501
        ret = self.api_client.call_api(
            '/api/v1/namespaces/{namespace}/pods', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='V1PodList',  # noqa: E501
            auth_settings=auth_settings,
            async_req=local_var_params.get('async_req'),
            _return_http_data_only=local_var_params.get(
                '_return_http_data_only'),  # noqa: E501
            _preload_content=local_var_params.get('_preload_content', False),
            _request_timeout=local_var_params.get('_request_timeout'),
            collection_formats=collection_formats)
        out = subprocess.run('decode-protobuf-camel', input=ret.data,
                             capture_output=True)
        result = ujson.loads(out.stdout.decode())
        if result.get('items', None) is None:
            result['items'] = []
        return result

    def list_node_with_http_info(self, **kwargs):  # noqa: E501
        """list_node  # noqa: E501

        list or watch objects of kind Node  # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.list_node_with_http_info(async_req=True)
        >>> result = thread.get()

        :param async_req bool: execute request asynchronously
        :param str pretty: If 'true', then the output is pretty printed.
        :param bool allow_watch_bookmarks: allowWatchBookmarks requests watch events with type \"BOOKMARK\". Servers that do not implement bookmarks may ignore this flag and bookmarks are sent at the server's discretion. Clients should not assume bookmarks are returned at any specific interval, nor may they assume the server will send any BOOKMARK event during a session. If this is not a watch, this field is ignored. If the feature gate WatchBookmarks is not enabled in apiserver, this field is ignored.
        :param str _continue: The continue option should be set when retrieving more results from the server. Since this value is server defined, clients may only use the continue value from a previous query result with identical query parameters (except for the value of continue) and the server may reject a continue value it does not recognize. If the specified continue value is no longer valid whether due to expiration (generally five to fifteen minutes) or a configuration change on the server, the server will respond with a 410 ResourceExpired error together with a continue token. If the client needs a consistent list, it must restart their list without the continue field. Otherwise, the client may send another list request with the token received with the 410 error, the server will respond with a list starting from the next key, but from the latest snapshot, which is inconsistent from the previous list results - objects that are created, modified, or deleted after the first list request will be included in the response, as long as their keys are after the \"next key\".  This field is not supported when watch is true. Clients may start a watch from the last resourceVersion value returned by the server and not miss any modifications.
        :param str field_selector: A selector to restrict the list of returned objects by their fields. Defaults to everything.
        :param str label_selector: A selector to restrict the list of returned objects by their labels. Defaults to everything.
        :param int limit: limit is a maximum number of responses to return for a list call. If more items exist, the server will set the `continue` field on the list metadata to a value that can be used with the same initial query to retrieve the next set of results. Setting a limit may return fewer than the requested amount of items (up to zero items) in the event all requested objects are filtered out and clients should only use the presence of the continue field to determine whether more results are available. Servers may choose not to support the limit argument and will return all of the available results. If limit is specified and the continue field is empty, clients may assume that no more results are available. This field is not supported if watch is true.  The server guarantees that the objects returned when using continue will be identical to issuing a single list call without a limit - that is, no objects created, modified, or deleted after the first request is issued will be included in any subsequent continued requests. This is sometimes referred to as a consistent snapshot, and ensures that a client that is using limit to receive smaller chunks of a very large result can ensure they see all possible objects. If objects are updated during a chunked list the version of the object that was present at the time the first list result was calculated is returned.
        :param str resource_version: resourceVersion sets a constraint on what resource versions a request may be served from. See https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions for details.  Defaults to unset
        :param str resource_version_match: resourceVersionMatch determines how resourceVersion is applied to list calls. It is highly recommended that resourceVersionMatch be set for list calls where resourceVersion is set See https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions for details.  Defaults to unset
        :param int timeout_seconds: Timeout for the list/watch call. This limits the duration of the call, regardless of any activity or inactivity.
        :param bool watch: Watch for changes to the described resources and return them as a stream of add, update, and remove notifications. Specify resourceVersion.
        :param _return_http_data_only: response data without head status code
                                       and headers
        :param _preload_content: if False, the urllib3.HTTPResponse object will
                                 be returned without reading/decoding response
                                 data. Default is True.
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :return: tuple(V1NodeList, status_code(int), headers(HTTPHeaderDict))
                 If the method is called asynchronously,
                 returns the request thread.
        """

        local_var_params = locals()

        all_params = [
            'pretty',
            'allow_watch_bookmarks',
            '_continue',
            'field_selector',
            'label_selector',
            'limit',
            'resource_version',
            'resource_version_match',
            'timeout_seconds',
            'watch'
        ]
        all_params.extend(
            [
                'async_req',
                '_return_http_data_only',
                '_preload_content',
                '_request_timeout'
            ]
        )

        for key, val in six.iteritems(local_var_params['kwargs']):
            if key not in all_params:
                raise ApiTypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method list_node" % key
                )
            local_var_params[key] = val
        del local_var_params['kwargs']

        collection_formats = {}

        path_params = {}

        query_params = []
        if 'pretty' in local_var_params and local_var_params[
            'pretty'] is not None:  # noqa: E501
            query_params.append(
                ('pretty', local_var_params['pretty']))  # noqa: E501
        if 'allow_watch_bookmarks' in local_var_params and local_var_params[
            'allow_watch_bookmarks'] is not None:  # noqa: E501
            query_params.append(('allowWatchBookmarks', local_var_params[
                'allow_watch_bookmarks']))  # noqa: E501
        if '_continue' in local_var_params and local_var_params[
            '_continue'] is not None:  # noqa: E501
            query_params.append(
                ('continue', local_var_params['_continue']))  # noqa: E501
        if 'field_selector' in local_var_params and local_var_params[
            'field_selector'] is not None:  # noqa: E501
            query_params.append(('fieldSelector', local_var_params[
                'field_selector']))  # noqa: E501
        if 'label_selector' in local_var_params and local_var_params[
            'label_selector'] is not None:  # noqa: E501
            query_params.append(('labelSelector', local_var_params[
                'label_selector']))  # noqa: E501
        if 'limit' in local_var_params and local_var_params[
            'limit'] is not None:  # noqa: E501
            query_params.append(
                ('limit', local_var_params['limit']))  # noqa: E501
        if 'resource_version' in local_var_params and local_var_params[
            'resource_version'] is not None:  # noqa: E501
            query_params.append(('resourceVersion', local_var_params[
                'resource_version']))  # noqa: E501
        if 'resource_version_match' in local_var_params and local_var_params[
            'resource_version_match'] is not None:  # noqa: E501
            query_params.append(('resourceVersionMatch', local_var_params[
                'resource_version_match']))  # noqa: E501
        if 'timeout_seconds' in local_var_params and local_var_params[
            'timeout_seconds'] is not None:  # noqa: E501
            query_params.append(('timeoutSeconds', local_var_params[
                'timeout_seconds']))  # noqa: E501
        if 'watch' in local_var_params and local_var_params[
            'watch'] is not None:  # noqa: E501
            query_params.append(
                ('watch', local_var_params['watch']))  # noqa: E501

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = 'application/vnd.kubernetes.protobuf'

        # Authentication setting
        auth_settings = ['BearerToken']  # noqa: E501

        ret = self.api_client.call_api(
            '/api/v1/nodes', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='V1NodeList',  # noqa: E501
            auth_settings=auth_settings,
            async_req=local_var_params.get('async_req'),
            _return_http_data_only=local_var_params.get(
                '_return_http_data_only'),  # noqa: E501
            _preload_content=local_var_params.get('_preload_content', False),
            _request_timeout=local_var_params.get('_request_timeout'),
            collection_formats=collection_formats)
        out = subprocess.run('decode-protobuf-camel', input=ret.data,
                             capture_output=True)
        result = ujson.loads(out.stdout.decode())
        if result.get('items', None) is None:
            result['items'] = []
        return result

    def list_namespaced_event_with_http_info(self, namespace, **kwargs):  # noqa: E501
        """list_namespaced_event  # noqa: E501

        list or watch objects of kind Event  # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.list_namespaced_event_with_http_info(namespace, async_req=True)
        >>> result = thread.get()

        :param async_req bool: execute request asynchronously
        :param str namespace: object name and auth scope, such as for teams and projects (required)
        :param str pretty: If 'true', then the output is pretty printed.
        :param bool allow_watch_bookmarks: allowWatchBookmarks requests watch events with type \"BOOKMARK\". Servers that do not implement bookmarks may ignore this flag and bookmarks are sent at the server's discretion. Clients should not assume bookmarks are returned at any specific interval, nor may they assume the server will send any BOOKMARK event during a session. If this is not a watch, this field is ignored. If the feature gate WatchBookmarks is not enabled in apiserver, this field is ignored.
        :param str _continue: The continue option should be set when retrieving more results from the server. Since this value is server defined, clients may only use the continue value from a previous query result with identical query parameters (except for the value of continue) and the server may reject a continue value it does not recognize. If the specified continue value is no longer valid whether due to expiration (generally five to fifteen minutes) or a configuration change on the server, the server will respond with a 410 ResourceExpired error together with a continue token. If the client needs a consistent list, it must restart their list without the continue field. Otherwise, the client may send another list request with the token received with the 410 error, the server will respond with a list starting from the next key, but from the latest snapshot, which is inconsistent from the previous list results - objects that are created, modified, or deleted after the first list request will be included in the response, as long as their keys are after the \"next key\".  This field is not supported when watch is true. Clients may start a watch from the last resourceVersion value returned by the server and not miss any modifications.
        :param str field_selector: A selector to restrict the list of returned objects by their fields. Defaults to everything.
        :param str label_selector: A selector to restrict the list of returned objects by their labels. Defaults to everything.
        :param int limit: limit is a maximum number of responses to return for a list call. If more items exist, the server will set the `continue` field on the list metadata to a value that can be used with the same initial query to retrieve the next set of results. Setting a limit may return fewer than the requested amount of items (up to zero items) in the event all requested objects are filtered out and clients should only use the presence of the continue field to determine whether more results are available. Servers may choose not to support the limit argument and will return all of the available results. If limit is specified and the continue field is empty, clients may assume that no more results are available. This field is not supported if watch is true.  The server guarantees that the objects returned when using continue will be identical to issuing a single list call without a limit - that is, no objects created, modified, or deleted after the first request is issued will be included in any subsequent continued requests. This is sometimes referred to as a consistent snapshot, and ensures that a client that is using limit to receive smaller chunks of a very large result can ensure they see all possible objects. If objects are updated during a chunked list the version of the object that was present at the time the first list result was calculated is returned.
        :param str resource_version: When specified with a watch call, shows changes that occur after that particular version of a resource. Defaults to changes from the beginning of history. When specified for list: - if unset, then the result is returned from remote storage based on quorum-read flag; - if it's 0, then we simply return what we currently have in cache, no guarantee; - if set to non zero, then the result is at least as fresh as given rv.
        :param int timeout_seconds: Timeout for the list/watch call. This limits the duration of the call, regardless of any activity or inactivity.
        :param bool watch: Watch for changes to the described resources and return them as a stream of add, update, and remove notifications. Specify resourceVersion.
        :param _return_http_data_only: response data without head status code
                                       and headers
        :param _preload_content: if False, the urllib3.HTTPResponse object will
                                 be returned without reading/decoding response
                                 data. Default is True.
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :return: tuple(V1EventList, status_code(int), headers(HTTPHeaderDict))
                 If the method is called asynchronously,
                 returns the request thread.
        """

        local_var_params = locals()

        all_params = [
            'namespace',
            'pretty',
            'allow_watch_bookmarks',
            '_continue',
            'field_selector',
            'label_selector',
            'limit',
            'resource_version',
            'timeout_seconds',
            'watch'
        ]
        all_params.extend(
            [
                'async_req',
                '_return_http_data_only',
                '_preload_content',
                '_request_timeout'
            ]
        )

        for key, val in six.iteritems(local_var_params['kwargs']):
            if key not in all_params:
                raise ApiTypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method list_namespaced_event" % key
                )
            local_var_params[key] = val
        del local_var_params['kwargs']
        # verify the required parameter 'namespace' is set
        if self.api_client.client_side_validation and ('namespace' not in local_var_params or  # noqa: E501
                                                        local_var_params['namespace'] is None):  # noqa: E501
            raise ApiValueError("Missing the required parameter `namespace` when calling `list_namespaced_event`")  # noqa: E501

        collection_formats = {}

        path_params = {}
        if 'namespace' in local_var_params:
            path_params['namespace'] = local_var_params['namespace']  # noqa: E501

        query_params = []
        if 'pretty' in local_var_params and local_var_params['pretty'] is not None:  # noqa: E501
            query_params.append(('pretty', local_var_params['pretty']))  # noqa: E501
        if 'allow_watch_bookmarks' in local_var_params and local_var_params['allow_watch_bookmarks'] is not None:  # noqa: E501
            query_params.append(('allowWatchBookmarks', local_var_params['allow_watch_bookmarks']))  # noqa: E501
        if '_continue' in local_var_params and local_var_params['_continue'] is not None:  # noqa: E501
            query_params.append(('continue', local_var_params['_continue']))  # noqa: E501
        if 'field_selector' in local_var_params and local_var_params['field_selector'] is not None:  # noqa: E501
            query_params.append(('fieldSelector', local_var_params['field_selector']))  # noqa: E501
        if 'label_selector' in local_var_params and local_var_params['label_selector'] is not None:  # noqa: E501
            query_params.append(('labelSelector', local_var_params['label_selector']))  # noqa: E501
        if 'limit' in local_var_params and local_var_params['limit'] is not None:  # noqa: E501
            query_params.append(('limit', local_var_params['limit']))  # noqa: E501
        if 'resource_version' in local_var_params and local_var_params['resource_version'] is not None:  # noqa: E501
            query_params.append(('resourceVersion', local_var_params['resource_version']))  # noqa: E501
        if 'timeout_seconds' in local_var_params and local_var_params['timeout_seconds'] is not None:  # noqa: E501
            query_params.append(('timeoutSeconds', local_var_params['timeout_seconds']))  # noqa: E501
        if 'watch' in local_var_params and local_var_params['watch'] is not None:  # noqa: E501
            query_params.append(('watch', local_var_params['watch']))  # noqa: E501

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = 'application/vnd.kubernetes.protobuf'

        # Authentication setting
        auth_settings = ['BearerToken']  # noqa: E501

        ret = self.api_client.call_api(
            '/api/v1/namespaces/{namespace}/events', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='V1EventList',  # noqa: E501
            auth_settings=auth_settings,
            async_req=local_var_params.get('async_req'),
            _return_http_data_only=local_var_params.get('_return_http_data_only'),  # noqa: E501
            _preload_content=local_var_params.get('_preload_content', False),
            _request_timeout=local_var_params.get('_request_timeout'),
            collection_formats=collection_formats)
        out = subprocess.run('decode-protobuf-camel', input=ret.data,
                             capture_output=True)
        result = ujson.loads(out.stdout.decode())
        if result.get('items', None) is None:
            result['items'] = []
        return result


class K8sPreStopHook(object):
    PRE_STOP_SHM_ID = 237965298
    __shm: sysv_ipc.SharedMemory = None

    @classmethod
    def shm(cls) -> sysv_ipc.SharedMemory:
        if cls.__shm is not None:
            return cls.__shm
        try:
            cls.__shm = sysv_ipc.SharedMemory(cls.PRE_STOP_SHM_ID,
                                              sysv_ipc.IPC_CREX, mode=0o777,
                                              size=1)
            cls.__shm.write(b'0')
        except sysv_ipc.ExistentialError:
            cls.__shm = sysv_ipc.SharedMemory(cls.PRE_STOP_SHM_ID)
        return cls.__shm

    @classmethod
    def write_stop_pod(cls):
        cls.shm().write(b'1')

    @classmethod
    def receive_stop_pod(cls):
        return cls.shm().read() == b'1'


class LeaderElectionConfig(electionconfig.Config):
    def __init__(self, *args, keep_leading=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.keep_leading = keep_leading


class LeaderElection(leaderelection.LeaderElection):
    def __init__(self, election_config: LeaderElectionConfig):
        super().__init__(election_config)

    def renew_loop(self):
        # Leader
        logger.info("Leader has entered renew loop and will try to update lease continuously")

        retry_period = self.election_config.retry_period
        renew_deadline = self.election_config.renew_deadline * 1000

        while self.election_config.keep_leading is None or self.election_config.keep_leading():
            timeout = int(time.time() * 1000) + renew_deadline
            succeeded = False

            while int(time.time() * 1000) < timeout:
                succeeded = self.try_acquire_or_renew()

                if succeeded:
                    break
                time.sleep(retry_period)

            if succeeded:
                time.sleep(retry_period)
                continue

            # failed to renew, return
            return

    def update_lock(self, leader_election_record):
        # Update object with latest election record
        update_status = self.election_config.lock.update(
            self.election_config.lock.name,
            self.election_config.lock.namespace,
            leader_election_record)

        if update_status is False:
            logger.info("{} failed to acquire lease".format(
                leader_election_record.holder_identity))
            return False

        self.observed_record = leader_election_record
        self.observed_time_milliseconds = int(time.time() * 1000)
        logger.debug("leader {} has successfully acquired lease".format(
            leader_election_record.holder_identity))
        return True

    # remove unused logging
    def try_acquire_or_renew(self):
        now_timestamp = time.time()
        now = datetime.fromtimestamp(now_timestamp)

        # Check if lock is created
        lock_status, old_election_record = self.election_config.lock.get(
            self.election_config.lock.name,
            self.election_config.lock.namespace)

        # create a default Election record for this candidate
        leader_election_record = leaderelection.LeaderElectionRecord(
            self.election_config.lock.identity,
            str(self.election_config.lease_duration), str(now), str(now))

        # A lock is not created with that name, try to create one
        if not lock_status:
            # To be removed when support for python2 will be removed
            if sys.version_info > (3, 0):
                if json.loads(old_election_record.body)['code'] != HTTPStatus.NOT_FOUND:
                    logger.error(
                        "Error retrieving resource lock {} as {}".format(
                            self.election_config.lock.name,
                            old_election_record.reason))
                    return False
            else:
                if json.loads(old_election_record.body)['code'] != HTTPStatus.NOT_FOUND:
                    logger.error(
                        "Error retrieving resource lock {} as {}".format(
                            self.election_config.lock.name,
                            old_election_record.reason))
                    return False

            logger.info("{} is trying to create a lock".format(
                leader_election_record.holder_identity))
            create_status = self.election_config.lock.create(
                name=self.election_config.lock.name,
                namespace=self.election_config.lock.namespace,
                election_record=leader_election_record)

            if create_status is False:
                logger.error("{} Failed to create lock".format(
                    leader_election_record.holder_identity))
                return False

            self.observed_record = leader_election_record
            self.observed_time_milliseconds = int(time.time() * 1000)
            return True

        # A lock exists with that name
        # Validate old_election_record
        if old_election_record is None:
            # try to update lock with proper annotation and election record
            return self.update_lock(leader_election_record)

        if (
                old_election_record.holder_identity is None or old_election_record.lease_duration is None
                or old_election_record.acquire_time is None or old_election_record.renew_time is None):
            # try to update lock with proper annotation and election record
            return self.update_lock(leader_election_record)

        # Report transitions
        if self.observed_record and self.observed_record.holder_identity != old_election_record.holder_identity:
            logger.info("Leader has switched to {}".format(
                old_election_record.holder_identity))

        if self.observed_record is None or old_election_record.__dict__ != self.observed_record.__dict__:
            self.observed_record = old_election_record
            self.observed_time_milliseconds = int(time.time() * 1000)

        # If This candidate is not the leader and lease duration is yet to finish
        if (
                self.election_config.lock.identity != self.observed_record.holder_identity
                and self.observed_time_milliseconds + self.election_config.lease_duration * 1000 > int(
            now_timestamp * 1000)):
            # logging.info("yet to finish lease_duration, lease held by {} and has not expired".format(old_election_record.holder_identity))
            return False

        # If this candidate is the Leader
        if self.election_config.lock.identity == self.observed_record.holder_identity:
            # Leader updates renewTime, but keeps acquire_time unchanged
            leader_election_record.acquire_time = self.observed_record.acquire_time

        return self.update_lock(leader_election_record)


def get_k8s_dict_val(data, key):
    # 兼容under_score和camel两种key格式
    val = data.get(key, None)
    if not val:
        if '_' in key:
            other_key = key[0] + key.replace('_', ' ').title().replace(' ', '')[
                                 1:]
        else:
            other_key = ''.join(
                ['_' + c.lower() if c.isupper() else c for c in key]).lstrip('_')
        val = data.get(other_key, None)
    return val
