

from .default import *
from .custom import *


import api.task.experiment as at_exp
import api.task.port as at_port
import api.task.service_task as at_service_task
import api.user.admin as au_admin
import api.user.external as au_ext
import api.user.user as au_user
import api.resource.cluster as ar_cluster
import api.training as a_train
import api.query.optimized.task as aq_optimized_task
import api.query.optimized.user as aq_optimized_user
import api.query.optimized.resource as aq_optimized_resource
import api.query.optimized.service_task as aq_optimized_service_task
import api.user.access as au_access
import api.resource.cloud_storage as ar_cloud_storage
import api.resource.storage as ar_storage


if 'operating' in REG_SERVERS:
    app.post('/operating/task/create')(at_exp.create_task_v2)
    app.post('/operating/task/resume')(at_exp.resume_task)
    app.post('/operating/task/stop')(at_exp.stop_task)
    app.post('/operating/task/suspend')(at_exp.suspend_task_by_name)
    app.post('/operating/task/tag')(at_exp.tag_task)
    app.post('/operating/task/untag')(at_exp.untag_task)
    app.post('/operating/task/fail')(a_train.fail_task)
    app.post('/operating/task/priority/update')(at_exp.update_priority)
    app.post('/operating/task/group/update')(at_exp.switch_group)
    app.post('/operating/task/service_control')(at_exp.service_control_api)
    app.post('/operating/task/restart_log/set')(at_exp.set_task_restart_log_api)

    app.post('/operating/user/tag/delete')(at_exp.delete_tags)
    app.post('/operating/user/training_quota/update')(au_user.set_user_gpu_quota)
    app.post('/operating/user/training_quota_limit/update')(au_admin.set_user_gpu_quota_limit)
    app.post('/operating/user/access_token/create')(au_access.create_access_token)
    app.post('/operating/user/access_token/delete')(au_access.delete_access_token)
    app.post('/operating/user/active/update')(au_admin.set_user_active_state_api)
    app.post('/operating/user/create')(au_admin.create_user_api)
    app.post('/operating/user/group/update')(au_admin.update_user_group)

    app.post('/operating/service_task/delete')(at_service_task.delete_task_api)
    app.post('/operating/service_task/move_node')(at_service_task.move_node_api)

    app.post('/operating/node/state/update')(ar_cluster.change_node_state_api)
    app.post('/operating/node/host_info/update')(ar_cluster.update_host_info_api)
    app.post('/operating/node/host_info/create')(ar_cluster.create_host_info_api)
    app.post('/operating/node/host_info/delete')(ar_cluster.delete_host_info_api)
    app.post('/operating/node/label')(ar_cluster.label_node_api)


if 'ugc' in REG_SERVERS:
    app.post('/ugc/user/nodeport/create')(at_port.node_port_svc)
    app.post('/ugc/user/nodeport/delete')(at_port.delete_node_port_svc)
    app.post('/ugc/user/train_image/list')(aq_optimized_resource.get_train_images)

    app.post('/ugc/cloud/cluster_files/list')(ar_cloud_storage.list_cluster_files)


if 'query' in REG_SERVERS:
    app.post('/query/task')(aq_optimized_task.get_task_api)
    app.post('/query/task/log')(at_exp.task_node_log_api)
    app.post('/query/task/sys_log')(at_exp.task_sys_log_api)
    app.post('/query/task/log/search')(at_exp.task_search_in_global)
    app.post('/query/task/ssh_ip')(at_port.task_ssh_ip)
    app.post('/query/task/list')(aq_optimized_task.get_tasks_api)
    app.post('/query/task/list_all_unfinished')(aq_optimized_task.get_running_tasks_api)
    app.post('/query/task/list_all_with_priority')(aq_optimized_task.get_tasks_overview)
    app.post('/query/task/container_monitor_stats/list')(aq_optimized_task.get_task_container_monitor_stats_api)
    app.post('/query/task/time_range_overview')(aq_optimized_task.get_time_range_schedule_info_api)
    app.post('/query/task/get_task_on_node')(at_exp.get_task_on_node_api)

    app.post('/query/service_task/list')(aq_optimized_service_task.data_api)
    app.post('/query/service_task/list_all')(aq_optimized_service_task.all_tasks_api)

    app.post('/query/user/info')(aq_optimized_user.get_user_api)
    app.post('/query/user/list_all')(aq_optimized_user.get_all_user_api)
    app.post('/query/user/quota/list')(au_user.get_user_all_quota)
    app.post('/query/user/training_quota')(aq_optimized_user.get_user_node_quota_api)
    app.post('/query/user/training_quota/get_used')(aq_optimized_user.get_quota_used_api)
    app.post('/query/user/training_quota/list_all')(aq_optimized_user.get_all_user_node_quota_api)
    app.post('/query/user/training_quota/internal_list_all')(au_admin.get_internal_user_priority_quota)
    app.post('/query/user/training_quota/external_list_all')(au_ext.get_external_user_priority_quota)
    app.post('/query/user/access_token/list')(au_access.list_access_token)
    app.post('/query/user/tag/list')(at_exp.get_task_tags)
    app.post('/query/user/nodeport/list')(at_port.get_node_port_svc_list)

    app.post('/query/node/list')(ar_cluster.cluster_df)
    app.post('/query/node/host_info')(ar_cluster.get_host_info_api)
    app.post('/query/node/overview')(aq_optimized_resource.get_nodes_overview_api)
    app.post('/query/node/client_overview')(aq_optimized_resource.get_cluster_overview_for_client_api)


if 'monitor' in REG_SERVERS:
    app.post('/monitor/task/chain_perf_series')(at_exp.chain_perf_series_api)
    app.post('/monitor/user/storage/list')(ar_storage.get_user_storage_list)


logger.info(f'通过 server.py 初始化了 app: {app}')
