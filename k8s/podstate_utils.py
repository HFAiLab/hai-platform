
class PodStatus:
    CREATED = 'created'
    BUILDING = 'building'
    UNSCHEDULABLE = 'unschedulable'
    SCHEDULED = 'scheduled'
    RUNNING = 'running'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    STOPPED = 'stopped'
    UNKNOWN = 'unknown'

    ABNORMAL_LIST = [SCHEDULED, UNKNOWN]
    ALL = [CREATED, BUILDING, UNSCHEDULABLE, SCHEDULED, RUNNING, SUCCEEDED, FAILED, STOPPED, UNKNOWN]

class PodPhase:
    PENDING = 'Pending'
    RUNNING = 'Running'
    SUCCEEDED = 'Succeeded'
    FAILED = 'Failed'

class PodConditions:
    CONTAINERSREADY = 'ContainersReady'
    INITIALIZED = 'Initialized'
    SCHEDULED = 'PodScheduled'
    UNSCHEDULABLE = 'Unschedulable'

    VALUES = [CONTAINERSREADY, INITIALIZED, SCHEDULED]

class ContainerStatus:
    RUNNING = 'running'
    WAITING = 'waiting'
    TERMINATED = 'terminated'

class PodStateException(Exception):
    pass


def get_pod_state(pod_dict, container_names=None):
    try:
        pod_details = _get_pod_details(pod_dict=pod_dict)
        status, message = _get_pod_status(pod_details, container_names)
    except (KeyError, TypeError) as e:
        raise PodStateException(e)
    pod_state = {
        'status': status,
        'message': message,
        'details': pod_details
    }
    return pod_state


def _get_pod_details(pod_dict):
    pod_name = pod_dict['metadata'].get('name', "")
    labels = pod_dict['metadata'].get('labels', dict())
    node_name = pod_dict['spec'].get('nodeName', "")
    pod_phase = pod_dict['status'].get('phase', "")
    deletion_timestamp = pod_dict['metadata'].get('deletionTimestamp', None)
    conditions = pod_dict['status'].get('conditions', None)
    container_statuses = pod_dict['status'].get('containerStatuses', None)
    container_statuses_by_name = {
        s['name']: {
            'ready': s['ready'],
            'state': s['state'],
        } for s in container_statuses
    } if container_statuses else {}
    conditions_by_type = {c['type']: {
        'status': c.get('status', ""),
        'reason': c.get('reason', ''),
        'message': c.get('message', "")
    } for c in conditions
    } if conditions else {}

    return {
        'labels': labels,
        'phase': pod_phase,
        'deletion_timestamp': str(deletion_timestamp) if deletion_timestamp else None,
        'pod_conditions': conditions_by_type,
        'container_statuses': container_statuses_by_name,
        'node_name': node_name,
        'pod_name': pod_name
    }


def _get_container_status(container_statuses_dict, container_names):
    if container_names is None:
        return next(iter(container_statuses_dict.items()))[1]
    container_status = None
    for container_name in container_names:
        container_status = container_statuses_dict.get(container_name)
        if container_status:
            break
    return container_status


def _get_failed_reason(container_status):
    if not container_status:
        return None
    terminated = container_status['state'].get(ContainerStatus.TERMINATED, None)
    if not terminated:
        return None
    if terminated.get('reason', '') == 'Error':
        return f"exist-code({terminated.get('exitCode', '')})-message({terminated.get('message', '')})"
    return terminated.get('reason', '')


def _get_pod_status(pod_details, container_names=None):
    # check phase
    if pod_details['phase'] == PodPhase.FAILED:
        container_status = _get_container_status(
            container_statuses_dict=pod_details['container_statuses'],
            container_names=container_names)
        return PodStatus.FAILED, _get_failed_reason(container_status)
    if pod_details['phase'] == PodPhase.SUCCEEDED:
        return PodStatus.SUCCEEDED, None
    if not pod_details.get('pod_conditions', None):
        if pod_details['phase'] == PodPhase.PENDING:
            return PodStatus.SCHEDULED, 'Pod is pending'
        return PodStatus.UNKNOWN, 'Unknown pod conditions'
    if pod_details.get('deletion_timestamp', None):
        return PodStatus.STOPPED, f"Deletion time: {pod_details['deletion_timestamp']}"

    # check pod conditions
    if PodConditions.UNSCHEDULABLE in pod_details['pod_conditions']:
        return (PodStatus.UNSCHEDULABLE,
                pod_details['pod_conditions'][PodConditions.UNSCHEDULABLE].get('reason', ''))

    if PodConditions.SCHEDULED in pod_details['pod_conditions']:
        if pod_details['pod_conditions'][PodConditions.SCHEDULED].get('reason', '') == PodConditions.UNSCHEDULABLE:
            return (PodStatus.UNSCHEDULABLE,
                    pod_details['pod_conditions'][PodConditions.SCHEDULED].get('message', ''))
        if pod_details['pod_conditions'][PodConditions.SCHEDULED].get('status', '') != 'True':
            return (PodStatus.BUILDING,
                    pod_details['pod_conditions'][PodConditions.SCHEDULED].get('reason', ''))
        if PodConditions.CONTAINERSREADY not in pod_details['pod_conditions']:
            return PodStatus.BUILDING, None

    # check container status
    container_status = _get_container_status(
        container_statuses_dict=pod_details['container_statuses'],
        container_names=container_names)

    if not container_status:
        return PodStatus.UNKNOWN, None

    terminated = container_status['state'].get(ContainerStatus.TERMINATED, None)
    if terminated:
        if terminated.get('reason', '') == 'Completed':
            return PodStatus.SUCCEEDED, 'Completed'
        return PodStatus.FAILED, _get_failed_reason(container_status)
    waiting = container_status['state'].get(ContainerStatus.WAITING, None)
    if waiting:
        return PodStatus.BUILDING, waiting.get('reason', '')
    running = container_status['state'].get(ContainerStatus.RUNNING, None)
    if running:
        return PodStatus.RUNNING, running.get('reason', '')

    return PodStatus.UNKNOWN, None
