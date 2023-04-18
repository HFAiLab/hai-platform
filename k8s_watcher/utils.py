import os
from conf import CONF

from k8s import get_corev1_api, get_custom_corev1_api


v1 = get_corev1_api()
custom_v1 = get_custom_corev1_api()

module = os.environ.get('POD_NAME', 'k8swatcher-0')
namespace = CONF.launcher.task_namespace