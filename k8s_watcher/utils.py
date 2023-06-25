import os

from k8s import get_corev1_api, get_custom_corev1_api


corev1 = get_corev1_api()
all_corev1 = get_corev1_api('all')
all_custom_corev1 = get_custom_corev1_api('all')

module = os.environ.get('POD_NAME', 'k8swatcher-0')
