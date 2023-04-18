import pandas as pd

from base_model.base_user import BaseUser
from db import MarsDB
from utils import  asyncwrap
from k8s import get_corev1_api

k8s_corev1_api = get_corev1_api()

class UserCheckpoint:
    """
    管理用户开发容器中的持久化 checkpoint 镜像
    """
    def __init__(self, user: BaseUser):
        self.user = user
        self._checkpoint_df = None

    @property
    def _sql(self):
        # 暂时只支持以 user_name 保存，而非 group_name
        return f"""
        select "user_name", "description", "image_ref", "created_at", "updated_at" 
        from "user_image" 
        where "user_name" = '{self.user.user_name}'
        """

    async def create_checkpoint_df(self):
        result = await MarsDB().a_execute(self._sql)
        self._checkpoint_df = pd.DataFrame(result, columns=['usage_name', 'description', 'image_ref', 'created_at', 'updated_at'])

    @property
    def checkpoint_df(self):
        if self._checkpoint_df is None:
            self._checkpoint_df = pd.read_sql(self._sql, MarsDB().db)
        return self._checkpoint_df

    def checkpoint_list(self, description=None):
        return self.checkpoint_df[self.checkpoint_df.description == description].to_dict('records') if description else self.checkpoint_df.to_dict('records')

    def find_one(self, description):
        images = self.checkpoint_df[self.checkpoint_df.description == description].sort_values('updated_at', ascending=False).to_dict('records')
        if len(images):
            return images[0]
        return None

    async def async_get_pod(self, pod_name, namespace):
        async_func = asyncwrap(k8s_corev1_api.read_namespaced_pod_with_retry)
        return await async_func(pod_name, namespace, _request_timeout=5)

    async def async_set_pod_annotations(self, pod_name, namespace, annotations):
        async_func = asyncwrap(k8s_corev1_api.patch_namespaced_pod_with_retry)
        return await async_func(pod_name, namespace,
                                body={
                                    "metadata": {
                                        "annotations": annotations,
                                    }
                                },
                                _request_timeout=5)
