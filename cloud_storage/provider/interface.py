from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Any


class CloudApiException(Exception):
    pass


class CloudObjectStorageInterface(ABC):

    @abstractmethod
    def _get_bucket_handler(self, bucket_name: str, **kwargs):
        '''
        获取指定bucket handler
        '''
        raise NotImplementedError

    @abstractmethod
    def list_bucket(self,
                    bucket_name: str,
                    prefix: str,
                    recursive: bool = True,
                    **kwargs) -> Tuple[List[Any], List[Any]]:
        '''
        列举指定前缀目录下所有文件信息
        '''
        raise NotImplementedError

    @abstractmethod
    def resumable_download(self, bucket_name: str, key: str, filename: str,
                           **kwargs) -> None:
        '''
        从对象存储中下载
        '''
        raise NotImplementedError

    @abstractmethod
    def resumable_upload(self, bucket_name: str, key: str, filename: str,
                         **kwargs) -> None:
        '''
        上传到对象存储
        '''
        raise NotImplementedError

    @abstractmethod
    def get_object_tagging(self, bucket_name: str, key: str,
                           **kwargs) -> Dict[str, str]:
        '''
        获取文件对象的标签
        '''
        raise NotImplementedError

    @abstractmethod
    def set_object_tagging(self, bucket_name: str, key: str,
                           tag: Dict[str, str], **kwargs) -> None:
        '''
        设置文件对象的标签
        '''
        raise NotImplementedError

    @abstractmethod
    def batch_delete_objects(self, bucket_name: str, files: List[str],
                             **kwargs) -> None:
        '''
        批量删除文件
        '''
        raise NotImplementedError

    @abstractmethod
    def get_access_token(self, bucket_name: str, **kwargs) -> Dict[str, str]:
        '''
        获取临时访问token
        '''
        raise NotImplementedError
