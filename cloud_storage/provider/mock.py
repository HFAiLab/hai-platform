
from .interface import *
from loguru import logger


class MockApi(CloudObjectStorageInterface):
    def __init__(self, **kwargs) -> None:
        pass

    def _get_bucket_handler(self, bucket_name, **kwargs):
        return None

    def list_bucket(self,
                    bucket_name: str,
                    prefix: str,
                    recursive: bool = True,
                    **kwargs) -> Tuple[List[Any], List[Any]]:
        '''
        列举指定前缀目录下所有文件信息
        '''
        logger.info(
            f'mock list_bucket: bucket_name {bucket_name}, prefix {prefix}')
        return (list(), list())

    def resumable_download(self, bucket_name: str, key: str, filename: str,
                           **kwargs) -> None:
        '''
        从对象存储中下载
        '''
        logger.info(
            f'mock resumable_download: bucket_name {bucket_name}, key {key}')
        return

    def resumable_upload(self, bucket_name: str, key: str, filename: str,
                         **kwargs) -> None:
        '''
        上传到对象存储
        '''
        logger.info(
            f'mock resumable_upload: bucket_name {bucket_name}, key {key}')
        return

    def get_object_tagging(self, bucket_name: str, key: str,
                           **kwargs) -> Dict[str, str]:
        '''
        获取文件对象的标签
        '''
        logger.info(
            f'mock get_object_tagging: bucket_name {bucket_name}, key {key}')
        return dict()

    def set_object_tagging(self, bucket_name: str, key: str,
                           tag: Dict[str, str], **kwargs) -> None:
        '''
        设置文件对象的标签
        '''
        logger.info(
            f'mock set_object_tagging: bucket_name {bucket_name}, key {key}')
        return

    def batch_delete_objects(self, bucket_name: str, files: List[str],
                             **kwargs) -> None:
        '''
        批量删除文件
        '''
        logger.info(
            f'mock batch_delete_objects: bucket_name {bucket_name}, files {files}'
        )
        return

    def get_access_token(self, bucket_name: str, **kwargs) -> Dict[str, str]:
        '''
        获取临时访问token
        '''
        logger.info(f'mock get_access_token: bucket_name {bucket_name}')
        return dict()
