import ujson
from dataclasses import dataclass
from typing import List, Dict, Callable, Tuple, Optional

from aliyunsdkcore import client
from aliyunsdksts.request.v20150401 import AssumeRoleRequest
import oss2

from loguru import logger
from .interface import *

@dataclass
class FileInfo:
    path: str
    size: Optional[int] = None
    last_modified: Optional[str] = None
    md5: Optional[str] = None
    ignored: Optional[bool] = None

class OSSApi(CloudObjectStorageInterface):

    def __init__(self,
                 endpoint: str,
                 access_key_id: str,
                 access_key_secret: str,
                 security_token: str = '',
                 uid: str = None,
                 role_arn: str = None,
                 breakpoint_info_path: str = None,
                 proxies: Dict[str, str] = None,
                 connect_timeout: int = 120) -> None:
        assert not (endpoint is None or access_key_id is None
                    or access_key_secret is None), 'missing cloud config'
        self.endpoint = endpoint
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.security_token = security_token
        self.proxies = proxies
        self.connect_timeout = connect_timeout
        self.breakpoint_info_path = breakpoint_info_path
        # uid, role_arn 用于云角色扮演
        self.uid = uid
        self.role_arn = f'acs:ram::{uid}:role/{role_arn}'
        self.bucket_pool = dict()

    def _get_bucket_handler(self, bucket_name: str, **kwargs):
        '''
        获取指定bucket handler
        '''
        if bucket_name not in self.bucket_pool:
            if self.security_token:
                auth = oss2.StsAuth(self.access_key_id, self.access_key_secret,
                                    self.security_token)
            else:
                auth = oss2.Auth(self.access_key_id, self.access_key_secret)
            self.bucket_pool[bucket_name] = oss2.Bucket(
                auth,
                self.endpoint,
                bucket_name,
                connect_timeout=self.connect_timeout,
                proxies=self.proxies)
        return self.bucket_pool[bucket_name]

    def list_bucket(self,
                    bucket_name: str,
                    prefix: str,
                    recursive: bool = True,
                    max_keys: int = 1000,
                    max_retries: int = 3,
                    **kwargs) -> Tuple[List[FileInfo], List[FileInfo]]:
        '''
        列举指定前缀目录下所有文件信息
        '''
        bucket = self._get_bucket_handler(bucket_name)
        delimiter = '' if recursive else '/'
        files, folders = list(), list()
        for obj in oss2.ObjectIteratorV2(bucket,
                                         prefix=prefix,
                                         delimiter=delimiter,
                                         max_keys=max_keys,
                                         max_retries=max_retries):
            if obj.is_prefix():
                folders.append(FileInfo(path=obj.key))
            else:
                files.append(
                    FileInfo(
                        path=obj.key,
                        size=int(obj.size),
                        last_modified=obj.last_modified,
                    ))
        return files, folders

    def resumable_download(self, bucket_name: str, key: str, filename: str,
                           multipart_threshold: int, part_size: int,
                           percentage: Callable, num_threads: int,
                           **kwargs) -> None:
        '''
        从对象存储中下载
        '''
        bucket = self._get_bucket_handler(bucket_name)
        store = oss2.ResumableDownloadStore(root=self.breakpoint_info_path)
        oss2.resumable_download(bucket, key, filename, multipart_threshold,
                                part_size, percentage, num_threads, store)

    def resumable_upload(self, bucket_name: str, key: str, filename: str,
                         multipart_threshold: int, part_size: int,
                         percentage: Callable, num_threads: int,
                         tagging: Dict[str, str], **kwargs) -> None:
        '''
        上传到对象存储
        '''
        bucket = self._get_bucket_handler(bucket_name)
        store = oss2.ResumableStore(root=self.breakpoint_info_path)
        headers = {oss2.headers.OSS_OBJECT_TAGGING: tagging}
        result = oss2.resumable_upload(bucket, key, filename, store, headers,
                                       multipart_threshold, part_size,
                                       percentage, num_threads)
        if result.status != 200:
            raise CloudApiException(
                f'upload {key} failed: {result.status}, {result.request_id}')

    def get_object_tagging(self, bucket_name: str, key: str,
                           **kwargs) -> Dict[str, str]:
        '''
        获取文件对象的标签
        '''
        ret = dict()
        try:
            bucket = self._get_bucket_handler(bucket_name)
            tagging_rst = bucket.get_object_tagging(key)
            ret = tagging_rst.tag_set.tagging_rule
        except (oss2.exceptions.NotFound, oss2.exceptions.NoSuchKey):
            pass
        except Exception as e:
            raise CloudApiException(f'get tagging failed {key}: {str(e)}')
        return ret

    def set_object_tagging(self, bucket_name: str, key: str,
                           tag: Dict[str, str], **kwargs) -> None:
        '''
        设置文件对象的标签
        '''
        tagging_rule = oss2.models.TaggingRule()
        tagging_rule.tagging_rule = tag
        try:
            bucket = self._get_bucket_handler(bucket_name)
            resp = bucket.put_object_tagging(key,
                                             oss2.models.Tagging(tagging_rule))
            if resp.status != 200:
                raise Exception((
                    f'set tagging failed {key}: {tag}, resp: {resp.request_id} {resp.status}'
                ))
            logger.debug(
                f'set tagging success {key}: {tag}, resp: {resp.request_id} {resp.status}'
            )
        except Exception as e:
            raise CloudApiException(str(e))

    def batch_delete_objects(self, bucket_name: str, files: List[str],
                             **kwargs) -> None:
        '''
        批量删除文件
        '''
        try:
            bucket = self._get_bucket_handler(bucket_name)
            logger.debug(f'batch delete bucket files started, {files}')
            resp = bucket.batch_delete_objects(files)
            if resp.status != 200:
                raise Exception(
                    f'batch delete bucket files failed, {files}, resp: {resp.request_id} {resp.status}'
                )
            logger.debug(f'batch delete bucket files finished, {files}')
        except Exception as e:
            raise CloudApiException(str(e))

    def get_access_token(self, bucket_name: str, prefix: str, ttl_seconds,
                         **kwargs) -> Dict[str, str]:
        '''
        获取临时访问token
        '''
        # 最小过期时间900s
        token_expire_seconds = 900
        if ttl_seconds > token_expire_seconds:
            token_expire_seconds = min(ttl_seconds,
                                       43200)  # 设置的role最大会话时间不超过12h

        clt = client.AcsClient(self.access_key_id,
                               self.access_key_secret,
                               'cn-hangzhou',
                               proxy=self.proxies,
                               connect_timeout=30,
                               timeout=30)
        req = AssumeRoleRequest.AssumeRoleRequest()
        req.set_accept_format('json')
        req.set_RoleArn(self.role_arn)
        req.set_RoleSessionName('multi-server')
        req.set_DurationSeconds(token_expire_seconds)

        # 签发的token只允许操作用户自己的目录
        policy_text = '''{
        "Statement" : [
            {
                "Action" : [
                    "oss:GetObject",
                    "oss:PutObject",
                    "oss:InitiateMultipartUpload",
                    "oss:CompleteMultipartUpload",
                    "oss:UploadPart",
                    "oss:ListMultipartUploads",
                    "oss:ListParts",
                    "oss:UploadPartCopy",
                    "oss:AbortMultipartUpload",
                    "oss:GetObjectTagging",
                    "oss:PutObjectTagging"
                ],
                "Effect" : "Allow",
                "Resource" : [
                    "acs:oss:*:%s:%s/%s/*"
                ]
            },
            {
                "Action" : [
                    "oss:ListObjectsV2",
                    "oss:ListObjects"
                ],
                "Effect" : "Allow",
                "Resource" : [
                    "acs:oss:*:%s:%s"
                ],
                "Condition":{
                    "StringLike":{
                        "oss:Prefix": [
                            "%s/*"
                        ]
                    }
                }
            }
        ],
        "Version" : "1"
        }''' % (self.uid, bucket_name, prefix, self.uid, bucket_name, prefix)
        req.set_Policy(policy_text.replace(' ', '').replace('\n', ''))
        body = clt.do_action_with_exception(req)
        data = ujson.loads(oss2.to_unicode(body))
        resp = {
            'request_id': data['RequestId'],
            'access_key_id': data['Credentials']['AccessKeyId'],
            'access_key_secret': data['Credentials']['AccessKeySecret'],
            'security_token': data['Credentials']['SecurityToken'],
            'expiration': data['Credentials']['Expiration'],
            'bucket': bucket_name,
            'endpoint': self.endpoint,
            'authorized_path': prefix
        }
        return resp
