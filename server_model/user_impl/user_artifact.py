from typing import Dict, Any

from base_model.base_user import BaseUser
from base_model.base_user_modules import IUserArtifact
from db import MarsDB
from logm import logger


class UserArtifact(IUserArtifact):
    '''
    用户自定义制品，用于归类和连接不同task, 以形成逻辑上的taskgroup和group之间串联关系
    '''

    def __init__(self, user: BaseUser):
        super().__init__(user)
        self.user = user
        self.table_name = 'user_artifact'
        self.mapping_table_name = 'task_artifact_mapping'
        self.column_str = '"user_name", "name", "version", "type", "location", "description", "extra", "shared_group"'
        self.primary_column_str = '"user_name", "name", "version"'
        self.all = '*'

    def _where(self, query: Dict = dict()):
        return ' AND '.join([f'''{k} in ('{"', '".join(v)}')''' if isinstance(v, list) else f"{k} = '{v}'" for k, v in query.items()])

    async def async_get(self, name, version, page, page_size) -> Dict[str, Any]:
        query = dict()
        if name != self.all:
            query['name'] = name
        if version != self.all:
            query['version'] = version
        query_own = {'user_name': self.user.user_name}
        query_group = {'shared_group': self.user.shared_group}
        where = f'({self._where(query_own)} OR {self._where(query_group)})'
        if query:
            where += f' AND {self._where(query)}'
        _sql =  f"""
        SELECT {self.column_str}
        FROM "{self.table_name}"
        WHERE {where}
        ORDER BY "user_name", "name", "shared_group" DESC
        {f'LIMIT {page_size} offset {page_size * (page - 1)}' if page else ''}
        """
        results = await MarsDB().a_execute(_sql)
        return results.fetchall()

    async def async_create_update_artifact(self,
                                           name,
                                           version,
                                           type,
                                           location,
                                           description,
                                           extra='',
                                           private=False,
                                           **kwargs):
        try:
            shared_group = self.user.user_name if private else self.user.shared_group
            existed_artifacts = (await MarsDB().a_execute(f"""
            SELECT {self.column_str}
            FROM "{self.table_name}"
            WHERE "name" = '{name}'
              AND "version" = '{version}'
            """)).fetchall()
            if len(existed_artifacts) > 0:
                for artifact in existed_artifacts:
                    if artifact.user_name != self.user.user_name:
                        if artifact.shared_group == self.user.shared_group:
                            # already exists in other's shared_group
                            raise Exception(f'artifact {name}:{version} 已经在组"{self.user.shared_group}"中，您只能编辑自己的artifact')
                        peer_users = [row.user_name for _, row in self.user.other_shared_group_users.iterrows()]
                        if artifact.shared_group != self.user.shared_group and not private and artifact.user_name in peer_users:
                            # already exists privately
                            raise Exception(f'artifact {name}:{version} 已经被组"{self.user.shared_group}"其他成员创建且为私有')
                    if artifact.user_name == self.user.user_name and artifact.shared_group != shared_group:
                        # already exists in your group
                        raise Exception(f'''artifact {name}:{version} 已经存在, 如需编辑，请 {"删除" if private else "增加"} --private 选项''')

            _sql = f'''
                INSERT INTO "{self.table_name}" ({self.column_str})
                VALUES ({', '.join(['%s']*8)})
                ON CONFLICT ({self.primary_column_str})
                    DO UPDATE SET "type" = excluded."type",
                                  "location" = excluded."location",
                                  "description" = excluded."description",
                                  "extra" = excluded."extra"
            '''
            _params = (self.user.user_name, name, version, type, location, description, extra, shared_group)
            # private时使用用户名作为shared_group标识
            await MarsDB().a_execute(_sql, params=_params, remote_apply=kwargs.get('remote_apply', False))
            logger.debug(f'create or update artifact {name}:{version} successful')
            return
        except Exception as e:
            logger.error(
                f'create or update  artifact {name}:{version} error: {str(e)}')
            raise e

    async def async_delete_artifact(self, name, version, **kwargs):
        try:
            query = {'name': name, 'user_name': self.user.user_name}
            if version != self.all:
                query['version'] = version
            _sql = f'''
                DELETE FROM "{self.table_name}"
                WHERE {self._where(query)}
                RETURNING *
            '''
            count = len((await MarsDB().a_execute(_sql,
                                     remote_apply=kwargs.get(
                                         'remote_apply', False))).fetchall())
            logger.debug(f'delete artifact {name}:{version}, count: {count}')
            return count
        except Exception as e:
            logger.error(f'delete artifact {name}:{version} error: {str(e)}')
            raise e
