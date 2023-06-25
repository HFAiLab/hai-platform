import sys
import asyncclick as click

from .utils import HandleHfaiCommandArgs, get_experiment_auto
from hfai.client.api.artifact_api import async_get_artifact, async_create_update_artifact, \
      async_delete_artifact, async_get_all_task_artifact_mapping


@click.group()
def artifact():
    """
    用户自定义任务制品的管理接口

    artifact管理命令: set/get/list/remove

    任务映射命令: map/unmap/showmapped
    """
    pass


class ArtifactHandleHfaiCommandArgs(HandleHfaiCommandArgs):
    def format_options(self, ctx, formatter):
        pieces = self.collect_usage_pieces(ctx)
        with formatter.section("Arguments"):
            if 'experiment' in pieces:
                formatter.write_dl(rows=[('experiment', '任务信息，可以是任务名、是任务ID，也可以是提交的任务配置文件')])
            if 'artifact_name' in pieces:
                formatter.write_dl(rows=[('artifact_name', '制品命名，格式为name:version或name，不指定version时默认为name:default')])
        super(ArtifactHandleHfaiCommandArgs, self).format_options(ctx, formatter)


def parse_artifact_info(artifact_name):
    if not artifact_name:
        print('artifact_name 不能为空')
        sys.exit(1)
    artifact_info = artifact_name.split(':')
    if len(artifact_info) > 2 or (len(artifact_info) == 2 and artifact_info[1] == '*'):
        print('artifact_name格式为name:version, 且version不能为"*"')
        sys.exit(1)
    if len(artifact_info) == 1:
        print('未指定artifact version, 设置为默认"default"')
        version = 'default'
    else:
        version = artifact_info[1]
    return artifact_info[0], version


@artifact.command(cls=ArtifactHandleHfaiCommandArgs, name='list')
@click.option('-p', '--page', type=int, help='分页编号，0为不分页', default=1, show_default=True)
@click.option('-ps', '--page_size', type=int, help='分页时，分页大小', default=1000, show_default=True)
async def list_artifact(page, page_size):
    '''
    列举所有artifact
    '''
    await async_get_artifact(name='*', version='*', page=page, page_size=page_size)


@artifact.command(cls=ArtifactHandleHfaiCommandArgs, name='get')
@click.argument('artifact_name', required=True, metavar='artifact_name')
async def get_artifact(artifact_name):
    '''
    获取artifact信息
    '''
    await async_get_artifact(artifact_name, version='*')


@artifact.command(cls=ArtifactHandleHfaiCommandArgs, name='set')
@click.argument('artifact_name', required=True, metavar='artifact_name')
@click.option('-t', '--type', required=False, default='', help='制品类型，如dataset/model')
@click.option('-l', '--location', required=False, default='', help='制品对应数据实体所在位置，如共享目录、git仓库等')
@click.option('-d', '--description', required=False, default='', help='制品描述')
@click.option('-e', '--extra', required=False, default='', help='制品自定义信息')
@click.option('-p', '--private', required=False, is_flag=True, default=False, help='是否私有，组内不共享，默认为共享')
async def create_update_artifact(artifact_name, type, location, description, extra, private=False):
    '''
    创建或更新artifact
    '''
    name, version = parse_artifact_info(artifact_name)
    await async_create_update_artifact(name, version, type, location, description, extra, private)


@artifact.command(cls=ArtifactHandleHfaiCommandArgs, name='remove')
@click.argument('artifact_name', required=True, metavar='artifact_name')
@click.option('--force', required=False, is_flag=True, default=False, help='强制删除artifact，将导致关联的task映射关系被删除且无法复原')
async def delete_artifact(artifact_name, force):
    '''
    删除artifact
    '''
    name, version = parse_artifact_info(artifact_name)
    mapping_count = await async_get_all_task_artifact_mapping(name, version, days=365)
    if mapping_count > 0:
        if force:
            print('存在关联的task，映射关系将被强制删除')
        else:
            print('存在关联的task，请先删除映射关系')
            sys.exit(1)
    await async_delete_artifact(name, version)


@artifact.command(cls=ArtifactHandleHfaiCommandArgs, name='map')
@click.argument('experiment', required=True, metavar='experiment')
@click.argument('artifact_name', required=True, metavar='artifact_name')
@click.option('-d', '--direction', type=click.Choice(['input', 'output']), required=True, help='指定任务的input/output artifact')
async def map_artifact(experiment, artifact_name, direction):
    '''
    设置任务输入输出artifact信息，如artifact创建者不是当前用户，相应任务必须是共享任务
    '''
    try:
        experiment = await get_experiment_auto(experiment, 'auto')
        name, version = parse_artifact_info(artifact_name)
        msg = await experiment.map_task_artifact(name, version, direction)
        print(msg)
    except Exception as e:
        print(str(e))
        sys.exit(1)


@artifact.command(cls=ArtifactHandleHfaiCommandArgs, name='unmap')
@click.argument('experiment', required=True, metavar='experiment')
@click.option('-d', '--direction', type=click.Choice(['input', 'output', 'all']), required=True, help='指定任务的input/output/all artifact')
async def unmap_artifact(experiment, direction):
    '''
    移除任务输入输出artifact信息
    '''
    try:
        experiment = await get_experiment_auto(experiment, 'auto')
        msg = await experiment.unmap_task_artifact(direction)
        print(msg)
    except Exception as e:
        print(str(e))
        sys.exit(1)


@artifact.command(cls=ArtifactHandleHfaiCommandArgs, name='showmapped')
@click.argument('name', required=False, default='', metavar='name')
@click.option('-t', '--type', type=click.Choice(['experiment', 'artifact']), required=False, default='experiment', help='显示的节点类型，默认为experiment')
@click.option('-p', '--page', type=int, help='显示所有任务时，分页编号，0为不分页', default=1, show_default=True)
@click.option('-ps', '--page_size', type=int, help='显示所有任务且分页时，分页大小', default=1000, show_default=True)
@click.option('-d', '--days', type=int, help='显示所有任务时，需要查找的任务映射最早天数', default=90, show_default=True)
async def show_artifact_mapping(name, type, page, page_size, days):
    '''
    显示任务与artifact的映射信息 \n
    未指定name时，显示所有任务的映射关系 \n
    指定name时，如type选项为experiment，则name参数为任务信息，命令显示该任务的映射关系; 如type为artifact，则name参数为制品信息，命令显示映射到该制品的任务列表
    '''
    try:
        if name:
            if type == 'experiment':
                experiment = await get_experiment_auto(name, 'auto')
                result = await experiment.get_task_artifact()
                print(result)
            else:
                artifact_name, artifact_version = parse_artifact_info(name)
                await async_get_all_task_artifact_mapping(artifact_name, artifact_version, page, page_size, days)
            return
        await async_get_all_task_artifact_mapping('', '', page, page_size, days)
    except Exception as e:
        print(str(e))
        sys.exit(1)
