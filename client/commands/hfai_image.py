import os.path

import asyncclick as click
from rich.console import Console
from rich.box import ASCII2
from rich.table import Table

from hfai.client.api.image_api import fetch_images, load_image_tar, delete_image_by_name
from .utils import HandleHfaiGroupArgs, HandleHfaiCommandArgs


@click.group(cls=HandleHfaiGroupArgs)
def images():
    """
    用户自定义镜像的管理接口
    """
    pass


class WorkspaceHandleHfaiCommandArgs(HandleHfaiCommandArgs):
    def format_options(self, ctx, formatter):
        pieces = self.collect_usage_pieces(ctx)
        with formatter.section("Arguments"):
            if 'image_tar' in pieces:
                formatter.write_dl(rows=[('image_tar', '用户要加载进萤火的镜像 TAR 包。'
                                                       '在用户本地调用为 workspace 中的路径，'
                                                       '在萤火上调用则为其共享存储中的路径')])
            if 'image' in pieces:
                formatter.write_dl(rows=[('image', '完整的镜像名，[registry]/image:<tag>')])
        super(WorkspaceHandleHfaiCommandArgs, self).format_options(ctx, formatter)


@images.command(cls=WorkspaceHandleHfaiCommandArgs, name='list')
@click.option('-a', '--all', 'show_all', required=False, is_flag=True, default=False, show_default=True, help='是否显示所有镜像(含删除的)')
async def list_images(show_all=False):
    """
    列举用户组在萤火二号上的镜像列表，以及镜像在萤火二号上的状态
    """
    mars_images, user_imgs = await fetch_images()

    last_img_status = {}

    console = Console()

    mars_table = Table(title='萤火二号内建镜像', title_justify=True, box=ASCII2, style='dim', show_header=True)
    for column in ['image', 'default_python', 'cuda', 'supported_hf_envs', 'environments']:
        mars_table.add_column(column)
    default_flag = True
    for i in sorted(mars_images, key=lambda x: x['quota'], reverse=True):
        image = f'{i["env_name"]}{"(default)" if default_flag else ""}'
        default_flag = False
        default_python = i.get('config', {}).get('python', 'unspecified')
        cuda_version = i.get('config', {}).get('cuda', 'unknown')
        supported_hf_envs = ';'.join(i.get('config', {}).get('hf_envs', []))
        environments = ';'.join([f'{k}={v}' for k, v in i.get('config', {}).get('environments', {}).items()])
        mars_table.add_row(image, default_python, cuda_version, supported_hf_envs, environments)
    console.print(mars_table)

    user_table = Table(title='用户自定义镜像', title_justify=True, box=ASCII2, style='dim', show_header=True)
    for column in ['image', 'status', 'shared_group', 'image_tar', 'updated_at']:
        user_table.add_column(column)
    for i in user_imgs:
        i_name = os.path.join(i['registry'], i['shared_group'], i['image'])
        if i_name not in last_img_status:
            last_img_status[i_name] = i['status']
        # 以最新的为准
        if i_name in last_img_status and i['status'] != last_img_status[i_name]:
            i['status'] = f"{last_img_status[i_name]} by new tar({i['status']})"
        if 'deleted' in i['status'] and not show_all:
            continue
        user_table.add_row(i_name,
                      i['status'],
                      i['shared_group'], os.path.basename(i['image_tar']),
                      i['updated_at'],
                      end_section=True)
    console.print(user_table)


@images.command(cls=WorkspaceHandleHfaiCommandArgs, name='load')
@click.argument('image_tar', required=True, metavar='image_tar')
async def load_image(image_tar):
    """
    加载镜像 tar 包到萤火二号上，tar包应该在萤火二号上共享目录下的，外部用户需要先把 tar 包上传上来操作
    """
    if os.path.exists(image_tar):
        abs_image_tar = os.path.abspath(image_tar)
        await load_image_tar(abs_image_tar)
    else:
        print('不存在这个镜像包')


@images.command(cls=WorkspaceHandleHfaiCommandArgs, name='delete')
@click.argument('image', required=True, metavar='image')
async def delete_image(image):
    """
    删除萤火二号上的镜像，以释放空间
    注意: 1、 该镜像的命名并不会被回收 2、 用户也可以删除自己组内的其他用户的镜像
    """
    await delete_image_by_name(image_name=image)
