import asyncio

from datetime import datetime
from dateutil.parser import isoparse
from functools import wraps
from munch import Munch
from rich import box
from rich.align import Align
from rich.console import Group
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn
from rich.table import Table, Column
from rich.text import Text

from hfai.conf.flags import EXP_PRIORITY
from ...model.utils import get_current_user


BOX_TYPE = Munch(
    box_table = box.HORIZONTALS,
    box_panel = box.HEAVY
)
data_tasks = Munch()


def create_data_tasks(components):
    global data_tasks
    user = get_current_user()
    for k in components:
        data_tasks[k] = asyncio.get_event_loop().create_task(getattr(user, k).async_get())


def wrap_with_panel(*panel_args, title=None, title_align='left', box=None, **panel_kwargs):
    title = f' {title}' if title is not None else None
    def decorator(coro_func):
        @wraps(coro_func)
        async def wrapper(*args, **kwargs):
            component = await coro_func(*args, **kwargs)
            return Panel(component, *panel_args, title=title, title_align=title_align, box=box or BOX_TYPE.box_panel, **panel_kwargs)
        return wrapper
    return decorator


def skip_on_exception(coro_func):
    """ 某个 section 加载失败时暂时不显示, 避免整个 crash """
    @wraps(coro_func)
    async def wrapper(*args, **kwargs):
        try:
            return await coro_func(*args, **kwargs)
        except Exception as e:
            return Align.center(f'[bold red]Failed to Load\n{e}', vertical='middle')
    return wrapper


def add_rows(table: Table, rows, max_n_rows):
    """ 行数太多时最后加省略号 """
    if len(rows) > max_n_rows:
        for row in rows[:max_n_rows-1]: table.add_row(*row)
        table.add_row(*[Text('...', justify='center') for _ in range(len(table.columns))])
    else:
        for row in rows: table.add_row(*row)


PROFILE_HEIGHT=4
async def profile_section():
    user = get_current_user()
    table = Table(show_header=False, title=f'{user.user_name} | {user.nick_name}', box=BOX_TYPE.box_table)
    style = "[bold blue]"
    cells = [
        f"{style} ID", str(user.user_id),
        f"{style} Shared Group", user.shared_group,
        f"{style} Role", user.role,
    ]
    if not user.is_internal and 'community' in data_tasks:
        profile = (await data_tasks.community).get('result', {}).get('profile', {})
        cells += [
            f"{style} Name", profile.get('chinese_name') or 'Unknown',
            f"{style} Org", str(profile.get('school') or profile.get('company') or 'Unknown')
        ]
    table.add_row(*cells)
    return Align.center(table)


@wrap_with_panel(title="节点 Quota 用量")
@skip_on_exception
async def node_quota_section(height):
    quota = (await data_tasks.quota).get('result', {})
    node_quota, used_quota = quota.get('node_quota', {}), quota.get('used_node_quota', {})

    quota_data = [(*k.split('-'), used_quota[k], node_quota[f'node-{k}']) for k in used_quota if f'node-{k}' in node_quota]
    n_digits = max(len(str(max(used, quota))) for _, _, used, quota in quota_data)
    node_sum_used = {node: sum(used for qnode, _, used, _ in quota_data if qnode==node)
                     for node in set([x[0] for x in quota_data])}

    progress = Progress(
        TextColumn("{task.fields[node]} [bold]{task.fields[priority]}"),
        BarColumn(style='grey50', complete_style='green', finished_style='red'),
        TextColumn("[dark_cyan]{task.completed:%dd}/{task.total:%dd}" % (n_digits, n_digits)),
    )
    # 节点quota: 组间按用量排序, 组内按优先级排序
    quota_data.sort(key=lambda x: (node_sum_used[x[0]], EXP_PRIORITY.__dict__.get(x[1], -1)), reverse=True)
    for node, priority, used, quota in quota_data:
        progress.add_task("", node=node, priority=priority, total=quota, completed=used)
    max_n_rows = height-2
    if len(progress.tasks) > max_n_rows:
        for task in progress.tasks[max_n_rows-1:]:
            progress.remove_task(task.id)
        progress = Group(progress, Align.center("..."))
    elif len(progress.tasks) == 0:
        progress = '[bold grey50](No Data)'
    return Align.center(progress, vertical='middle')


@wrap_with_panel(title="Jupyter Quota")
@skip_on_exception
async def jupyter_quota_section(height):
    table = Table("Group", Column("CPU", style='dark_cyan'), Column("Mem", style='dark_cyan'),
                  Column("Quota", style='dark_cyan'), box=BOX_TYPE.box_table)
    quota = (await data_tasks.quota).get('result', {})
    rows = [(k, str(v['cpu']), str(v['memory']), str(v['quota'])) for k, v in quota.get('jupyter_quota', {}).items()]
    add_rows(table, rows, max_n_rows=height-5)

    if not get_current_user().is_internal:
        ext_table = Table(Column("Spot 开发容器", ratio=1, justify='center'), Column(ratio=1),
                        Column("独占开发容器", ratio=1, justify='center'), box=None)
        ext_table.add_row(str(quota.get("spot_jupyter", 0)), "", str(quota.get("dedicated_jupyter", 0)))
        ext_table = Align.center(ext_table)
        table = Group(ext_table, table)

    return Align.center(table, vertical='middle')


@wrap_with_panel(title="Access Tokens")
@skip_on_exception
async def access_section(height):
    user = get_current_user()
    if user.access_scope != 'all':
        return Align.center('[bold orange_red1]当前 token 权限不足, 无法查看', vertical='middle')
    access = await user.access.async_get()
    rows = []
    for token in access.get('result', {}).get('access_tokens', []):
        if token['from_user_name'] != user.user_name and token['access_user_name'] != user.user_name:
            continue    # 管理员可以看到跟自己无关的 tokens
        expired = isoparse(token['expire_at']) <= datetime.now()
        state = '[bold grey50]inactive' if not token.get('active', False) else \
            ('[bold grey50]expired' if expired else '[green4 bold]valid')
        rows.append((token['from_user_name'], token['access_user_name'], token['access_scope'], state))
    table = Table(Column("From User"), Column("Access User"), Column("Scope"), Column("State"), box=BOX_TYPE.box_table)
    add_rows(table, rows, max_n_rows=height-5)
    return Align.center(table, vertical='middle')
