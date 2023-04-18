import asyncclick as click

from .command import create, list, remove, config


class HandleHfaiGroupArgs(click.Group):
    def format_usage(self, ctx, formatter):
        pieces = ['COMMAND', '<argument>...', '[OPTIONS]', '默认haienv路径为HOME，可以通过设置环境变量HAIENV_PATH来指定haienv路径（萤火平台下已设置该环境变量）']
        formatter.write_usage(ctx.command_path, " ".join(pieces))


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(cls=HandleHfaiGroupArgs, context_settings=CONTEXT_SETTINGS)
async def cli():
    pass

cli.add_command(create)
cli.add_command(list)
cli.add_command(remove)
cli.add_command(config)

if __name__ == '__main__':
    cli(_anyio_backend='asyncio')
