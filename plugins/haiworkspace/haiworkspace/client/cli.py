import asyncclick as click
from .command import init, push, pull, download, diff, list, remove


class HandleHaiGroupArgs(click.Group):
    def format_usage(self, ctx, formatter):
        pieces = ['COMMAND', '<argument>...', '[OPTIONS]']
        formatter.write_usage(ctx.command_path, " ".join(pieces))


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(cls=HandleHaiGroupArgs, context_settings=CONTEXT_SETTINGS)
async def cli():
    pass

cli.add_command(init)
cli.add_command(push)
cli.add_command(pull)
cli.add_command(download)
cli.add_command(diff)
cli.add_command(list)
cli.add_command(remove)

if __name__ == '__main__':
    cli(_anyio_backend='asyncio')
