import re
import traceback
import warnings

import asyncclick as click
from docutils import nodes
from docutils.parsers import rst
from docutils.parsers.rst import directives
from docutils import statemachine
from sphinx.util import logging
from sphinx.util import nodes as sphinx_nodes

LOG = logging.getLogger(__name__)

NESTED_FULL = 'full'
NESTED_SHORT = 'short'
NESTED_NONE = 'none'

ANSI_ESC_SEQ_RE = re.compile(r'\x1B\[\d+(;\d+){0,2}m', flags=re.MULTILINE)


def _indent(text, level=1):
    prefix = ' ' * (4 * level)

    def prefixed_lines():
        for line in text.splitlines(True):
            yield (prefix + line if line.strip() else line)

    return ''.join(prefixed_lines())


def _get_usage(ctx):
    """Alternative, non-prefixed version of 'get_usage'."""
    formatter = ctx.make_formatter()
    # pieces = ctx.command.collect_usage_pieces(ctx)
    # formatter.write_usage(ctx.command_path, ' '.join(pieces), prefix='')
    # return formatter.getvalue().rstrip('\n')

    ctx.command.format_usage(ctx, formatter)
    return formatter.getvalue().rstrip('\n')[6:]


def _get_help_record(opt):
    """Re-implementation of click.Opt.get_help_record.

    The variant of 'get_help_record' found in Click makes uses of slashes to
    separate multiple opts, and formats option arguments using upper case. This
    is not compatible with Sphinx's 'option' directive, which expects
    comma-separated opts and option arguments surrounded by angle brackets [1].

    [1] http://www.sphinx-doc.org/en/stable/domains.html#directive-option
    """

    def _write_opts(opts):
        rv, _ = click.formatting.join_options(opts)
        if not opt.is_flag and not opt.count:
            name = opt.name
            if opt.metavar:
                name = opt.metavar.lstrip('<[{($').rstrip('>]})$')
            rv += ' <{}>'.format(name)
        return rv

    rv = [_write_opts(opt.opts)]
    if opt.secondary_opts:
        rv.append(_write_opts(opt.secondary_opts))

    out = []
    if opt.help:
        if opt.required:
            out.append('**required**。%s' % opt.help)
        else:
            out.append(opt.help)
    else:
        if opt.required:
            out.append('**Required**')

    extras = []

    if isinstance(opt.show_default, str):
        # Starting from Click 7.0 show_default can be a string. This is
        # mostly useful when the default is not a constant and
        # documentation thus needs a manually written string.
        extras.append('默认值: %s' % opt.show_default)
    elif opt.default is not None and opt.show_default:
        extras.append(
            '默认值: %s'
            % (
                ', '.join(str(d) for d in opt.default)
                if isinstance(opt.default, (list, tuple))
                else opt.default,
            )
        )

    if isinstance(opt.type, click.Choice):
        extras.append(':options: %s' % ' | '.join(str(x) for x in opt.type.choices))

    if extras:
        if out:
            out.append('')

        out.extend(extras)

    return ', '.join(rv), '\n'.join(out)


def _format_help(help_string):
    help_string = ANSI_ESC_SEQ_RE.sub('', help_string)

    bar_enabled = False
    for line in statemachine.string2lines(
        help_string, tab_width=4, convert_whitespace=True
    ):
        if line == '\b':
            bar_enabled = True
            continue
        if line == '':
            bar_enabled = False
        line = '| ' + line if bar_enabled else line
        yield line
    yield ''


def _format_description(ctx):
    """Format the description for a given `click.Command`.

    We parse this as reStructuredText, allowing users to embed rich
    information in their help messages if they so choose.
    """
    help_string = ctx.command.help or ctx.command.short_help
    if help_string:
        yield from _format_help(help_string)


def _format_usage(ctx):
    """Format the usage for a `click.Command`."""
    yield '.. code-block:: shell'
    yield ''
    for line in _get_usage(ctx).splitlines():
        yield _indent(line)
    yield ''


def _format_option(opt):
    """Format the output for a `click.Option`."""
    opt = _get_help_record(opt)

    yield '- ``{}``'.format(opt[0])
    if opt[1]:
        yield ''
        for line in statemachine.string2lines(
            ANSI_ESC_SEQ_RE.sub('', opt[1]), tab_width=4, convert_whitespace=True
        ):
            yield _indent(line)


def _format_options(ctx, option_type=""):
    """Format all `click.Option` for a `click.Command`."""
    # the hidden attribute is part of click 7.x only hence use of getattr

    # params = [
    #     param
    #     for param in ctx.command.params
    #     if isinstance(param, click.Option) and not getattr(param, 'hidden', False)
    # ]

    params = []
    cluster_opts = []
    simulate_opts = []
    workspace_opts = []
    for param in ctx.command.params:
        if isinstance(param, click.Option) and not getattr(param, 'hidden', False):
            if option_type == "":
                params.append(param)
            else:
                rv = param.get_help_record(ctx)
                if any([n in param.name for n in ['suspend_seconds', 'life_state']]):
                    simulate_opts.append(param)
                elif rv[-1].startswith('push时'):
                    workspace_opts.append(param)
                else:
                    if any([n in param.name for n in ['group', 'priority']]):
                        continue
                    cluster_opts.append(param)

    if option_type == "workspace":
        params = workspace_opts
    elif option_type == "simulate":
        params = simulate_opts
    elif option_type == "cluster" :
        params = cluster_opts

    for param in params:
        for line in _format_option(param):
            yield line
        yield ''


def _format_argument(arg):
    """Format the output of a `click.Argument`."""

    arg_maps = {
        'token': '用于认证 hai 服务的密钥，由管理员分发',
        'remote_path': '远端文件路径，远端目录请通过diff获取，如 checkpoint/model.pt，默认: checkpoint',
        'workspace_name': '工作区的名字',
        'venv_name': 'venv的名字',
        'experiment': '用于检索的任务，可以是任务名、是任务ID，也可以是提交的任务配置文件',
        'experiment.yml': '创建任务的任务配置文件',
        'image': '完整的镜像名，[registry]/image:<tag>',
        'image_tar': '用户要加载进萤火的镜像 TAR 包。在用户本地调用为 workspace 中的路径，在萤火上调用则为其共享存储中的路径'
    }

    arg_name = arg.human_readable_name

    if arg_name in arg_maps.keys():

        yield '- ``{}``'.format(arg_name)
        yield ''
        yield _indent(arg_maps[arg_name])

    else:
        yield ''


def _format_arguments(ctx):
    """Format all `click.Argument` for a `click.Command`."""
    params = [x for x in ctx.command.params if isinstance(x, click.Argument)]

    for param in params:
        for line in _format_argument(param):
            yield line
        yield ''


def _format_envvar(param):
    """Format the envvars of a `click.Option` or `click.Argument`."""
    yield '.. envvar:: {}'.format(param.envvar)
    yield '   :noindex:'
    yield ''
    if isinstance(param, click.Argument):
        param_ref = param.human_readable_name
    else:
        # if a user has defined an opt with multiple "aliases", always use the
        # first. For example, if '--foo' or '-f' are possible, use '--foo'.
        param_ref = param.opts[0]

    yield _indent('Provide a default for :option:`{}`'.format(param_ref))


def _format_envvars(ctx):
    """Format all envvars for a `click.Command`."""
    params = [x for x in ctx.command.params if getattr(x, 'envvar')]

    for param in params:
        yield '.. _{command_name}-{param_name}-{envvar}:'.format(
            command_name=ctx.command_path.replace(' ', '-'),
            param_name=param.name,
            envvar=param.envvar,
        )
        yield ''
        for line in _format_envvar(param):
            yield line
        yield ''


def _format_subcommand(command):
    """Format a sub-command of a `click.Command` or `click.Group`."""
    yield '.. object:: {}'.format(command.name)

    short_help = command.get_short_help_str()

    if short_help:
        yield ''
        for line in statemachine.string2lines(
            short_help, tab_width=4, convert_whitespace=True
        ):
            yield _indent(line)


def _format_epilog(ctx):
    """Format the epilog for a given `click.Command`.

    We parse this as reStructuredText, allowing users to embed rich
    information in their help messages if they so choose.
    """
    if ctx.command.epilog:
        yield from _format_help(ctx.command.epilog)


def _get_lazyload_commands(multicommand):
    commands = {}
    for command in multicommand.list_commands(multicommand):
        commands[command] = multicommand.get_command(multicommand, command)

    return commands


def _filter_commands(ctx, commands=None):
    """Return list of used commands."""
    lookup = getattr(ctx.command, 'commands', {})
    if not lookup and isinstance(ctx.command, click.MultiCommand):
        lookup = _get_lazyload_commands(ctx.command)

    if commands is None:
        return sorted(lookup.values(), key=lambda item: item.name)

    names = [name.strip() for name in commands.split(',')]
    return [lookup[name] for name in names if name in lookup]


def _format_command(ctx, nested, commands=None):
    """Format the output of `click.Command`."""
    if ctx.command.hidden:
        return

    is_hai_python = ctx.command.name == "python"

    if ctx.info_name == 'hai python':
        cmd = 'python'
        file_ext = 'py'
    elif ctx.info_name == 'hai bash':
        cmd = 'bash'
        file_ext = 'sh'
    else:
        cmd = 'exec'
        file_ext = 'exe'

    # description

    for line in _format_description(ctx):
        yield line

    yield '.. program:: {}'.format(ctx.command_path)

    # usage

    yield '.. rubric:: Usage:'
    yield ''

    if is_hai_python:
        yield '.. code-block:: shell'
        yield ''
        yield _indent(f'hai {cmd} <experiment.{file_ext}> [<experiment_params>...]                        # 在本地运行，等同 {cmd}')
        yield _indent(f'hai {cmd} <experiment.{file_ext}> [<experiment_params>...] -- [CLUSTER_OPTIONS]   # 提交任务到萤火二号运行')
        yield _indent(f'hai {cmd} <experiment.{file_ext}> [<experiment_params>...] ++ [SIMULATE_OPTIONS]  # 在本地模拟运行，会提供萤火二号一致的环境变量和表现')
        yield ''

    else:
        for line in _format_usage(ctx):
            yield line

    # options

    if is_hai_python:

        lines = list(_format_options(ctx, option_type="cluster"))
        if lines:
            yield '.. rubric:: Cluster Options'
            yield ''
        for line in lines:
            yield line

        lines = list(_format_options(ctx, option_type="simulate"))
        if lines:
            yield '.. rubric:: Simulate Options'
            yield ''
        for line in lines:
            yield line

        yield '.. rubric:: Environment Variables'
        yield ''
        yield '- ``HF_ENV_NAME=<var>``  用于显示指定在萤火上以什么环境运行, 可选萤火内建的：py38-202111, 或用户自建的虚拟环境'
        yield '- ``HF_ENV_OWNER=<var>``  若使用他人构建的虚拟环境，需要指定该环境的所属者，可以通过 haienv list 查看'
        yield ''

        lines = list(_format_options(ctx, option_type="workspace"))
        if lines:
            yield '.. rubric:: Workspace Options'
            yield ''
        for line in lines:
            yield line

    else:

        lines = list(_format_options(ctx))
        if lines:
            # we use rubric to provide some separation without exploding the table
            # of contents
            yield '.. rubric:: Options:'
            yield ''

        for line in lines:
            yield line

    # arguments

    if is_hai_python:

        yield '.. rubric:: Arguments'
        yield ''
        yield '- ``experiment.{}``'.format(file_ext)
        yield ''
        yield _indent('远程运行的脚本')
        yield ''
        yield '- ``experiment_params...``'
        yield ''
        yield _indent('远程运行的脚本脚本需要的一系列参数，可输入多项，与本地运行的时候一致')
        yield ''

    else:
        lines = list(_format_arguments(ctx))
        if lines:
            yield '.. rubric:: Arguments'
            yield ''

        for line in lines:
            yield line

    # hai python examples

    if is_hai_python:
        yield '.. rubric:: Examples'
        yield ''
        yield '.. code-block:: shell'
        yield ''
        yield _indent(f'hai {cmd} bert.{file_ext} -c large.yml -- -n 10  # 使用萤火十个节点')
        yield _indent(f'HF_ENV_NAME=py38-202111 hai {cmd} bert.{file_ext} -c large.yml -- -n 1  # 使用萤火内建的 py38-202111 环境运行')
        yield _indent(f'hai {cmd} bert.{file_ext} -c large.yml  # 本地运行')
        yield _indent(f'hai {cmd} bert.{file_ext} -c large.yml -- --nodes 1  # 使用萤火一节点')
        yield _indent(f'hai {cmd} bert.{file_ext} -c large.yml -- -n 1 -i default -e A=B -e C=D  # 使用 default 镜像，配置环境变量')
        yield _indent(f'hai {cmd} bert.{file_ext} -c large.yml ++ --suspend_seconds 100  # 本地模拟100秒后打断')
        yield ''

    # environment variables

    lines = list(_format_envvars(ctx))
    if lines:
        yield '.. rubric:: Environment variables'
        yield ''

    for line in lines:
        yield line

    # description

    for line in _format_epilog(ctx):
        yield line

    # if we're nesting commands, we need to do this slightly differently
    if nested in (NESTED_FULL, NESTED_NONE):
        return

    commands = _filter_commands(ctx, commands)

    if commands:
        yield '.. rubric:: Commands'
        yield ''

    for command in commands:
        # Don't show hidden subcommands
        if command.hidden:
            continue

        for line in _format_subcommand(command):
            yield line
        yield ''


def nested(argument):
    values = (NESTED_FULL, NESTED_SHORT, NESTED_NONE)
    if not argument:
        return None

    if argument not in values:
        raise ValueError(
            "%s is not a valid value for ':nested:'; allowed values: %s"
            % directives.format_values(values)
        )

    return argument


class ClickDirective(rst.Directive):

    has_content = False
    required_arguments = 1
    option_spec = {
        'prog': directives.unchanged_required,
        'nested': nested,
        'commands': directives.unchanged,
        'show-nested': directives.flag,
    }

    def _load_module(self, module_path):
        """Load the module."""
        # __import__ will fail on unicode,
        # so we ensure module path is a string here.
        module_path = str(module_path)

        try:
            module_name, attr_name = module_path.split(':', 1)
        except ValueError:  # noqa
            raise self.error(
                '"{}" is not of format "module:parser"'.format(module_path)
            )

        try:
            mod = __import__(module_name, globals(), locals(), [attr_name])
        except (Exception, SystemExit) as exc:  # noqa
            err_msg = 'Failed to import "{}" from "{}". '.format(attr_name, module_name)
            if isinstance(exc, SystemExit):
                err_msg += 'The module appeared to call sys.exit()'
            else:
                err_msg += 'The following exception was raised:\n{}'.format(
                    traceback.format_exc()
                )

            raise self.error(err_msg)

        if not hasattr(mod, attr_name):
            raise self.error(
                'Module "{}" has no attribute "{}"'.format(module_name, attr_name)
            )

        parser = getattr(mod, attr_name)

        if not isinstance(parser, click.BaseCommand):
            raise self.error(
                '"{}" of type "{}" is not derived from '
                '"click.BaseCommand"'.format(type(parser), module_path)
            )
        return parser

    def _generate_nodes(
        self, name, command, parent, nested, commands=None, semantic_group=False
    ):
        """Generate the relevant Sphinx nodes.

        Format a `click.Group` or `click.Command`.

        :param name: Name of command, as used on the command line
        :param command: Instance of `click.Group` or `click.Command`
        :param parent: Instance of `click.Context`, or None
        :param nested: The granularity of subcommand details.
        :param commands: Display only listed commands or skip the section if
            empty
        :param semantic_group: Display command as title and description for
            CommandCollection.
        :returns: A list of nested docutil nodes
        """
        ctx = click.Context(command, info_name=name, parent=parent)

        if command.hidden:
            return []

        # Title

        if parent is not None:
            if parent.info_name == 'haienv' and ctx.info_name == "push":
                return []

        pname = parent.info_name + ' ' if parent else ''
        section = nodes.section(
            '',
            nodes.title(text=pname + name),
            ids=[nodes.make_id(ctx.command_path)],
            names=[nodes.fully_normalize_name(ctx.command_path)],
        )

        # Summary
        source_name = ctx.command_path
        result = statemachine.ViewList()

        if semantic_group:
            lines = _format_description(ctx)
        else:
            lines = _format_command(ctx, nested, commands)

        for line in lines:
            LOG.debug(line)
            result.append(line, source_name)

        sphinx_nodes.nested_parse_with_titles(self.state, result, section)

        # Subcommands

        if nested == NESTED_FULL:
            if isinstance(command, click.CommandCollection):
                for source in command.sources:
                    section.extend(
                        self._generate_nodes(
                            source.name,
                            source,
                            parent=ctx,
                            nested=nested,
                            semantic_group=True,
                        )
                    )
            else:
                commands = _filter_commands(ctx, commands)
                for command in commands:
                    parent = ctx if not semantic_group else ctx.parent
                    section.extend(
                        self._generate_nodes(
                            command.name, command, parent=parent, nested=nested
                        )
                    )

        return [section]

    def run(self):
        self.env = self.state.document.settings.env

        command = self._load_module(self.arguments[0])

        if 'prog' not in self.options:
            raise self.error(':prog: must be specified')

        prog_name = self.options.get('prog')
        show_nested = 'show-nested' in self.options
        nested = self.options.get('nested')

        if show_nested:
            if nested:
                raise self.error(
                    "':nested:' and ':show-nested:' are mutually exclusive"
                )
            else:
                warnings.warn(
                    "':show-nested:' is deprecated; use ':nested: full'",
                    DeprecationWarning,
                )
                nested = NESTED_FULL if show_nested else NESTED_SHORT

        commands = self.options.get('commands')

        return self._generate_nodes(prog_name, command, None, nested, commands)


def setup(app):
    app.add_directive('click', ClickDirective)

    return {
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
