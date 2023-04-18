import os
import ast
import astunparse
from argparse import ArgumentParser


"""
根据 external 环境变量来 patch 文件，支持以下用法：

MATCH_CONDITION = 
                    os.environ.get('external') == 'true' | 
                    os.environ.get('external') != 'true'

if <IF_CLAUSE>:
    <EXPRESSION>
[ELIF_EXPRESSION]
[ELSE_EXPRESSION]
"""


MATCH_AST = ast.parse("""
import os
if os.environ.get('external') == 'true':
    pass
else:
    pass
if os.environ.get('external') != 'true':
    pass
""")
EXTERNAL_IF_AST_DUMP = ast.dump(MATCH_AST.body[1].test)
INTERNAL_IF_AST_DUMP = ast.dump(MATCH_AST.body[2].test)
IS_EXTERNAL = os.environ.get('external') == 'true'


def patch_user_role(code_ast: ast.AST):
    if hasattr(code_ast, 'body'):
        patched_body = []
        for sub_code_ast in code_ast.body:
            if isinstance(sub_code_ast, ast.If):
                # 判断是不是 if os.environ.get('external') == 'true': xxx else: xxx 这个语句
                # 不可能存在嵌套情况，直接替换就行了
                if ast.dump(sub_code_ast.test) == EXTERNAL_IF_AST_DUMP:
                    if IS_EXTERNAL:
                        patched_body += sub_code_ast.body
                    else:
                        patched_body += sub_code_ast.orelse
                elif ast.dump(sub_code_ast.test) == INTERNAL_IF_AST_DUMP:
                    if IS_EXTERNAL:
                        patched_body += sub_code_ast.orelse
                    else:
                        patched_body += sub_code_ast.body
                else:
                    # 不是待 patch 的 if 语句
                    patched_body.append(patch_user_role(sub_code_ast))
            else:
                patched_body.append(patch_user_role(sub_code_ast))
        code_ast.body = patched_body
        return code_ast
    else:
        return code_ast


if __name__ == '__main__':
    parser = ArgumentParser('patch user role')
    parser.add_argument('--hfai_path', required=True, help='要 patch 的 hfai 目录')
    options, _ = parser.parse_known_args()
    for root_dir, _, files in os.walk(options.hfai_path):
        for file in files:
            if file == 'patch_client.py' or not file.endswith('.py'):
                continue
            file_to_patch = os.path.join(root_dir, file)
            with open(file_to_patch, 'r') as f:
                file_content = f.read()
            if "if os.environ.get('external') == 'true':" in file_content or "if os.environ.get('external') != 'true':" in file_content:
                print(f'patching {file_to_patch}')
                code_ast = ast.parse(file_content)
                with open(file_to_patch, 'w') as f:
                    f.write(astunparse.unparse(patch_user_role(code_ast)))
