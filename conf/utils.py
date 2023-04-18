import hashlib
import fnmatch
import mmap
import os
import shutil
import stat
import zipfile

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel


# 文件分片大小
slice_bytes = 104857600  # 100 * 1024 * 1024

# 北京时间
tz_utc_8 = timezone(timedelta(hours=8))

class FileType(str, Enum):
    # 私有、公开数据集
    DATASET = 'dataset'
    # 工作区
    WORKSPACE = 'workspace'
    # venv
    ENV = 'env'
    # hfai 文档
    DOC = 'doc'
    # hfai pip 源
    PYPI = 'pypi'
    # hfai 官网
    WEBSITE = 'website'


class FilePrivacy(str, Enum):
    PUBLIC = "public"
    GROUP_SHARED = "group_shared"
    PRIVATE = "private"


class DatasetType(str, Enum):
    FULL = 'full'
    MINI = 'mini'


class SyncStatus(str, Enum):
    '''
    数据库中push/pull的状态记录
    stage1, stage2 分别标识同步过程的两个阶段: 本地 - 云端 - 集群
    状态转换为:  stage1_running -> stage1_finished -> stage2_running -> finished
                                |                                    |
                                -> stage1_failed                     -> stage2_failed
    '''
    INIT = 'init'
    STAGE1_RUNNING = 'stage1_running'
    STAGE2_RUNNING = 'stage2_running'
    RUNNING = 'running'
    STAGE1_FINISHED = 'stage1_finished'
    FINISHED = 'finished'
    STAGE1_FAILED = 'stage1_failed'
    STAGE2_FAILED = 'stage2_failed'
    FAILED = 'failed'


class SyncDirection(str, Enum):
    PUSH = 'push'
    PULL = 'pull'


class FileInfo(BaseModel):
    path: str
    size: Optional[int] = None
    last_modified: Optional[str] = None
    md5: Optional[str] = None
    ignored: Optional[bool] = None


class FileInfoList(BaseModel):
    files: List[FileInfo]


class FileList(BaseModel):
    files: List[str]


@contextmanager
def directio_iter(filename, readsize, offset):
    if offset % mmap.PAGESIZE:
        raise ValueError(f"offset {offset} doesn't align with os PAGESIZE")
    fd = os.open(filename, os.O_RDONLY | os.O_DIRECT)
    try:
        with mmap.mmap(fd, readsize, access=mmap.ACCESS_READ, offset=offset) as m:
            yield m
    finally:
        os.close(fd)


def calculate_md5(file_name, size, directio=False):
    """
    计算文件的md5 hash
    @param file_name: 文件路径
    """
    md5 = hashlib.md5()
    chunk_size = 1 << 30  # 1G
    if directio:
        with open(file_name, mode='rb') as fobj:
            size = min(fobj.seek(0, os.SEEK_END), size)
        offset = 0
        while offset < size:
            read_size = min(size - offset, chunk_size)
            with directio_iter(file_name, read_size, offset) as m:
                if not m:
                    break
                offset += len(m)
                md5.update(m)
    else:
        with open(file_name, mode='rb') as fobj:
            while True:
                data = fobj.read(chunk_size)
                if not data:
                    break
                md5.update(data)
    return md5.hexdigest()


# 默认忽略文件
default_ignored_patterns = [
    '.vscode',
    '.idea',  # ide generated config
    '.git',
    '.gitignore',
    '.gitattributes',  # git
    '__pycache__',
]


def get_ignored_pattern(hfignore_path):
    """
    默认从workspace根目录读取.hfignore文件, 如没有, 则使用default_ignored_patterns
    规则为：
      *       匹配所有字符
      ?       匹配任意单个字符
      [seq]   匹配seq中的任意单个字符
      [!seq]  匹配任意不在seq中的单个字符
      不支持转义，即 \[ \? 等不会被解析
      末尾带 / 匹配目录下的所有内容，不包括目录本身；末尾不带 / 则匹配同名文件、同名目录和目录下的所有内容
      pattern按行优先, 在冲突情况下, 以前面的pattern为准

    示例:
      test?.py      匹配 testn.py
      test*.py      匹配 testabc.py
      test[0-5].py  匹配 test1.py, 不匹配 test6.py
      test[!0-5].py 匹配 test6.py, 不匹配 test1.py
      test          匹配 任意目录下 test 文件或 任意名为 test 的子目录及 test/ 目录下所有文件
      test/         匹配 任意名为 test 的子目录下所有文件
    """
    if os.path.exists(hfignore_path):
        with open(hfignore_path, 'r') as f:
            lines = list(f)
    else:
        lines = default_ignored_patterns
    patterns = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith('#') and not line.startswith(
                './') and not '\\' in line:
            line = line.lstrip('/')
            if line.endswith('/'):
                patterns.append(line + '*')
                patterns.append('*/' + line + '*')
            else:
                patterns.append(line)
                patterns.append(line + '/*')
                patterns.append('*/' + line)
                patterns.append('*/' + line + '/*')
    return patterns


def is_file_ignored(abspath, base_path, patterns, no_hfignore=False):
    if no_hfignore:
        return False
    subpath = abspath[len(base_path):]
    subpath = subpath.lstrip(os.path.sep)
    return any(fnmatch.fnmatch(subpath, p) for p in patterns)


def get_file_info(file_path, base_path, no_checksum, directio=False):
    key = file_path[len(base_path):].replace('//', '/').replace('\\', '/').lstrip('/')
    size = os.path.getsize(file_path)
    last_modified = datetime.fromtimestamp(
        os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
    if no_checksum:
        return FileInfo(path=key, size=size, last_modified=last_modified)
    else:
        md5 = calculate_md5(file_path, size, directio)
        return FileInfo(path=key,
                        size=size,
                        last_modified=last_modified,
                        md5=md5)


def list_local_files_inner(base_path, subpath, no_checksum=False, no_hfignore=False, recursive=True, directio=False):
    """
    获取本地文件详情列表
    @param base_path: 工作区目录
    @param subpath: 子目录
    @param no_checksum: 是否禁用checksum
    @param no_hfignore: 会否忽略hfignore
    @param recursive: 是否递归list子目录
    @return: FileInfo列表
    """
    ret = []
    base_path, subpath = os.path.normpath(base_path), os.path.normpath(subpath)
    root_path = os.path.abspath(f'{base_path}/{subpath}')
    if not root_path.startswith(base_path):
        return ret
    subpath = root_path[len(base_path):]
    patterns = get_ignored_pattern(f'{base_path}/.hfignore')

    if not os.path.exists(root_path):
        return ret

    if recursive:
        if is_file_ignored(root_path, base_path, patterns, no_hfignore):
            return ret
        if os.path.isfile(root_path):
            info = get_file_info(root_path, base_path, no_checksum, directio)
            ret.append(info)
            return ret

        for filepath, _, files in os.walk(root_path):
            if is_file_ignored(filepath, base_path, patterns, no_hfignore):
                continue
            for filename in files:
                fullpath = os.path.join(filepath, filename)
                if os.path.dirname(fullpath).split('/')[-1] == '.hfai' and '.zip' in os.path.basename(fullpath):
                    continue
                if is_file_ignored(fullpath, base_path, patterns, no_hfignore):
                    continue
                try:
                    info = get_file_info(fullpath, base_path, no_checksum, directio)
                except FileNotFoundError as e:
                    # print(f'本地文件 {fullpath} 被删除或链接不存在，忽略: {str(e)}')
                    continue
                ret.append(info)
    else:
        # 该分支仅给前端展示用
        if os.path.isfile(root_path):
            info = get_file_info(root_path, base_path, no_checksum, directio)
            if is_file_ignored(root_path, base_path, patterns, no_hfignore):
                info.ignored = True
            ret.append(info)
            return ret
        for f in os.listdir(root_path):
            key = f'{subpath}/{f}'.replace('//', '/').replace('\\', '/').lstrip('/').rstrip('/')
            file_path = f'{base_path}/{key}'
            try:
                if os.path.isdir(file_path):
                    key += '/'
                size = getPathSize(file_path)
                last_modified = datetime.fromtimestamp(
                    os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
                info = FileInfo(path=key, size=size, last_modified=last_modified)
                if is_file_ignored(file_path, base_path, patterns, no_hfignore):
                    info.ignored = True
                ret.append(info)
            except FileNotFoundError as e:
                # print(f'{file_path} 可能正在被删除: {str(e)}')
                continue

    return ret


def getPathSize(filePath):
    if os.path.isdir(filePath):
        size=0
        for root, _, files in os.walk(filePath):
            for f in files:
                try:
                    size += os.path.getsize(os.path.join(root, f))
                except FileNotFoundError:
                    continue
        return size
    else:
        return os.path.getsize(filePath)


def hashkey(*args):
    return hashlib.sha256(''.join(args).encode('utf-8')).hexdigest()


class MyZipFile(zipfile.ZipFile):
    '''
    extend builtin zipfile class with file mode persistence
    '''
    def _extract_member(self, member, targetpath, pwd):
        """Extract the ZipInfo object 'member' to a physical
           file on the path targetpath.
        """
        if not isinstance(member, zipfile.ZipInfo):
            member = self.getinfo(member)

        # build the destination pathname, replacing
        # forward slashes to platform specific separators.
        arcname = member.filename.replace('/', os.path.sep)

        if os.path.altsep:
            arcname = arcname.replace(os.path.altsep, os.path.sep)
        # interpret absolute pathname as relative, remove drive letter or
        # UNC path, redundant separators, "." and ".." components.
        arcname = os.path.splitdrive(arcname)[1]
        invalid_path_parts = ('', os.path.curdir, os.path.pardir)
        arcname = os.path.sep.join(x for x in arcname.split(os.path.sep)
                                   if x not in invalid_path_parts)
        if os.path.sep == '\\':
            # filter illegal characters on Windows
            arcname = self._sanitize_windows_name(arcname, os.path.sep)

        targetpath = os.path.join(targetpath, arcname)
        targetpath = os.path.normpath(targetpath)

        # Create all upper directories if necessary.
        upperdirs = os.path.dirname(targetpath)
        if upperdirs and not os.path.exists(upperdirs):
            os.makedirs(upperdirs)
            os.chmod(upperdirs, stat.S_IMODE(member.external_attr>>16))

        if member.is_dir():
            if not os.path.isdir(targetpath):
                os.mkdir(targetpath)
                os.chmod(targetpath, stat.S_IMODE(member.external_attr>>16))
            return targetpath

        with self.open(member, pwd=pwd) as source, \
             open(targetpath, "wb") as target:
            shutil.copyfileobj(source, target)
        os.chmod(targetpath, stat.S_IMODE(member.external_attr>>16))

        return targetpath


def zip_dir(base_path, target_files, zip_file_path, exclude_list):
    '''
    压缩目录到zip包
    '''
    os.makedirs(os.path.dirname(zip_file_path), exist_ok=True)
    f = MyZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED)
    if target_files is None:
        # 没传入子目录，则打包整个base_path
        for path, filepaths, filenames in os.walk(base_path):
            fpath = path[len(base_path):]
            if fpath in exclude_list:
                continue
            f.write(path, fpath)

            for filepath in filepaths:
                p = os.path.join(path, filepath)
                if p[len(base_path):] in exclude_list:
                    continue
                f.write(p, p[len(base_path):])

            for filename in filenames:
                if os.path.join(path, filename) == zip_file_path or filename in exclude_list:
                    continue
                f.write(os.path.join(path, filename), os.path.join(fpath, filename))
    else:
        dirnames = []
        for target_file in target_files:
            if target_file.path.split('/')[-1] in exclude_list:
                continue
            src_path = os.path.join(base_path, target_file.path)
            parent_path = base_path
            for target_file_subpath in target_file.path.split(os.path.sep)[:-1]:
                parent_path += f'{os.path.sep}{target_file_subpath}'
                if parent_path not in dirnames:
                    f.write(parent_path, parent_path[len(base_path):])
                    dirnames.append(parent_path)
            if src_path not in dirnames:
                f.write(src_path, target_file.path)
                dirnames.append(src_path)
    f.close()


def unzip_dir(zip_file_path, dst_dir):
    """
    解压缩zip包到指定路径
    """
    os.makedirs(dst_dir, exist_ok=True)
    f = MyZipFile(zip_file_path)
    f.extractall(dst_dir)
    f.close()
    return f.namelist()


def bytes_to_human(n):
    if n is None or n == '-':
        return '-'
    n = float(n) / 1024
    symbol = ['K', 'M', 'G', 'T', 'P', 'E']
    idx = 0
    while n >= 1024:
        n /= 1024
        idx += 1
    return '%.2f%sB' % (n, symbol[idx])
