import re
from datetime import date, datetime, timedelta
from os import path as os_path, sep, listdir, walk, stat
from re import compile as re_compile
from shutil import copy2, make_archive
from typing import Union, List
from zipfile import ZipFile, ZIP_DEFLATED


class UnsupportedBackupTask(Exception):
    pass


def read(folder: str, file: str, full_path: str = None, encoding: str = None):
    if folder and file:
        full_path = os_path.join(folder, file)
    with open(full_path, 'r', encoding=encoding) as f:
        return f.read()


def write(folder: str, file: str, text: str, full_path: str = None, encoding: str = None):
    if folder and file:
        full_path = os_path.join(folder, file)
    with open(full_path, 'w', encoding=encoding) as f:
        return f.write(text)


def copy(source, destination):
    copy2(source, destination)


def join_paths(root_path: str, *child_paths):
    result_path = os_path.normpath(root_path)
    for child_path in child_paths:
        child_path = os_path.normpath(child_path)
        if child_path.startswith(os_path.sep):
            result_path += child_path
        else:
            result_path += sep + child_path
    return result_path


def get_extension(path: str):
    ext = path.split('.')[-1].lower()
    if ext and len(ext) in range(1, 5) and '\\' not in ext and '/' not in ext:
        return ext


def get_modify_dt(path: str, with_time: bool = True) -> Union[datetime, date]:
    if path and os_path.exists(path):
        mtime = os_path.getmtime(path)
        mdt = datetime.fromtimestamp(mtime)
        return mdt if with_time else mdt.date()


def get_last_part(path: str):
    last_part = os_path.split(path)[-1].replace(' ', '_')
    if os_path.exists(path) and os_path.isfile(path):
        last_part = '.'.join(last_part.split('.')[:-1])  # exclude extension
    return last_part


def get_last_backup_file(path: str, file_name: str, extensions: list):
    max_date = datetime.today() - timedelta(days=500)
    last_file = None
    for path, file in walk_through_files(path, extensions):
        c = re_compile(file_name + r'_\d\d\d\d\d\d\d\d\.zip')
        if c.search(file):
            file_mdt = get_modify_dt(path)
            if file_mdt > max_date:
                max_date = file_mdt
                last_file = path
    return last_file


def get_folders_list(path):
    return listdir(path)


def walk_through_files(root_path: str,
                       extensions: list,
                       exclusions: list = None,
                       start_date: datetime = None,
                       only_top: bool = False,
                       re_pattern: str = None):
    exclusions = exclusions if exclusions else []
    for root, dirs, files in walk(root_path):
        for file in files:
            file_name = os_path.split(file)[1]
            ext = os_path.splitext(file_name)[1]
            mdt = get_modify_dt(join_paths(root, file))
            if file_name.startswith('~'):
                continue
            if len(extensions) > 0 and ext not in extensions:
                continue
            if file in exclusions:
                continue
            if re_pattern and not re.match(re_pattern, file):
                continue
            if start_date and mdt < start_date:
                continue
            yield join_paths(root, file), file
        if only_top:
            break


def find_extension(path_wo_extension, extensions):
    for ext in extensions:
        path = path_wo_extension + '.' + ext
        if os_path.exists(path):
            return ext


def remove_root_from_path(path, root_path):
    return os_path.normpath(path)[len(os_path.normpath(root_path)):]


def remove_extension_from_path(path):
    ext = get_extension(path)
    if ext:
        return path[:-(len(ext) + 1)]
    else:
        return path


def zip_backup(source, target, depth, file_format):
    bo_base_name = get_last_part(source)
    bo_target_path = get_last_backup_file(target, bo_base_name, ['.' + file_format])
    bo_base_name = bo_base_name + f'_{date.today():%Y%m%d}'
    bo_modify_dt = get_modify_dt(bo_target_path)
    if not bo_modify_dt or bo_modify_dt < datetime.today() - timedelta(days=depth):
        if os_path.exists(source):
            if os_path.isdir(source):
                make_archive(join_paths(target, bo_base_name), file_format, source)
            else:
                zip_path = join_paths(target, bo_base_name + '.' + file_format)
                ZipFile(zip_path, 'w', ZIP_DEFLATED).write(source, os_path.basename(source))


def file_exists(folder: str = None,
                file_name: str = None,
                file_path: str = None) -> bool:
    if not file_path:
        file_path = join_paths(folder, file_name)
    return os_path.exists(file_path)


def get_file_size(folder: str, file_name: str) -> int:
    file_path = join_paths(folder, file_name)
    return stat(file_path).st_size


def backup(backup_tasks: List):
    for task in backup_tasks:
        if task['type'] == 'zip':
            zip_backup(task['source'], task['target'], task['depth'], task['file_format'])
        else:
            raise UnsupportedBackupTask(task)
