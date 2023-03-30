import json
import os
import re
from datetime import date, datetime, timedelta
from os import path as os_path, sep, listdir, walk, stat, makedirs
from re import compile as re_compile
from shutil import copy2, make_archive
from typing import Union, List
from zipfile import ZipFile, ZIP_DEFLATED


class UnsupportedBackupTask(Exception):
    pass


def computer_name():
    return os.environ['COMPUTERNAME']


def read(folder: str, file: str, full_path: str = None, encoding: str = "UTF-8"):
    if folder and file:
        full_path = os_path.join(folder, file)
    with open(full_path, 'r', encoding=encoding) as f:
        return f.read()


def read_lines(folder: str, file: str, full_path: str = None, encoding: str = "UTF-8"):
    if folder and file:
        full_path = os_path.join(folder, file)
    with open(full_path, 'r', encoding=encoding) as f:
        return f.readlines()


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


def split_file_name(file_name: str) -> (str, str):
    file_name, ext = os.path.splitext(file_name)
    return file_name, ext[1:]


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


def add_file_name_prefix(file_name: str, prefix: str, separator: str = "_") -> str:
    file_name, ext = split_file_name(file_name)
    return f"{file_name}{separator}{prefix}.{ext}"


def remove_root_from_path(path, root_path):
    return os_path.normpath(path)[len(os_path.normpath(root_path)):]


def remove_extension_from_path(path):
    ext = get_extension(path)
    if ext:
        return path[:-(len(ext) + 1)]
    else:
        return path


def get_delta(delta: int, delta_unit: str):
    weeks = delta if delta_unit == "week" else 0
    days = delta if delta_unit == "day" else 0
    hours = delta if delta_unit == "hour" else 0
    minutes = delta if delta_unit == "minute" else 0
    return timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes)


def purge_archive(path: str, depth: dict, extensions: list[str], mask: str):
    days = int(depth.get('day', "0"))
    weeks = int(depth.get('week', "0"))
    monthes = int(depth.get('month', "0"))
    year = int(depth.get('year', "0"))
    for p, f in walk_through_files(path, extensions):
        # print(p)
        pass
        # TODO: Add checks how old the file and remove if it's


def zip_backup(source, target, freq, file_format, base_name = '', freq_unit = 'day', arch_depth = {}):
    if not os_path.exists(target) or not os_path.isdir(target):
        raise NotADirectoryError(f"Directory {target} does not exist")
    if arch_depth:
        purge_archive(target, arch_depth, ['.' + file_format], '*')
    bo_base_name = base_name if base_name else get_last_part(source)
    bo_target_path = get_last_backup_file(target, bo_base_name, ['.' + file_format])
    bo_modify_dt = get_modify_dt(bo_target_path)
    delta = get_delta(freq, freq_unit)
    if not bo_modify_dt or bo_modify_dt < datetime.today() - delta:
        if os_path.exists(source):
            bo_base_name = bo_base_name + f'_{datetime.today():%Y-%m-%d-%H-%M}'
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


def folder_exists(folder: str = None) -> bool:
    return os_path.exists(folder)


def folder_create(folder: str):
    if not os_path.exists(folder):
        makedirs(folder)


def get_file_size(folder: str, file_name: str) -> int:
    file_path = join_paths(folder, file_name)
    return stat(file_path).st_size


def save_as_json(data, folder_path: str, file_name: str, data_cls = None):
    if not file_name.endswith(".json"):
        file_name += ".json"
    path = join_paths(folder_path, file_name)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4, cls=data_cls)


def backup(backup_tasks: List):
    for task in backup_tasks:
        if task['type'] == 'zip':
            zip_backup(source=task['source'], 
                       target=task['target'], 
                       freq=task['freq'],
                       freq_unit=task['freq_unit'],
                       file_format=task['file_format'],
                       base_name=task.get('base_name', ''), 
                       arch_depth=task.get('arch_depth', {}))
        else:
            raise UnsupportedBackupTask(task)


def write_to_file(path: str, content: str):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)


def rename_file(src: str, dst: str, path: str = None):
    if path:
        src = join_paths(path, src)
        dst = join_paths(path, dst)
    os.rename(src, dst)
