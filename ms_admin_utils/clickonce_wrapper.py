from datetime import date, timedelta

from ms_admin_utils.file_wrapper import join_paths, get_folders_list, get_modify_dt


class ClickonceConfig:
    def __init__(self, **kwargs):
        self.root_path = kwargs.pop('root_path')
        self.app_folder = kwargs.pop('app_folder')


conf: ClickonceConfig


def configure(**kwargs):
    global conf
    conf = ClickonceConfig(**kwargs)


def _walk_clickonce_versions(app_path: str) -> (str, date):
    for app in get_folders_list(app_path):
        version_number = '.'.join(app.split('_')[1:])
        yield version_number, get_modify_dt(join_paths(app_path, app), False)


def _get_next_version_name(version_name):
    parts = version_name.split('.')
    number = int(parts[-2] if parts[-1] == 'beta' else parts[-1]) + 1
    if parts[-1] == 'beta':
        parts[-2] = str(number)
    else:
        parts[-1] = str(number)
    return '.'.join(parts)


class ClickonceApplication:
    @property
    def last_version(self):
        return self.versions[self.__last_version_name] if self.__last_version_name else None

    def __init__(self, **kwargs):
        self.folder = kwargs.pop('folder')
        self.is_beta = kwargs.pop('is_beta', False)
        self.prefix = kwargs.pop('prefix')
        self.versions: {ClickonceVersion} = {}
        self.__last_version_name = ''
        self.load_versions()
        self.next_version_name = _get_next_version_name(self.last_version.name)
        self.next_start_date = self.last_version.publish_date + timedelta(days=1)

    def load_versions(self):
        app_path = join_paths(conf.root_path, self.folder, conf.app_folder)
        for v, d in _walk_clickonce_versions(app_path):
            cov = ClickonceVersion(self, v, d)
            self.versions[cov.name] = cov
            if self.last_version:
                if cov.publish_date > self.last_version.publish_date:
                    self.__last_version_name = cov.name
            else:
                self.__last_version_name = cov.name


class ClickonceVersion:
    @property
    def name(self):
        return f'{self.__app.prefix}_{self.version_number}{".beta" if self.__app.is_beta else ""}'

    def __init__(self, app: ClickonceApplication, version_number: str, publish_date: date):
        self.__app = app
        self.version_number = version_number
        self.publish_date = publish_date
