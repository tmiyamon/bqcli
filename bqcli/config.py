import pathlib


class Config:
    DEFAULT_DIR_NAME = 'bqcli'

    def __init__(self, dir_name=DEFAULT_DIR_NAME):
        self.dir_name = dir_name

    def prepare(self):
        self.dir_path.mkdir(exist_ok=True)

    @property
    def dir_path(self):
        return pathlib.Path.home() / '.config' / self.dir_name

    @property
    def history_path(self):
        return self.dir_path / 'history'

