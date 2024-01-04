from pathlib import Path

import requests

from data_utils import DataUtils
from job import Job


class NetworkJob(Job):
    def target(self, **kwargs):
        result = requests.get(**kwargs)
        DataUtils.dump_data(Path(self.name) / 'file', result.content.decode())


class CreateFileJob(Job):
    def target(self, **kwargs):
        name = kwargs.get('name')
        DataUtils.create_file(name)


class DeleteFileJob(Job):
    def target(self, **kwargs):
        name = kwargs.get('name')
        DataUtils.delete_file(name)


class AnalyzeFileJob(Job):
    def target(self, **kwargs):
        name = kwargs.get('name')
        DataUtils.analyze_file(name)
