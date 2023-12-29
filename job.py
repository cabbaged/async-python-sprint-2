import datetime
import requests
from pathlib import Path
from threading import Thread
from functools import partial
from task_state import TaskState

DATA_PATH = Path(__file__).parent / 'artifacts'


def create_dir(dir):
    dir.mkdir(parents=True, exist_ok=True)


def is_job_finished(job_name):
    lock_file = DATA_PATH / job_name / '.lock'
    if lock_file.exists():
        return lock_file.read_text() == 'running'
    else:
        return False


def create_job_lock(job_name):
    lock_file = DATA_PATH / job_name / '.lock'
    return lock_file.exists()


def finalize_job_lock(job_name):
    lock_file = DATA_PATH / job_name / '.lock'
    lock_file.write_text('True')


def dump_data(file: Path, data):
    output_file = DATA_PATH / file
    output_file.write_text(data)


import uuid


def download(url):
    result = requests.get(url)
    dump_data(DATA_PATH / str(uuid.uuid4()), result.content.decode())


def get_file_size(path):
    return Path(path).stat().st_size


def create_network_job(url, **kwargs):
    return Job(target=partial(download, url), **kwargs)


class Job:
    def __init__(self, target, start_at="", max_working_time=-1, restarts=0, dependencies=[]):
        self.start_at = start_at or datetime.datetime.now()
        self.max_working_time = max_working_time
        self.restarts = restarts
        self.dependencies = dependencies
        self.tries = 0
        self.target = target
        self.name = str(uuid.uuid4())

    def run(self):
        self._create_job_lock()
        print(f'Run job {self}')
        while self.dependencies:
            for dep in self.dependencies:
                if self.is_job_finished(dep):
                    self.dependencies.remove(dep)
            if self.dependencies:
                yield from self._send_status_and_gather_commands(TaskState.WAIT_DEPENDCIES)

        while datetime.datetime.now() < self.start_at:
            yield from self._send_status_and_gather_commands(TaskState.WAIT_STARTTIME)

        while True:
            try:
                self.target()
                break
            except Exception as err:
                print(f'error {self}')
                print(err)
                self.restarts -= 1
                if self.restarts < 0:
                    yield from self._send_status_and_gather_commands(TaskState.FINISH)
                else:
                    self._dump_self()
                    yield from self._send_status_and_gather_commands(TaskState.ERROR)

        self._finalize_job_lock()
        self._dump_self()
        print(f'job finished {self}')



    def pause(self):
        pass

    def stop(self):
        pass

    def _create_job_lock(self):
        lock_dir = DATA_PATH / self.name
        create_dir(lock_dir)
        lock_file = lock_dir / '.lock'
        lock_file.write_text('running')

    def _finalize_job_lock(self):
        lock_dir = DATA_PATH / self.name
        create_dir(lock_dir)
        lock_file = lock_dir / '.lock'
        lock_file.write_text('finished')

    def _send_status_and_gather_commands(self, status):
        self.status = status
        command = yield status
        if command == 'wrap up':
            self._dump_self()

    def _dump_self(self):
        import pickle
        path = DATA_PATH / self.name / 'picled'
        path.touch()
        with path.open('wb') as f:
            pickle.dump(self, f)

    def is_job_finished(self, job_name):
        lock_file = DATA_PATH / job_name / '.lock'
        if lock_file.exists():
            return lock_file.read_text() == 'finished'
        else:
            return False
