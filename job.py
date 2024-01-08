import abc
import datetime
import logging
import pickle
import random
import time
from pathlib import Path
from pprint import pformat
from typing import Dict, List, Optional

from constants import DATA_PATH, TaskCommand, TaskState
from lock_manager import LockManager


def get_file_size(path):
    return Path(path).stat().st_size


class Job:
    def __init__(self, start_at: Optional[datetime.datetime] = None, max_working_time: datetime.timedelta = datetime.timedelta(),
                 restarts: int = 0, dependencies: Optional[List] = None, target_kwargs: Optional[Dict] = None):
        self.start_at = start_at or datetime.datetime.now()
        self.max_working_time = max_working_time
        self.restarts = restarts
        self.dependencies = dependencies or []
        self.tries = 0
        self.target_kwargs = target_kwargs or {}
        self.name = 'Job ' + str(random.randint(1, 1000))
        self.initial_status = None

    def __str__(self):
        return f'{self.name}'

    def run(self, initial_status: Optional[TaskState] = None):
        self.initial_status = initial_status
        self._create_job_lock()
        yield from self._wait_dependencies()
        yield from self._wait_start_time()
        yield from self._run_target_job()
        yield from self._finish_job()

    @abc.abstractmethod
    def target(self, **kwargs):
        pass

    def _create_job_lock(self):
        self.logger.info('creating lock')
        LockManager.create_lock(self.name)

    def _finalize_job_lock(self):
        self.logger.info('removing lock')
        LockManager.finalize_lock(self.name)

    def _finish_job(self):
        self._finalize_job_lock()
        self.logger.info('finished')
        yield from self._communicate(TaskState.FINISH)

    def _communicate(self, status: TaskState):
        self.status = status
        self.logger.info(f'sending status {status}')
        command = yield status
        if command == TaskCommand.DUMP:
            self.logger.info(f'recieved {command}')
            self._dump_self()
            yield

    def _dump_self(self):
        path = DATA_PATH / self.name / 'picled'
        path.touch()
        with path.open('wb') as f:
            pickle.dump(self, f)
        self.logger.info(f'dumped {pformat(vars(self))}')

    def _wait_dependencies(self):
        if self.initial_status and self.initial_status != TaskState.WAIT_DEPENDENCIES:
            self.logger.info('skipping wait dependencies step')
            return
        self.logger.info('waiting for dependencies')
        while self.dependencies:
            for dep in self.dependencies:
                if LockManager.is_finished(dep):
                    self.dependencies.remove(dep)
            if self.dependencies:
                yield from self._communicate(TaskState.WAIT_DEPENDENCIES)
        self.logger.info('all dependencies are complete')

    def _wait_start_time(self):
        if self.initial_status and self.initial_status != TaskState.WAIT_STARTTIME:
            self.logger.info('skipping wait starttime step')
            return
        self.logger.info('waiting starttime')
        while datetime.datetime.now() < self.start_at:
            time.sleep(0.1)
            yield from self._communicate(TaskState.WAIT_STARTTIME)
        self.logger.info('starttime reached')

    def _run_target_job(self):
        self.logger.info(f'run target {self.__class__.__name__}')
        self.job_start = datetime.datetime.now()
        while True:
            try:
                self.target(**self.target_kwargs)
                break
            except Exception as err:
                self.logger.info(f'error {self}')
                self.logger.info(err)
                self.restarts -= 1
                if self.restarts < 0:
                    self.logger.info('run target: restarts exceeded')
                    yield from self._finish_job()
                else:
                    self.logger.info('run target: attempt failure')
                    yield from self._finish_job()

        if self.max_working_time and datetime.datetime.now() - self.job_start > self.max_working_time:
            self.logger.info('run target: max working time exceeded')
            yield from self._communicate(TaskState.FINISH)

        self.logger.info('run target: success')

    @property
    def logger(self):
        return logging.getLogger(self.name)
