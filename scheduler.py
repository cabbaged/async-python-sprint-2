import logging
import pickle
import time
from pprint import pformat
from threading import Event, Thread
from typing import Dict, Generator, Optional

from constants import DATA_PATH, TaskCommand, TaskState
from job import Job


class Scheduler(Thread):
    def __init__(self, pool_size: int = 10):
        super().__init__()
        self.active_tasks: Dict[str, Generator] = {}
        self.pool_size = pool_size
        self.stop_event = Event()
        self.dump_event = Event()

    def schedule(self, task: Job, initial_status: Optional[TaskState] = None):
        job = task.run(initial_status)
        if len(self.active_tasks) > self.pool_size:
            raise Exception('Pool size exceeded')
        self.active_tasks[task.name] = job
        self.logger.info(f'scheduled {task}')

    def run(self):
        self.logger.info('run')
        while not self.stop_event.is_set():
            for name, task in list(self.active_tasks.items()):
                try:
                    status = next(task)
                    if status == TaskState.FINISH:
                        self.active_tasks.pop(name)
                except StopIteration:
                    pass
            if not self.active_tasks:
                break
            time.sleep(0.1)
        self.logger.info('setting dumping event')
        self.dump_event.set()

    def stop(self):
        self.logger.info('setting stop event')
        self.stop_event.set()
        while not self.dump_event.is_set():
            time.sleep(0.1)
        for name, task in self.active_tasks.items():
            self.logger.info(f'sending dump command to {name}')
            self.send_dump_command(task)

    def restore_jobs(self):
        jobs = []
        for dir in DATA_PATH.rglob('*'):
            if dir.name == 'picled':
                with dir.open('rb') as f:
                    jobs.append(pickle.load(f))
        self.logger.info('jobs statuses')
        for job in jobs:
            self.logger.info(pformat(vars(job)))
            self.schedule(job, job.status)
        self.run()

    def send_dump_command(self, task: Generator):
        try:
            task.send(TaskCommand.DUMP)
        except StopIteration:
            self.logger.info('task already finished')

    @property
    def logger(self):
        return logging.getLogger('Scheduler')
