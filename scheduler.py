import datetime
from threading import Thread, Event
import time
from task_state import TaskState
from job import Job, create_network_job
from pathlib import Path
DATA_PATH = Path(__file__).parent / 'artifacts'


class Scheduler(Thread):
    def __init__(self, pool_size=10):
        super().__init__()
        self.active_tasks = []
        self.stop_event = Event()
        self.wrap_up_event = Event()

    def schedule(self, task):
        job = task.run()
        self.active_tasks.append(job)

    def run(self):
        while not self.stop_event.is_set():
            for task in self.active_tasks:
                try:
                    status = next(task)
                    if status == TaskState.FINISH:
                        task.send("HOORAY")
                        self.active_tasks.remove(task)
                except StopIteration:
                    pass
        print('wrap up event up')
        self.wrap_up_event.set()
        print(self.wrap_up_event.is_set())

    def restart(self):
        pass

    def stop(self):
        self.stop_event.set()
        while not self.wrap_up_event.is_set():
            print('here')
            time.sleep(0.1)
        for task in self.active_tasks:
            print(len(self.active_tasks))
            self.send_wrap_up(task)

    def send_wrap_up(self, task):
        try:
            print(f'sending wrap up command to {task}')
            task.send('wrap up')
            print('wrap up command sent')
        except StopIteration:
            print(f'task {task} already finished')

    def restore_jobs(self):
        jobs = []
        for dir in DATA_PATH.rglob('*'):
            if dir.name == 'picled':
                with dir.open('rb') as f:
                    import pickle

                    jobs.append(pickle.load(f))
        print(jobs)
        for job in jobs:
            from pprint import pprint
            pprint(vars(job))


# if __name__ == '__main__':
#     scheduler = Scheduler()
#     scheduler.restore_jobs()


#
if __name__ == '__main__':
    job1 = create_network_job('http://google.com', restarts=1, start_at = datetime.datetime.now() + datetime.timedelta(seconds=2))
    job2 = create_network_job('yandex.ru', restarts=1, dependencies=[job1.name])
    job3 = create_network_job('http://google.com', restarts=1)
    scheduler = Scheduler()
    scheduler.schedule(job1)
    scheduler.schedule(job2)
    scheduler.schedule(job3)
    scheduler.start()
    time.sleep(2)
    scheduler.stop()
