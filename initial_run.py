import datetime
import time

from jobs import AnalyzeFileJob, DeleteFileJob, NetworkJob
from logger import setup_logging
from scheduler import Scheduler

if __name__ == '__main__':
    setup_logging()
    job1 = NetworkJob(
        target_kwargs={'url': 'http://google.com'},
        restarts=1,
        start_at=datetime.datetime.now() + datetime.timedelta(seconds=2)
    )
    job2 = NetworkJob(
        target_kwargs={'url': 'yandex.ru'},
        restarts=3,
        dependencies=[job1.name]
    )
    job3 = NetworkJob(
        target_kwargs={'url': 'http://google.com'},
        restarts=1
    )
    job4 = AnalyzeFileJob(
        target_kwargs={'name': job1.name},
        dependencies=[job1.name]
    )
    job5 = DeleteFileJob(
        target_kwargs={'name': job1.name},
        dependencies=[job1.name, job4.name]
    )
    scheduler = Scheduler()
    scheduler.schedule(job1)
    scheduler.schedule(job2)
    scheduler.schedule(job3)
    scheduler.schedule(job4)
    scheduler.schedule(job5)
    scheduler.start()
    time.sleep(2)
    scheduler.stop()
