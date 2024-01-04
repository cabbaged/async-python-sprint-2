from logger import setup_logging
from scheduler import Scheduler

if __name__ == '__main__':
    setup_logging()
    scheduler = Scheduler()
    scheduler.restore_jobs()
