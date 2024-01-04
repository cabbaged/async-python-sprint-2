from constants import DATA_PATH
from data_utils import DataUtils


class LockManager:
    @staticmethod
    def create_lock(name: str):
        lock_dir = DATA_PATH / name
        DataUtils.create_dir(lock_dir)
        lock_file = lock_dir / '.lock'
        lock_file.write_text('running')

    @staticmethod
    def finalize_lock(name: str):
        lock_dir = DATA_PATH / name
        DataUtils.create_dir(lock_dir)
        lock_file = lock_dir / '.lock'
        lock_file.write_text('finished')

    @staticmethod
    def is_finished(name: str):
        lock_file = DATA_PATH / name / '.lock'
        if lock_file.exists():
            return lock_file.read_text() == 'finished'
        else:
            return False
