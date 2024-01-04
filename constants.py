import enum
from pathlib import Path


class TaskState(enum.Enum):
    WAIT_DEPENDENCIES = 'wait dependencies'
    WAIT_STARTTIME = 'wait starttime'
    TIMEOUT = 'timeout'
    ERROR = 'error'
    FINISH = 'finish'


class TaskCommand(enum.Enum):
    DUMP = 'dump'


DATA_PATH = Path(__file__).parent / 'artifacts'
