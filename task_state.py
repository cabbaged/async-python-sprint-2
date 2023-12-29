import enum


class TaskState(enum.Enum):
    WAIT_DEPENDCIES = 'wait dependencies'
    WAIT_STARTTIME = 'wait starttime'
    TIMEOUT = 'timeout'
    ERROR = 'error'
    FINISH = 'finish'
