from typing import Dict, Tuple
import subprocess

from metasporeflow.executors.local_flow_executor import LocalFlowExecutor
from metasporeflow.flows.metaspore_oflline_flow import OfflineScheduler, OfflineTask
from metasporeflow.offline.scheduler.crontab_scheduler import CrontabScheduler, \
    METASPORE_OFFLINE_FLOW_LOCAL_CONTAINER_NAME
from metasporeflow.offline.scheduler.scheduler import Scheduler
from metasporeflow.offline.task.offline_python_task import OfflinePythonTask
from metasporeflow.offline.task.task import Task
from metasporeflow.resources.resource import Resource

_SCHEDULER_TYPES = {'Crontab': CrontabScheduler}
_TASK_TYPES = {'OfflinePythonTask': OfflinePythonTask}


class LocalOfflineFlowExecutor(LocalFlowExecutor):
    def _initialize_(self):

        self._schedulers_conf: Tuple[Resource] = self._resources.find_all(OfflineScheduler)
        self._tasks_conf: Tuple[Resource] = self._resources.find_all(OfflineTask)
        self._tasks: Dict[str, Task] = self._get_tasks(self._tasks_conf)
        self._schedulers: LocalOfflineFlowExecutor.Schedulers = self.Schedulers(self._schedulers_conf,
                                                                                self._tasks)

    async def execute_up(self):
        self._initialize_()
        self._schedulers.start()

    def _get_tasks(self, tasks_conf) -> Dict[str, Task]:
        return self.Tasks(tasks_conf).get_tasks()

    class Schedulers:

        def __init__(self, schedulers_conf, tasks):
            self._schedulers_conf = schedulers_conf
            self._tasks = tasks
            self._schedulers: Dict[str, Scheduler] = self._get_schedulers()

        def start(self):
            self._init_crontab_env()
            for scheduler in self._schedulers.values():
                scheduler.publish()

        def _get_schedulers(self) -> Dict[str, Scheduler]:
            schedulers: Dict[str, Scheduler] = {}
            for scheduler_conf in self._schedulers_conf:
                scheduler_name = scheduler_conf.name
                scheduler_type = scheduler_conf.kind
                scheduler = self._create_scheduler(scheduler_type,
                                                   scheduler_conf)
                schedulers[scheduler_name] = scheduler
            return schedulers

        def _create_scheduler(self, scheduler_type, schedulers_conf):
            try:
                return _SCHEDULER_TYPES[scheduler_type](schedulers_conf, self._tasks)
            except Exception:
                raise Exception('Invalid scheduler type')

        def __init_schedulers_env(self):
            pass

        def _init_crontab_env(self):
            clear_crontab_history = 'crontab -r'
            clear_crontab_history_cmd = ['docker', 'exec', '-i', METASPORE_OFFLINE_FLOW_LOCAL_CONTAINER_NAME,
                                         '/bin/bash', '-c', clear_crontab_history]
            subprocess.run(clear_crontab_history_cmd)

    class Tasks:

        def __init__(self, tasks_conf):
            # todo: validate tasks_conf: check duplicate task name
            self._tasks_conf = tasks_conf

        def get_tasks(self) -> Dict[str, Task]:
            return self._create_tasks()

        def _create_tasks(self) -> Dict[str, Task]:
            tasks: Dict[str, Task] = {}
            for task in self._tasks_conf:
                task_name = task.name
                task_type = task.kind
                task_data = task.data
                task = self._create_task(task_name,
                                         task_type,
                                         task_data)
                tasks[task_name] = task
            return tasks

        def _create_task(self,
                         name,
                         type,
                         data):
            try:
                return _TASK_TYPES[type](name,
                                         type,
                                         data)
            except Exception:
                raise Exception('Invalid scheduler type')
