from scheduler.crontab_scheduler import CrontabScheduler
import yaml
from typing import Dict
import subprocess
from scheduler.scheduler import Scheduler
from task.pyspark_task import PySparkTask
from task.task import Task

_SCHEDULER_TYPES = {'Crontab': CrontabScheduler}
_TASK_TYPES = {'PySpark': PySparkTask}


class OfflineFlow(FlowExecutor):
    def __init__(self, is_local_mode=True):
        self._is_local_mode = is_local_mode
        self._config_file = 'example/offline_flow.yml'
        _offline_obj = self._get_offline_obj()
        _tasks_conf = _offline_obj['Tasks']
        self._customer_conf_path = _offline_obj['CustomerConfPath']
        self._schedulers_conf = _offline_obj['Schedulers']
        self._tasks: Dict[str, Task] = self._get_tasks(_tasks_conf)
        self._schedulers: self.Schedulers = self.Schedulers(self._schedulers_conf,
                                                            self._tasks,
                                                            self._is_local_mode,
                                                            self._customer_conf_path)

    def start(self):
        self._schedulers.start()

    def _get_offline_obj(self):
        with open(self._config_file, 'r') as stream:
            yaml_obj = yaml.full_load(stream)
            offline_obj = yaml_obj['OfflineFlow']
        return offline_obj

    def _get_tasks(self, tasks_conf) -> Dict[str, Task]:
        return self.Tasks(tasks_conf).get_tasks(self._customer_conf_path)

    class Schedulers:

        def __init__(self, schedulers_conf, tasks, is_local_mode, customer_conf_path):
            self._customer_conf_path = customer_conf_path
            self._is_local_mode = is_local_mode
            self._local_container_name = 'metaspore_offline'
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
                scheduler_name = scheduler_conf['name']
                scheduler_type = scheduler_conf['type']
                scheduler = self._create_scheduler(scheduler_type,
                                                   scheduler_conf)
                schedulers[scheduler_name] = scheduler
            return schedulers

        def _create_scheduler(self, scheduler_type, schedulers_conf):
            try:
                return _SCHEDULER_TYPES[scheduler_type](schedulers_conf, self._tasks, self._customer_conf_path)
            except Exception:
                raise Exception('Invalid scheduler type')

        def __init_schedulers_env(self):
            # self._tasks.values()
            pass

        def _init_crontab_env(self):
            if self._is_local_mode:
                clear_crontab_history = 'crontab -r'
                clear_crontab_history_cmd = ['docker', 'exec', '-i', self._local_container_name,
                                             '/bin/bash', '-c', clear_crontab_history]
                subprocess.run(clear_crontab_history_cmd)

    class Tasks:

        def __init__(self, tasks_conf):
            # todo: validate tasks_conf: check duplicate task name
            self._tasks_conf = tasks_conf

        def get_tasks(self, customer_conf_path) -> Dict[str, Task]:
            return self._create_tasks(customer_conf_path)

        def _create_tasks(self, customer_conf_path) -> Dict[str, Task]:
            tasks: Dict[str, Task] = {}
            for task in self._tasks_conf:
                task_name = task['name']
                task_type = task['type']
                task_meta = task['meta']
                customerParams = task['customerParams']
                task = self._create_task(task_name,
                                         task_type,
                                         task_meta,
                                         customerParams,
                                         customer_conf_path)
                tasks[task_name] = task
            return tasks

        def _create_task(self,
                         name,
                         type,
                         meta,
                         customerParams,
                         customer_conf_path):
            try:
                return _TASK_TYPES[type](name,
                                         type,
                                         meta,
                                         customerParams,
                                         customer_conf_path)
            except Exception:
                raise Exception('Invalid scheduler type')
