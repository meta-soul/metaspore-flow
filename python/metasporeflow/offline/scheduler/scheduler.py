from abc import ABC, abstractmethod
from typing import Dict, List

import networkx as nx

from task.task import Task


class Scheduler(ABC):
    def __init__(self, schedulers_conf, tasks: Dict[str, Task], customer_conf_path):
        self.name = schedulers_conf['name']
        self.type = schedulers_conf['type']
        self.cronExpr = schedulers_conf['cronExpr']
        self._customer_conf_path = customer_conf_path
        self._dag = self._get_dag(schedulers_conf['dag'])
        # self._tasks: Dict[str, Task] = tasks
        self._dag_tasks: List[Task] = self._get_dag_tasks(tasks)

    @abstractmethod
    def publish(self):
        raise NotImplementedError

    def _get_dag(self, dag_conf):
        tuples = []
        for k, value_list in dag_conf.items():
            for v in value_list:
                tuples.append((k, v))
        dag = nx.DiGraph()
        dag.add_edges_from(tuples)
        if not self._is_directed_acyclic_graph(dag):
            raise Exception(
                "%s dag is not a directed acyclic graph" % self.name)
        return dag

    def _is_directed_acyclic_graph(self, dag):
        return nx.is_directed_acyclic_graph(dag)

    def _get_dag_tasks(self, tasks: Dict[str, Task]) -> List[Task]:
        dag_tasks = []
        for task in self._dag.nodes():
            dag_tasks.append(tasks[task])
        return dag_tasks
