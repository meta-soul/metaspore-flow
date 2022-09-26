from metasporeflow.executors.local_offline_executor import LocalOfflineFlowExecutor
from metasporeflow.flows.flow_loader import FlowLoader
from metasporeflow.flows.metaspore_oflline_flow import OfflineScheduler, OfflineTask

flow_loader = FlowLoader()
resources = flow_loader.load()

schedulers = resources.find_all(OfflineScheduler)
tasks = resources.find_all(OfflineTask)
print(type(schedulers))

for scheduler in schedulers:
    print(type(scheduler))
    print(scheduler.data)

for task in tasks:
    print(task.name)
    print(task.kind)
    print(task.path)
    print(task.data)

import asyncio

flow_executor = LocalOfflineFlowExecutor(resources)
asyncio.run(flow_executor.execute_up())
