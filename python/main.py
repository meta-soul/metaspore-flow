from metasporeflow.executors.local_flow_executor import LocalFlowExecutor
from metasporeflow.flows.flow_loader import FlowLoader
from metasporeflow.flows.metaspore_oflline_flow import OfflineScheduler, OfflineTask
from metasporeflow.online.online_flow import OnlineFlow
import asyncio

flow_loader = FlowLoader()
resources = flow_loader.load()

online_flow = resources.find_by_type(OnlineFlow)
print(type(online_flow))
print(online_flow)

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
flow_executor = LocalFlowExecutor(resources)
asyncio.run(flow_executor.execute_up())
