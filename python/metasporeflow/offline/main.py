from metasporeflow.flows.flow_loader import FlowLoader
from metasporeflow.flows.metaspore_oflline_flow import OfflineScheduler, OfflineTask

flow_loader = FlowLoader()
resources = flow_loader.load()
# flow_executor = LocalOfflineFlowExecutor(resources)

schedulers = resources.find_all(OfflineScheduler)
tasks = resources.find_all(OfflineTask)

for scheduler in schedulers:
    print(scheduler.name)
    print(scheduler)

for task in tasks:
    print(task.name)
