#
# Copyright 2022 DMetaSoul
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from .flow_executor import FlowExecutor
from ..online.online_executor import OnlineLocalExecutor


class LocalFlowExecutor(FlowExecutor):
    def __init__(self, resources):
        super(LocalFlowExecutor, self).__init__(resources)
        self.online_executor = OnlineLocalExecutor(self._resources)

    async def execute_up(self):
        print(self._resources)
        print('local flow up')
        self.online_executor.execute_up()

    async def execute_down(self):
        print(self._resources)
        print('local flow down')
        self.online_executor.execute_down()

    async def execute_status(self):
        print(self._resources)
        print('local flow status')
        self.online_executor.execute_status()

    async def execute_reload(self):
        print(self._resources)
        print('local flow reload')
        self.online_executor.execute_reload()
