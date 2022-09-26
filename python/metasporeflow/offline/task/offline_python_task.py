from task.task import Task


class PySparkTask(Task):

    def __init__(self,
                 name,
                 type,
                 meta,
                 customer_params,
                 customer_conf_path
                 ):
        super().__init__(name,
                         type,
                         meta,
                         customer_params,
                         customer_conf_path
                         )

    def _execute(self):
        return "python %s %s" % (self._script_path, self.customer_params_yaml_file)
