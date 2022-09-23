from scheduler.scheduler import Scheduler
from utils.file_util import FileUtil
import os
import subprocess


class CrontabScheduler(Scheduler):
    def __init__(self, schedulers_conf, tasks, customer_conf_path):
        super().__init__(schedulers_conf, tasks, customer_conf_path)
        self._local_temp_dir = ".tmp"
        self.is_local_mode = True
        self._container_name = 'metaspore_offline'
        self._is_offline_container_running = True

    def publish(self):

        if not self.is_local_mode:
            return

        if not self._is_offline_container_running:
            return

        self._write_local_tmp_dir()

        self._copy_tmp_to_docker_container()

        self._publish_docker_crontab()

        self._exec_docker_crontab_script()

    def _generate_cmd(self):
        cmd = map(lambda x: x.execute + " ${SCHEDULER_TIME}", self._dag_tasks)
        cmd = " \n".join(cmd)
        return cmd

    @property
    def _local_crontab_script_file(self):
        return self._local_temp_dir + "/" + self.name + ".sh"

    @property
    def _docker_tmp_dir(self):
        return self._customer_conf_path + "/" + self._local_temp_dir

    @property
    def _docker_crontab_script_file(self):
        return self._docker_tmp_dir + "/" + self.name + ".sh"

    def _write_local_tmp_dir(self):
        self._write_customer_params_files()
        self._write_crontab_script()

    def _write_customer_params_files(self):
        for task in self._dag_tasks:
            customer_params_yaml_file = self._local_temp_dir + "/" + task.name + ".yml"
            FileUtil.write_dict_to_yaml_file(
                customer_params_yaml_file, task.customer_params)

    def _write_crontab_script(self):
        content = self._generate_crontab_script_content()
        FileUtil.write_file(self._local_crontab_script_file, content)

    def _generate_crontab_script_content(self):
        script_header = "#!/bin/bash" + "\n"
        scheduler_time = 'SCHEDULER_TIME="`date --iso-8601=seconds`"' + "\n"
        cmd = self._generate_cmd()
        script_content = script_header + \
            scheduler_time + \
            cmd
        return script_content

    def _copy_tmp_to_docker_container(self):
        src = self._local_temp_dir + "/."
        dst = "%s:%s/" % (self._container_name,
                          self._docker_tmp_dir)
        overwrite_docker_tmp_dir = "rm -rf %s && mkdir -p %s " % (
            self._docker_tmp_dir, self._docker_tmp_dir)

        overwrite_docker_tmp_dir_cmd = ['docker', 'exec', '-i', self._container_name,
                                        '/bin/bash', '-c', overwrite_docker_tmp_dir]
        copy_tmp_to_docker_cmd = ['docker', 'cp', src, dst]

        subprocess.run(overwrite_docker_tmp_dir_cmd)
        subprocess.run(copy_tmp_to_docker_cmd)

    def _publish_docker_crontab(self):
        # 'crontab -l | { cat; echo "* * * * * echo hello >> /tmp/hello.txt"; } | crontab -'

        publish_crontab_msg = "crontab -l | { cat; echo \"%s sh %s >> /tmp/%s.log\"; } | crontab -" % (
            self.cronExpr,
            self._docker_crontab_script_file,
            self.name)

        publish_docker_crontab_cmd = ['docker', 'exec', '-i', self._container_name,
                                      '/bin/bash', '-c', publish_crontab_msg]

        subprocess.run(publish_docker_crontab_cmd)

    def _exec_docker_crontab_script(self):
        exec_docker_crontab_script_msg = "sh %s > /tmp/%s.log" % (
            self._docker_crontab_script_file, self.name)
        print("trigger scheduler: %s once immediately" % self.name)
        print(exec_docker_crontab_script_msg)
        exec_docker_crontab_script_cmd = ['docker', 'exec', '-i', self._container_name,
                                          '/bin/bash', '-c', exec_docker_crontab_script_msg]
        subprocess.run(exec_docker_crontab_script_cmd)
