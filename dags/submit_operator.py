# This code is the source code for bash operator,
# and, it is copied directly from the airflow's website
# but a small change is made to generate the correct command I need.
# I need to add the execution date to the end of bash_command
# This only works on the scole of this project

import os
import signal
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile

from builtins import bytes

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.operator_helpers import context_to_airflow_vars


class SubmitOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BashOperator`

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed. (templated)
    :type bash_command: str
    :param xcom_push: If xcom_push is True, the last line written to stdout
        will also be pushed to an XCom when the bash command completes.
    :type xcom_push: bool
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :param output_encoding: Output encoding of bash command
    :type output_encoding: str
    """
    template_fields = ('bash_command', 'env')

    template_ext = ('.sh', '.bash',)

    ui_color = '#f0ede4'


    @apply_defaults
    def __init__(
            self,
            bash_command,
            xcom_push=False,
            env=None,
            output_encoding='utf-8',
            *args, **kwargs):

        super(SubmitOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        self.log.info("Tmp dir root location: \n %s", gettempdir())

        # Prepare env for child process.
        env = self.env
        if env is None:
            env = os.environ.copy()
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.info('Exporting the following env vars:\n%s',
                      '\n'.join(["{}={}".format(k, v)
                                 for k, v in
                                 airflow_context_vars.items()]))
        env.update(airflow_context_vars)

        # The following three lines are what I added
        exe_date = context.get("execution_date")
        sub_command = "{}/{}/{}/".format(exe_date.year,
                                          exe_date.month,
                                          exe_date.day)
        self.bash_command = self.bash_command.format(sub_command)
        self.lineage_data = self.bash_command
        self.log.info("Processing Command: " + self.lineage_data)

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(self.bash_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = os.path.abspath(fname)
                self.log.info(
                    "Temporary script location: %s",
                    script_location
                )

                def pre_exec():
                    # Restore default signal disposition and invoke setsid
                    for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                        if hasattr(signal, sig):
                            signal.signal(getattr(signal, sig), signal.SIG_DFL)
                    os.setsid()

                self.log.info("Running command: %s", self.bash_command)
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=env,
                    preexec_fn=pre_exec)

                self.sp = sp

                self.log.info("Output:")
                line = ''
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).rstrip()
                    self.log.info(line)
                sp.wait()
                self.log.info(
                    "Command exited with return code %s",
                    sp.returncode
                )

                if sp.returncode:
                    raise AirflowException("Bash command failed")

        if self.xcom_push_flag:
            return line


def on_kill(self):
        self.log.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
