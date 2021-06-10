# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

import pandas
from pysqa.wrapper.slurm import SlurmCommands


__author__ = "Sander Borgmans"
__copyright__ = (
    "Copyright 2019, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "1.0"
__maintainer__ = "Sander Borgmans"
__email__ = "sander.borgmans@ugent.be"
__status__ = "development"
__date__ = "Jun 10, 2021"


class BreniacCommands(TorqueCommands):
    def __init__(self):
        super(BreniacCommands, self).__init__()

    @property
    def submit_job_command(self):
        return ["qsub"]

    @property
    def get_queue_status_command(self):
        return ["qstat"]

    @staticmethod
    def get_job_id_from_output(queue_submit_output):
          return int(queue_submit_output.splitlines()[-1].rstrip().lstrip().split('.')[0])

    @staticmethod
    def convert_queue_status(queue_status_output):
        qstat = queue_status_output.splitlines()
        if len(qstat) <= 2: # first row contains cluster name, check if there are jobs
            return pandas.DataFrame(columns=['jobid','user','jobname','status'])
        line_split_lst = [line.split() for line in qstat[2:]]
        job_id_lst, job_name_lst, user_lst, status_lst = zip(*[(int(jobid.split('.')[0]), jobname, user, status.lower())
                                                               for jobid, jobname, user, _, status, _  in line_split_lst])
        return pandas.DataFrame({'jobid': job_id_lst,
                                 'user': user_lst,
                                 'jobname': job_name_lst,
                                 'status': status_lst})
