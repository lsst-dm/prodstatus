#!/usr/bin/env python
# This file is part of prodstatus package.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import sys
import os
import json
import urllib.error as url_error
from appdirs import user_data_dir
from urllib.request import urlopen
from pathlib import Path
from time import sleep
import datetime
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import pandas as pd
import numpy as np
import http.client
from lsst.prodstatus import LOG


__all__ = ['MakePandaPlots']


class MakePandaPlots:
    """Build production statistics tables using PanDa database queries.

    Parameters
    ----------
    Jira : `str`
        Jira ticket identifying production campaign used
        to select campaign workflows
    collType : `str`
        token that with jira ticket will uniquely define the dataset (workflow)
    bin_width : `str`
        plot bin width in sec.
    start_at : `float`
        time in hours at which to start plot
    stop_at : `float`
        time in hours at which to stop plot
    startTime : `str`
        time to start selecting workflows from in Y-m-d format
    stopTime : `str`
        time to stop selecting workflows in Y-m-d format
    """

    def __init__(self, **kwargs):
        self.collection_type = kwargs["collType"]
        self.jira_ticket = kwargs["Jira"]
        " bin width in seconds "
        self.bin_width = kwargs["bin_width"]
        " bin width in hours "
        self.scale_factor = float(self.bin_width) / 3600.0
        self.stop_at = int(kwargs["stop_at"])
        self.start_at = float(kwargs["start_at"])
        self.start_date = kwargs["start_date"]
        self.stop_date = kwargs["stop_date"]
        self.plot_n_bins = int((self.stop_at - self.start_at)
                               / self.scale_factor)
        self.start_time = 0
        self.work_keys = list()
        self.workflows = dict()
        self.workflow_info = dict()  # workflow status
        self.task_counts = dict()  # number of tasks of given type
        self.all_tasks = dict()  # info about task
        self.all_jobs = dict()  # info about jobs
        self.workflow_tasks = dict()  # tasks per workflow
        self.job_names = kwargs["job_names"]
        self.workflow_names = dict()
        app_name = "ProdStat"
        app_author = os.environ.get('USERNAME')
        data_dir = user_data_dir(app_name, app_author)
        self.data_path = Path(data_dir)
        self.start_stamp = datetime.datetime.strptime(self.start_date, "%Y-%m-%d").timestamp()
        self.stop_stamp = datetime.datetime.strptime(self.stop_date, "%Y-%m-%d").timestamp()
        self.log = LOG
        self.log.info(f" Collecting information for Jira ticket {self.jira_ticket}")
        LOG.info(f"Will store results in {self.data_path.absolute()}")
        if not os.path.exists(self.data_path):
            self.data_path.mkdir()
        self.max_time_slice = dict()
        self.old_jobs = defaultdict(list)
        http.client._MAXLINE = 655360

    def get_workflows(self):
        """First lets get all workflows with given keys.
        """

        workflow_data = self.query_panda(
            # url_string="http://panda-doma.cern.ch/idds/wfprogress_gcp/?json"
            # TBD: need to allow IDDS URL be a variable
            url_string="http://panda-doma.cern.ch/idds/wfprogress/?json"
        )
        comp = str(self.jira_ticket).lower()
        comp1 = str(self.collection_type)
        nwf = 0
        for wf in workflow_data:
            r_name = wf["r_name"]
            if comp in r_name and comp1 in r_name:
                key = str(r_name).split("_")[-1]
                date_str = key.split('t')[0]
                date_stamp = datetime.datetime.strptime(date_str, "%Y%m%d").timestamp()
                if self.start_stamp <= date_stamp <= self.stop_stamp:
                    self.work_keys.append(str(key))
                    nwf += 1
        self.log.info(f"number of workflows ={nwf}")
        if nwf == 0:
            self.log.warning("No workflows to work with -- exiting")
            sys.exit(-256)
        for key in self.work_keys:
            self.workflows[key] = list()
        for wfk in self.work_keys:
            for wf in workflow_data:
                r_name = wf["r_name"]
                if wfk in r_name:
                    self.workflows[wfk].append(wf)
        #
        self.log.info(f"Selected workflows:{self.workflows}")
        create_time = list()
        for key in self.work_keys:
            workflow = self.workflows[key]
            for wf in workflow:
                created = datetime.datetime.strptime(
                    wf["created_at"].split('.')[0], "%Y-%m-%d %H:%M:%S"
                ).timestamp()
                r_status = wf["r_status"]
                total_tasks = wf["total_tasks"]
                total_files = wf["total_files"]
                remaining_files = wf["remaining_files"]
                processed_files = wf["processed_files"]
                task_statuses = wf["tasks_statuses"]
                create_time.append(created)
                self.log.info(
                    f"created{created} total tasks {total_tasks}"
                    f" total files {total_files}"
                )
                if "Finished" in task_statuses:
                    finished = task_statuses["Finished"]
                else:
                    finished = 0
                if "SubFinished" in task_statuses:
                    sub_finished = task_statuses["SubFinished"]
                else:
                    sub_finished = 0
                if "Failed" in task_statuses:
                    failed = task_statuses["Failed"]
                else:
                    failed = 0
                if key not in self.workflow_info:
                    self.workflow_info[key] = {
                        "status": r_status,
                        "ntasks": float(total_tasks),
                        "nfiles": float(total_files),
                        "remaining files": float(remaining_files),
                        "processed files": float(processed_files),
                        "task_finished": float(finished),
                        "task_failed": float(failed),
                        "task_subfinished": float(sub_finished),
                        "created": created,
                    }
        self.start_time = min(create_time)
        " Now check if we have previous data and recover old start_time"
        for job_name in self.job_names:
            " first read saved data if any, and find last delta_time"
            data_file = self.data_path.joinpath(f"panda_time_series_{job_name}.csv")

            if os.path.exists(data_file):
                df = pd.read_csv(data_file, header=0, index_col=0,
                                 parse_dates=True, squeeze=True)
                self.max_time_slice[job_name] = df['delta_time'].iloc[-1]
                self.start_time = df['start_time'].iloc[0]
                LOG.info(f"Start time is {self.start_time}")
                _records = df.to_records(index=False)
                self.old_jobs[job_name] = list(_records)
            else:
                self.max_time_slice[job_name] = 0.
                self.old_jobs[job_name] = list()
        self.log.info(f"all started at {self.start_time}")

    def get_wf_tasks(self, workflow):
        """Select tasks for given workflow (jobs).

        Parameters
        ----------
        workflow: `Union`
            workflow for which tasks will be selected

        Returns
        -------
        tasks: `list`
            list of tasks in given workflow
        """
        url_string = str(workflow["r_name"])
        tasks = self.query_panda(
            url_string=(f"http://panda-doma.cern.ch/tasks/?"
                        f"taskname={url_string}*&days=120&json"
                        )
        )
        return tasks

    def get_task_info(self, task):
        """Extract data we need from task dictionary.

        Parameters
        ----------
        task : `dic`
            dictionary with task information
        """

        jeditaskid = task["jeditaskid"]
        """ Now select jobs to get timing information """
        uri = f"http://panda-doma.cern.ch/jobs/?jeditaskid={str(jeditaskid)}&json"
        jobs_data = self.query_panda(url_string=uri)
        """ list of jobs in the task """
        jobs = jobs_data["jobs"]
        n_jobs = len(jobs)
        if n_jobs > 0:
            for jb in jobs:
                job_name = jb["jobname"]
                if isinstance(jb["durationsec"], type(None)):
                    duration_sec = 0.0
                else:
                    duration_sec = float(jb["durationsec"])
                if isinstance(jb["starttime"], str):
                    tokens = jb["starttime"].split("T")
                    start_string = (
                        tokens[0] + " " + tokens[1]
                    )  # get rid of T in the date string
                    task_start = datetime.datetime.strptime(
                        start_string, "%Y-%m-%d %H:%M:%S"
                    ).timestamp()
                    delta_time = task_start - self.start_time
                else:
                    delta_time = -1.0
                for _name in self.job_names:
                    if _name in job_name:
                        if _name in self.all_jobs:
                            self.all_jobs[_name].append((delta_time,
                                                        duration_sec, self.start_time))
                        else:
                            self.all_jobs[_name] = list()
                            self.all_jobs[_name].append((delta_time,
                                                        duration_sec, self.start_time))
        else:
            return
        return

    def get_task_data(self, tasks):
        """Given list of jobs get statistics for each job type.

        Parameters
        ----------
        tasks : `list`
            list of tasks
        """
        task_ids = dict()
        """Let's sort tasks with jeditaskid """
        i = 0
        for task in tasks:
            _id = task["jeditaskid"]
            task_ids[_id] = i
            i += 1
        " select tasks for requested plots only"
        for _id in sorted(task_ids):
            t_ind = task_ids[_id]
            task = tasks[t_ind]
            task_name = task["taskname"]
            for _name in self.job_names:
                if _name in task_name:
                    self.get_task_info(task)
        return

    def get_tasks(self):
        """Select all workflow tasks.
        """
        for key in self.work_keys:
            self.workflow_tasks[key] = list()
            _workflows = self.workflows[key]
            for wf in _workflows:
                """get tasks for this workflow"""
                tasks = self.get_wf_tasks(wf)
                """get data for each task """
                self.get_task_data(tasks)

    @staticmethod
    def query_panda(url_string):
        """Read url with panda information.

        Parameters
        ----------
        url_string : `str`
            URL string to read

        Returns
        -------
        result : `dict`
            dictionary with panda data
        """
        success = False
        n_tries = 0
        result = dict()
        while not success:
            try:
                with urlopen(url_string) as url:
                    result = json.loads(url.read().decode())
                success = True
            except url_error.URLError:
                LOG.info(f"failed with {url_string} retrying")
                LOG.info(f"ntries={n_tries}")
                success = False
                n_tries += 1
                if n_tries >= 5:
                    break
                sleep(2)
        sys.stdout.write(".")
        sys.stdout.flush()
        return result

    def make_plot(self, data_list, max_time, job_name, figure_number):
        """Plot timing data in png file.

        Parameters
        ----------
        data_list : `list`
            list of tuples (start_time, duration) for given job
        max_time : `float`
            maximal time in the timing data list
        job_name : `str`
            name of the job to be used in the name of plot file
        figure_number : 'int' figure number
        """
        colors_list = list(mcolors.TABLEAU_COLORS)
        number_of_colors = len(colors_list)
        first_bin = int(self.start_at / self.scale_factor)
        last_bin = first_bin + self.plot_n_bins
        n_bins = int(max_time / float(self.bin_width))
        task_count = np.zeros(n_bins)
        for time_in, duration in data_list:
            task_count[int(time_in / float(self.bin_width)): int(
                (time_in + duration) / float(self.bin_width))] += 1
        if self.plot_n_bins > n_bins:
            last_bin = n_bins
        sub_task_count = np.copy(task_count[first_bin:last_bin])
        if len(sub_task_count) > 0:
            max_y = 1.2 * (max(sub_task_count) + 1.0)
            sub_task_count.resize([self.plot_n_bins])
            x_bins = np.arange(self.plot_n_bins) * self.scale_factor + self.start_at
            plt.figure(figure_number)
            _color_index = int(figure_number) - (int(figure_number) // number_of_colors) * number_of_colors
            plt.plot(x_bins, sub_task_count, label=str(job_name), color=colors_list[int(_color_index)])
            plt.axis([self.start_at, self.stop_at, 0, max_y])
            ts = int(float(self.start_time))
            time_stamp = datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
            plt.xlabel(f"Hours since first quantum start at {time_stamp}")
            self.log.info(f"Hours since first quantum start at {time_stamp}")
            plt.ylabel("Number of running quanta")
            plt.title(job_name)
            plt.legend()
            plt.savefig(self.data_path.joinpath(f"timing_{job_name}.png"))

    def prep_data(self):
        """Create file with timing data."""
        self.get_workflows()
        self.get_tasks()
        for key in self.all_jobs:
            self.all_jobs[key].sort()
        " prepare new dictionary with data extension "
        for job_name in self.all_jobs:
            for d_time, duration, start_time in self.all_jobs[job_name]:
                if d_time > self.max_time_slice[job_name]:
                    if job_name in self.old_jobs:
                        self.old_jobs[job_name].append((d_time,
                                                        duration, self.start_time))
                    else:
                        self.old_jobs[job_name] = list()
                        self.old_jobs[job_name].append((d_time,
                                                        duration, self.start_time))
        "Now we have updated time slices - let's save them"
        for job_name in self.old_jobs:
            dataframe = pd.DataFrame.from_records(self.old_jobs[job_name],
                                                  columns=["delta_time", "durationsec", "start_time"])
            data_file = self.data_path.joinpath(f"panda_time_series_{job_name}.csv")
            self.log.info(f"writing data frame {data_file}")
            dataframe.to_csv(data_file)

    def plot_data(self):
        """Create plot of timing data in form of png file."""
        figure_number = 0
        for job_name in self.job_names:
            data_file = self.data_path.joinpath(f"panda_time_series_{job_name}.csv")
            self.log.info(f"read data file {data_file}")
            if os.path.exists(data_file):
                df = pd.read_csv(
                    data_file, header=0, index_col=0, parse_dates=True,
                    squeeze=True)
                data_list = list()
                max_time = 0.0
                for index, row in df.iterrows():
                    if float(row[0]) >= max_time:
                        max_time = row[0]
                    data_list.append((row[0], row[1]))
                self.log.info(f" job name {job_name}")
                self.make_plot(data_list, max_time, job_name, figure_number)
                figure_number += 1
