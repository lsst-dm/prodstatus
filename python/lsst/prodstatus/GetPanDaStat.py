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
import json
import re
import urllib.error as url_error
from urllib.request import urlopen
import time
from time import sleep, gmtime, strftime
import datetime
import math
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import table
from lsst.prodstatus import LOG

__all__ = ['GetPanDaStat']


class GetPanDaStat:
    """Build production statistics tables using PanDa database queries.

    Parameters
    ----------
    Jira : `str`
        Jira ticket identifying production campaign used
        to select campaign workflows
    CollType : `str`
        token that with jira ticket will uniquely define campaign workflows
    startTime : `str`
        time to start selecting workflows from in Y-m-d format
    stopTime : `str`
        time to stop selecting workflows in Y-m-d
    maxtask : `int`
        maximum number of task files to analyse
    """

    def __init__(self, **kwargs):

        self.collection_type = kwargs["collType"]
        self.Jira = kwargs["Jira"]
        self.start_date = kwargs["start_date"]
        self.stop_date = kwargs["stop_date"]
        self.max_tasks = int(kwargs["maxtask"])
        self.workflow_keys = list()
        self.workflows = dict()
        self.workflow_info = dict()  # workflow status
        self.task_counts = dict()  # number of tasks of given type
        self.all_tasks = dict()  # info about tasks
        self.all_jobs = dict()  # info about jobs
        self.workflow_tasks = dict()  # tasks per workflow
        self.task_stat = dict()
        self.all_stat = dict()  # general statistics
        self.workflow_names = dict()
        self.start_stamp = datetime.datetime.strptime(self.start_date, "%Y-%m-%d").timestamp()
        self.stop_stamp = datetime.datetime.strptime(self.stop_date, "%Y-%m-%d").timestamp()
        self.log = LOG
        self.log.info(" Collecting information for Jira ticket ", self.Jira)

    def get_workflows(self):
        """First lets get all workflows with given keys."""

        workflow_data = self.query_panda(
            url_string="http://panda-doma.cern.ch/idds/wfprogress/?json"
        )
        comp = str(self.Jira).lower()
        comp1 = str(self.collection_type)
        nwf = 0
        for wf in workflow_data:
            r_name = wf["r_name"]
            if comp in r_name and comp1 in r_name:
                key = str(r_name).split("_")[-1]
                date_str = key.split('t')[0]
                date_stamp = datetime.datetime.strptime(date_str, "%Y%m%d").timestamp()
                self.log.info(f"key={key} date_str={date_str} date_stamp={date_stamp}")
                if self.start_stamp <= date_stamp <= self.stop_stamp:
                    self.workflow_keys.append(str(key))
                    nwf += 1
        self.log.info(f"number of workflows ={nwf}")
        if nwf == 0:
            self.log.warning(f"No workflows to work with -- exiting")
            sys.exit(-256)
        for key in self.workflow_keys:
            self.workflows[key] = []
        for wfk in self.workflow_keys:
            for wf in workflow_data:
                r_name = wf["r_name"]
                if wfk in r_name:
                    self.workflows[wfk].append(wf)
        #
        self.log.info(f"Selected workflows: {self.workflows}")
        for key in self.workflow_keys:
            workflow = self.workflows[key]
            for wf in workflow:
                created = datetime.datetime.strptime(
                    wf["created_at"], "%Y-%m-%d %H:%M:%S"
                ).timestamp()
                r_status = wf["r_status"]
                total_tasks = wf["total_tasks"]
                total_files = wf["total_files"]
                remaining_files = wf["remaining_files"]
                processed_files = wf["processed_files"]
                task_statuses = wf["tasks_statuses"]
                if "Finished" in task_statuses.keys():
                    finished = task_statuses["Finished"]
                else:
                    finished = 0
                if "SubFinished" in task_statuses.keys():
                    subfinished = task_statuses["SubFinished"]
                else:
                    subfinished = 0
                if "Failed" in task_statuses.keys():
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
                        "task_subfinished": float(subfinished),
                        "created": created,
                    }

    def get_workflow_tasks(self, workflow):
        """Select tasks for given workflow (jobs).

        Parameters
        ----------
        workflow : `Union`
            workflow name

        Returns
        -------
        tasks : `list`
            list of tasks in given workflow
        """

        urls = workflow["r_name"]
        tasks = self.query_panda(
            url_string=f"http://panda-doma.cern.ch/tasks/?taskname={urls}*&days=120&json"
        )
        return tasks

    def get_task_info(self, task):
        """Extract data we need from task dictionary.

        Parameters
        ----------
        task : `dict`
            dictionary of task parameters

        Returns
        -------
        data : `dict`
            subset of the task data
        """

        data = dict()
        jeditaskid = task["jeditaskid"]

        # Now select a number of jobs to calculate average cpu time and max Rss
        uri = (
            f"http://panda-doma.cern.ch/jobs/?jeditaskid={str(jeditaskid)}"
            f"&limit={str(self.max_tasks)}&jobstatus=finished&json"
        )
        jobsdata = self.query_panda(url_string=uri)
        """ list of jobs in the task """
        jobs = jobsdata["jobs"]
        njobs = len(jobs)
        corecount = 0
        max_rss = 0
        duration = 0
        attempts = 0
        start_time = float(round(time.time()))
        if njobs > 0:
            for jb in jobs:
                corecount += jb["actualcorecount"]
                duration += jb["durationsec"]
                attempts += jb["attemptnr"]
                tokens = jb["starttime"].split("T")
                start_stamp = tokens[0] + " " + tokens[1]  # get rid of T in the date string
                task_start = datetime.datetime.strptime(
                    start_stamp, "%Y-%m-%d %H:%M:%S"
                ).timestamp()
                if start_time >= task_start:
                    start_time = task_start
                if max_rss <= jb["minramcount"]:
                    max_rss = jb["minramcount"]
            corecount = float(corecount / njobs)
            duration = float(duration / njobs)
            attempt_number = float(attempts / njobs)
        else:
            return data
        """select first good job """
        jb = jobs[0]
        for jb in jobs:
            if jb["jobstatus"] != "failed":
                break
        """Fill data with the firs good job """
        ds_info = task["dsinfo"]
        data["jeditaskid"] = task["jeditaskid"]
        data["jobname"] = jb["jobname"]
        data["taskname"] = task["taskname"]
        data["status"] = task["status"]
        data["attemptnr"] = int(attempt_number)
        data["actualcorecount"] = jb["actualcorecount"]
        data["starttime"] = str(task["starttime"]).upper()
        if task["endtime"] is None:
            _now = datetime.datetime.now()
            task["endtime"] = _now.strftime("%Y-%m-%dT%H:%M:%S")
        _end_time = str(task["endtime"]).upper()
        tokens = _end_time.split("T")
        data["endtime"] = tokens[0] + " " + tokens[1]  # get rid of T in the date string
        if task["starttime"] is None:
            task["starttime"] = tokens[0] + " " + tokens[1]
        data["starttime"] = task["starttime"]
        data["maxattempt"] = jb["maxattempt"]
        data["basewalltime"] = task["basewalltime"]
        data["cpuefficiency"] = task["cpuefficiency"]
        data["maxdiskcount"] = jb["maxdiskcount"]
        data["maxdiskunit"] = jb["maxdiskunit"]
        data["cpuconsumptiontime"] = duration
        data["jobstatus"] = jb["jobstatus"]
        tokens = jb["starttime"].split("T")
        data["jobstarttime"] = (
                tokens[0] + " " + tokens[1]
        )  # get rid of T in the date string
        tokens = jb["endtime"].split("T")
        data["jobendtime"] = (
                tokens[0] + " " + tokens[1]
        )  # get rid of T in the date string
        task_start = datetime.datetime.strptime(
            data["starttime"], "%Y-%m-%d %H:%M:%S"
        ).timestamp()

        job_start = start_time
        task_end = datetime.datetime.strptime(
            data["endtime"], "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        job_duration = task_end - job_start
        data["ncpus"] = corecount
        data["taskduration"] = job_duration
        data["exeerrorcode"] = jb["exeerrorcode"]
        data["nfiles"] = ds_info["nfiles"]
        data["Rss"] = max_rss
        return data

    def get_task_data(self, key, tasks):
        """Given list of jobs get statistics for each job type.

        Parameters
        ----------
        key : `str`
            timestamp part of the workflow name
        tasks : `list`
            list of tasks data dictionaries

        Returns
        -------
        tasktypes : `dict`
            dictionary of task types with list of tasks
        """

        task_data = list()
        task_names = list()
        task_types = dict()
        task_ids = dict()
        """Let's sort tasks with jeditaskid """
        i = 0
        for task in tasks:
            _id = task["jeditaskid"]
            task_ids[_id] = i
            i += 1
        for _id in sorted(task_ids):
            t_ind = task_ids[_id]
            task = tasks[t_ind]
            comp = key.upper()
            task_name = task["taskname"].split(comp)[1]
            tokens = task_name.split("_")
            name = ""
            for i in range(1, len(tokens) - 1):
                name += tokens[i] + "_"
            task_name = name[:-1]
            data = self.get_task_info(task)
            if len(data) == 0:
                self.log.info(f"No data for {task_name}")
                continue
            job_name = data["jobname"].split("Task")[0]
            task_name = data["taskname"]
            comp = key.upper()
            task_name = task_name.split(comp)[1]
            tokens = task_name.split("_")
            name = ""
            for i in range(1, len(tokens) - 1):
                name += tokens[i] + "_"
            task_name = name[:-1]
            task_name = str(key) + "_" + task_name
            data["taskname"] = task_name
            data["jobname"] = job_name
            if job_name not in self.all_jobs:
                self.all_jobs[job_name] = []
            if task_name not in self.all_jobs[job_name]:
                self.all_jobs[job_name].append(task_name)
            data["walltime"] = data["taskduration"]
            task_data.append(data)
        """Now create a list of task types"""
        for data in task_data:
            name = data["taskname"]
            if name not in self.task_counts:
                self.task_counts[name] = 0
                self.all_tasks[name] = list()
                task_names.append(name)
        """Now create a list of tasks for each task type """
        for task_name in task_names:
            task_list = list()
            for task in task_data:
                if task_name == task["taskname"]:
                    task_list.append(task)
                    if task_name in self.all_tasks:
                        self.all_tasks[task_name].append(task)
                        self.task_counts[task_name] += 1
            task_types[task_name] = task_list
        return task_types

    def get_tasks(self):
        """Select finished and sub finished workflow tasks."""

        for key in self.workflow_keys:
            self.workflow_tasks[key] = list()
            _workflows = self.workflows[key]
            for wf in _workflows:
                if (
                        str(wf["r_status"]) == "finished"
                        or str(wf["r_status"]) == "subfinished"
                        or str(wf["r_status"]) == "running"
                        or str(wf["r_status"]) == "transforming"
                        or str(wf["r_status"]) == "cancelling"
                ):
                    """get tasks for this workflow"""
                    tasks = self.get_workflow_tasks(wf)
                    """get data for each task """
                    task_types = self.get_task_data(key, tasks)
                    self.workflow_tasks[key].append(task_types)

    @staticmethod
    def query_panda(url_string):
        """Read given URL to get panda data

        Parameters
        ----------
        url_string : `str`
            URL string to get data from

        Returns
        -------
        result : `dict`
            dictionary of panda data from given URL
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
                logging.warning(f"failed with {url_string} retrying")
                logging.warning(f"n_tries={n_tries}")
                success = False
                n_tries += 1
                if n_tries >= 5:
                    break
                sleep(2)
        sys.stdout.write(".")
        sys.stdout.flush()
        return result

    def get_all_stat(self):
        """Calculate campaign statistics."""

        workflow_wall_time = 0
        workflow_disk = 0
        workflow_cores = 0
        workflow_rss = 0
        workflow_duration = 0.0
        workflow_n_files = 0
        workflow_parallel_jobs = 0
        self.all_stat = dict()
        for task_type in self.all_tasks:
            self.all_stat[task_type] = dict()
            tasks = self.all_tasks[task_type]
            cpu_consumption = 0
            wall_time = 0
            cpu_efficiency = 0
            core_count = 0
            max_disk_count = 0
            duration = 0.0
            task_duration = 0.0
            start_time = ''
            n_files = 0
            max_rss = 0
            attempts = 0.0
            n_tasks = len(tasks)
            for i in range(n_tasks):
                wall_time += int(tasks[i]["walltime"])
                cpu_consumption += int(tasks[i]["cpuconsumptiontime"])
                cpu_efficiency += int(tasks[i]["cpuefficiency"])
                max_disk_count += int(tasks[i]["maxdiskcount"])
                duration += float(tasks[i]["cpuconsumptiontime"])
                attempts += tasks[i]["attemptnr"]
                start_time = tasks[i]["starttime"]
                task_duration += tasks[i]["taskduration"]
                core_count += int(tasks[i]["actualcorecount"])
                n_files = int(tasks[i]["nfiles"])
                rss = tasks[i]["Rss"]
                if max_rss <= rss:
                    max_rss = rss
            task_duration = task_duration / n_tasks
            wall_time_per_job = cpu_consumption / n_tasks
            wall_time = wall_time_per_job * n_files
            max_disk_per_job = max_disk_count / n_tasks
            disk_count = max_disk_per_job * n_files
            core_count_per_job = core_count / n_tasks
            core_count = core_count_per_job * n_files
            if task_duration <= 0.:
                n_parallel = 1
            else:
                n_parallel = int(math.ceil(wall_time / task_duration))
            if n_parallel < 1:
                n_parallel = 1
            workflow_duration += task_duration
            workflow_wall_time += wall_time
            workflow_disk += disk_count
            workflow_cores += n_parallel
            workflow_n_files += n_files
            if workflow_rss <= max_rss:
                workflow_rss = max_rss
            self.all_stat[task_type] = {
                "nQuanta": float(n_files),
                "starttime": start_time,
                "wallclock": str(datetime.timedelta(seconds=task_duration)),
                "cpu sec/job": float(wall_time_per_job),
                "cpu-hours": str(datetime.timedelta(seconds=wall_time)),
                "est. parallel jobs": n_parallel,
            }

        if workflow_duration > 0:
            workflow_parallel_jobs = int(math.ceil(workflow_wall_time / workflow_duration))
        else:
            workflow_parallel_jobs = 0
        self.all_stat["Campaign"] = {
            "nQuanta": float(workflow_n_files),
            "starttime": strftime("%Y-%m-%d %H:%M:%S", gmtime()),
            "wallclock": str(datetime.timedelta(seconds=workflow_duration)),
            "cpu sec/job": "-",
            "cpu-hours": str(datetime.timedelta(seconds=workflow_wall_time)),
            "est. parallel jobs": workflow_parallel_jobs,
        }

    @staticmethod
    def highlight_status(value):
        """Create background color for HTML table.

        Parameters
        ----------
        value : `str`
            status of the job

        Returns
        -------
        backgroupd_colors : `list`
            background color
        """
        if str(value) == "failed":
            return ["background-color: read"] * 9
        elif str(value) == "subfinisher":
            return ["background-color: yellow"] * 9
        else:
            return ["background-color: green"] * 9

    @staticmethod
    def highlight_greaterthan_0(s):
        """Create background color for HTML table

        Parameters
        ----------
        s : `class`
            task status flag

        Returns
        -------
        background_colors : `list`
            background color
        """
        if s.task_failed > 0.0:
            return ["background-color: red"] * 9
        elif s.task_subfinished > 0.0:
            return ["background-color: yellow"] * 9
        else:
            return ["background-color: white"] * 9

    def make_table_from_csv(self, buffer, out_file, index_name, comment):
        """Create Jira table from csv file.

        Parameters
        ----------
        buffer : `str`
            comma separated data buffer
        out_file : `str`
            output file name
        index_name : `str`
            list of index names to name table rows
        comment : `str`
            additional string to be added at top of the table
        """
        newbody = comment + "\n"
        newbody += out_file + "\n"
        lines = buffer.split("\n")
        comma_matcher = re.compile(r",(?=(?:[^\"']*[\"'][^\"']*[\"'])*[^\"']*$)")
        i = 0
        for line in lines:
            if i == 0:
                tokens = line.split(",")
                line = "|" + index_name
                for ln in range(1, len(tokens)):
                    line += "||" + tokens[ln]
                line += "||\r\n"
            elif i >= 1:
                tokens = comma_matcher.split(line)
                line = "|"
                for token in tokens:
                    line += token + "|"
                line = line[:-1]
                line += "|\r\n"
            newbody += line
            i += 1
        new_body = newbody[:-2]
        with open(f"/tmp/{out_file}-{self.Jira}.txt", "w") as tb_file:
            print(new_body, file=tb_file)
        return newbody

    def make_styled_table(self, dataframe, outfile):
        """Create styled HTML table.

        Parameters
        ----------
        dataframe : `pandas.DataFrame`
            pandas data frame containing table data
        outfile : `str`
            output file name
        :return:
        """
        df_styled = dataframe.style.apply(self.highlight_greaterthan_0, axis=1)
        df_styled.set_table_attributes('border="1"')
        df_html = df_styled.render()
        htfile = open(f"/tmp/{outfile}-{self.Jira}.html", "w")
        print(df_html, file=htfile)
        htfile.close()

    def make_table(self, data_frame, table_name, index_name, comment):
        """Create several types of tables from pandas data frame

        Parameters
        ----------
        data_frame : `pandas.DataFrame`
            pandas data frame
        table_name : `str`
            name of the output table
        index_name : `str`
            list of raw names
        comment : `str`
            additional text information to put at top of the table
        """
        fig, ax = plt.subplots(figsize=(20, 35))  # set size frame
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        ax.set_frame_on(False)  # no visible frame, uncomment if size is ok
        tabula = table(ax, data_frame, loc="upper right")
        tabula.auto_set_font_size(False)  # Activate set fontsize manually
        tabula.auto_set_column_width(col=list(range(len(data_frame.columns))))
        tabula.set_fontsize(12)  # if ++fontsize is necessary ++colWidths
        tabula.scale(1.2, 1.2)  # change size table
        plt.savefig(f"/tmp/{table_name}-{self.Jira}.png", transparent=True)
        plt.show()
        html_buff = data_frame.to_html(index=True)
        html_file = open(f"/tmp/{table_name}-{self.Jira}.html", "w")
        html_file.write(html_buff)
        html_file.close()
        data_frame.to_csv(f"/tmp/{table_name}-{self.Jira}.csv", index=True)
        csbuf = data_frame.to_csv(index=True)
        self.make_table_from_csv(csbuf, table_name, index_name, comment)

    def run(self):
        """Run the program."""
        self.get_workflows()
        self.get_tasks()
        self.get_all_stat()
        self.log.info(f"workflow info")
        wfind = list()
        wflist = list()
        #        wfIndF = open('./wfInd.txt','w')
        """ Let sort datasets by creation time"""
        _dfids = dict()
        _dfkeys = list()
        for key in self.workflow_info:
            utime = self.workflow_info[key]["created"]
            _sttime = datetime.datetime.utcfromtimestamp(utime)
            self.workflow_info[key]["created"] = _sttime
            _dfids[key] = utime
        #
        for key in dict(sorted(_dfids.items(), key=lambda item: item[1])):
            wfind.append(str(key))
            _dfkeys.append(key)
            wflist.append(self.workflow_info[key])

        pd.set_option("max_colwidth", 500)
        pd.set_option("precision", 1)
        dataframe = pd.DataFrame(wflist, index=wfind)
        comment = " workflow status " + self.Jira
        index_name = "workflow"
        table_name = "pandaWfStat"
        self.make_table(dataframe, table_name, index_name, comment)
        self.make_styled_table(dataframe, table_name)
        _taskids = dict()
        ttypes = list()
        statlist = list()
        """Let's sort entries by start time"""
        for ttype in self.all_stat:
            utime = self.all_stat[ttype]["starttime"]
            utime = datetime.datetime.strptime(utime, "%Y-%m-%d %H:%M:%S").timestamp()
            _taskids[ttype] = utime
        #
        for ttype in dict(sorted(_taskids.items(), key=lambda item: item[1])):
            ttypes.append(ttype)
            statlist.append(self.all_stat[ttype])
        dfs = pd.DataFrame(statlist, index=ttypes)
        table_name = "pandaStat"
        index_name = " Workflow Task "
        comment = f" Panda campaign statistics {self.Jira}"
        self.make_table(dfs, table_name, index_name, comment)
