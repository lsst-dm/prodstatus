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
# coding: utf-8
import sys
import re
import os
import datetime
from copy import deepcopy
from time import gmtime, strftime
from collections import defaultdict
import yaml
from appdirs import user_data_dir
from pathlib import Path
from tabulate import tabulate
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import table
from lsst.daf.butler import Butler
from lsst.resources import ResourcePath

from lsst.prodstatus import LOG

"""
#from lsst.daf.butler import ButlerURI
#
"""
# PropertySet needs to be imported to load the butler yaml.
from lsst.daf.base import PropertySet  # noqa: F401

__all__ = ['GetButlerStat']


class GetButlerStat:
    """Build production statistics table using Butler meta data.

    Parameters
    ----------
    Butler : `str`
        URL of the Butler storage
    Jira : `str`
        Jira ticket identifying production campaign used
        to select campaign workflows
    CollType : `str`
        token that with jira ticket will uniquely define the campaign workflows
    startTime : `str`
        time to start selecting workflows from in Y-m-d format
    stopTime : `str`
        time to stop selecting workflows in Y-m-d format
    maxtask : `int`
        maximum number of task files to analyse
    """

    def __init__(self, **kwargs):

        if "Butler" in kwargs:
            self.butler = kwargs["Butler"]
        else:
            self.butler = ""
        self.collection_type = kwargs["collType"]
        self.jira_ticket = kwargs["Jira"]
        self.start_date = kwargs["start_date"]
        self.stop_date = kwargs["stop_date"]
        self.max_task = int(kwargs["maxtask"])
        self.repo_root = self.butler
        self.butler = Butler(self.repo_root)
        self.registry = self.butler.registry
        self.workflow_res = dict()
        self.collection_keys = dict()
        self.collection_size = dict()
        self.collection_data = dict()
        self.old_stat = dict()
        self.last_stat = 0.
        app_name = "ProdStat"
        app_author = os.environ.get('USERNAME')
        data_dir = user_data_dir(app_name, app_author)
        self.data_path = Path(data_dir)
        if not os.path.exists(self.data_path):
            self.data_path.mkdir()
        self.start_stamp = datetime.datetime.strptime(self.start_date, "%Y-%m-%d").timestamp()
        self.stop_stamp = datetime.datetime.strptime(self.stop_date, "%Y-%m-%d").timestamp()
        self.log = LOG
        self.log.info(f" Collecting information for Jira ticket {self.jira_ticket}")
        self.log.info(f"Will store data in {self.data_path.absolute()}")

    @staticmethod
    def parse_metadata_yaml(yaml_file):
        """Parse the runtime and RSS data in the metadata yaml.

        Parameters
        ----------
        yaml_file : `str`
            File name for the runtime yaml metadata file.

        Returns
        -------
        results : `dict`
            dictionary of unpacked results

        Notes
        -----
        The yaml file should be file created by the lsst.pipe.base.timeMethod
        decorator as applied to pipetask methods.
        """

        time_types = "Cpu User System".split()
        min_fields = [f"Start{_}Time" for _ in time_types] + [
            f"start{_}Time" for _ in time_types
        ] + ['startUtc']
        max_fields = (
            [f"End{_}Time" for _ in time_types]
            + [f"end{_}Time" for _ in time_types]
            + ["MaxResidentSetSize", 'endUtc']
        )
        time_stamp = ["startUtc", "prepUtc"]
        results = dict()
        with open(yaml_file) as fd:
            md = yaml.safe_load(fd)
#        print("yaml dictionary")
#        print(md)
        methods = list(md.keys())
        for method in methods:
            print(f" method {method}")
            for key, value in md[method].items():
                print(f" key {key}  value {value}")
                if key == 'quantum':
                    quantum_dict = value['arrays']
                    print('quantum dictionary')
                    print(quantum_dict)
                    for qkey in quantum_dict:
                        if qkey in time_stamp:
                            start_string = value
                        if "T" in value:
                            tokens = start_string.split("T")
                            start_string = (
                                tokens[0] + " " + tokens[1]
                            )  # get rid of T in the date string
                        if "timestamp" not in results:
                            results["timestamp"] = start_string
                        for min_field in min_fields:
                            if min_field not in key:
                                continue
                            if min_field not in results or value < results[min_field]:
                                results[min_field] = float(value)
                                continue
                        for max_field in max_fields:
                            if max_field not in key:
                                continue
                            if max_field not in results or value > results[max_field]:
                                results[max_field] = float(value)
                                continue
        return results

    def set_butler(self, butler_string):
        """Set the butler URL if not set in input parameters.

        Parameters
        ----------
        butler_string : `str`
            Defines how to access the butler storage
        """

        self.butler = butler_string

    def search_collections(self):
        """Select collections.

        Returns
        -------
        collections : `list`
            A list of collections.
        """

        collections = list()
        pre_ops = self.jira_ticket
        for c in sorted(self.registry.queryCollections()):
            if pre_ops in str(c) and self.collection_type in str(c):
                sub_str = str(c).split(pre_ops)[1]
                if 'T' in sub_str and 'Z' in sub_str:
                    key = sub_str.split('/')[-1]
                    date_str = key.split('T')[0]
                    date_stamp = datetime.datetime.strptime(date_str, "%Y%m%d").timestamp()
                    if self.last_stat <= date_stamp <= self.stop_stamp:
                        collections.append(c)
                        self.collection_keys[c] = key
        self.log.info("selected collections ")
        print("selected collections")
        for key in collections:
            print(f"{key}")
            self.log.info(f"{key}")
        return collections

    def make_sum(self, task_size, task_res):
        """Calculate max RSS.

        Parameters
        ----------
        task_size : `int`
            number of quanta in the task
        task_res : `dict`
            dictionary with task parameters

        Returns
        -------
        summary : `dict`
            summary dictionary including:

            ``"nQuanta"``
                Number of quanta (`int`)
            ``"startTime"``
                Time stamp of task start (`str`)
            ``"cpu sec/job"``
                CPU time per quanta (`float`)
            ``"cpu-hours"``
                Total wall time for all quantas (`float`)
            ``"MaxRSS GB"``
                Maximum resident size of the task (`float`)
        """

        cpu_time = task_res["cpu_time"]
        max_rss = task_res["maxRSS"]
#        time_start = task_res["startTime"]
        ts = min(int(task_size), self.max_task)
        if len(cpu_time) > 0 and cpu_time[0] is not None:
            cpu_sum = 0.0
            for t in cpu_time:
                if isinstance(t, float):
                    cpu_sum += float(t)
            cpu_per_task = float(cpu_sum / ts)
            total_cpu = float(cpu_per_task * int(task_size))
        else:
            cpu_per_task = 0.0
            total_cpu = 0.0
        max_s = 0.0
        for s in max_rss:
            if isinstance(s, float) and float(s) >= max_s:
                max_s = float(s)
        if len(task_res["startTime"]) > 0:
            time_start = task_res["startTime"]
        else:
            time_start = [strftime("%Y-%m-%d %H:%M:%S", gmtime())]
        return {
            "nQuanta": int(task_size),
            "startTime": time_start[0],
            "cpu sec/job": float(cpu_per_task),
            "cpu-hours": float(total_cpu),
            "MaxRSS GB": float(max_s / 1048576.0),
        }

    def get_task_data(self, collections):
        """Collect datasets & IDs for collections in subsets of IDs by type.

        Parameters
        ----------
        collections : `list`
            list of data collection
        """

        data_type_pattern = ".*_metadata"
        pattern = re.compile(data_type_pattern)
        for collection in collections:
            try:
                dataset_refs = self.registry.queryDatasets(
                    pattern, collections=collection
                )
            except OSError():
                print(f"No datasets found for: {collection}")
                self.log.warning(f"No datasets found for: {collection}")
                continue
                #
            k = 0
            lc = 0  # task counter
            task_size = dict()
            task_refs = dict()
            curr_task = ''
            _refs = list()
            first = True
            for i, data_ref in enumerate(dataset_refs):
                k += 1
                task_name = str(data_ref).split("_")[0]
                if task_name not in task_size:
                    if first:
                        curr_task = task_name
                        first = False
                    else:
                        task_refs[curr_task] = _refs
                        curr_task = task_name
                    lc = 0
                    task_size[task_name] = 1
                    _refs = [data_ref]
                else:
                    task_size[task_name] += 1
                    lc += 1
                    if lc < self.max_task:
                        _refs.append(data_ref)
                    #                    else:
                task_refs[task_name] = _refs
            self.collection_data[collection] = task_refs
            self.collection_size[collection] = task_size

    def make_table_from_csv(self, buffer, out_file, index_name, comment):
        """Create table from csv file

        Parameters
        ----------
        buffer : `str`
            string buffer containing csv values
        out_file : `str`
            name of the table file
        index_name : `str`
            list of row names for the table
        comment : `str`
            additional string to be added to the top of table file

        Returns
        -------
        newbody : `str`
            buffer containing created table
        """

        new_body = comment + "\n"
        new_body += out_file + "\n"
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
            new_body += line
            i += 1
        new_body = new_body[:-2]
        with open(self.data_path.joinpath(f"{out_file}-{self.jira_ticket}.txt"), "w") as tb_file:
            print(new_body, file=tb_file)
        return new_body

    def get_old(self):
        """
         Read old data to append a new ones.
         This will permit to collecting statistics
         data on day by day bases
         """
        st_file = self.data_path.joinpath(f"butlerStat-{self.jira_ticket}.csv").absolute()
        self.log.info(f"Stat file {st_file}")
        if st_file.exists():
            self.old_stat = (pd.read_csv(st_file, header=0,
                                         index_col=0).squeeze('columns')).to_dict(orient='index')
            self.old_stat.pop('campaign')
        " Find latest time stamp "
        self.last_stat = 0.
        for key in self.old_stat:
            time_stat = datetime.datetime.strptime(self.old_stat[key]['startTime'],
                                                   "%Y-%m-%d %H:%M:%S").timestamp()
            if time_stat >= self.last_stat:
                self.last_stat = time_stat
        if self.last_stat == 0.:
            self.last_stat = self.start_stamp
        self.log.info(f"last stat stamp {self.last_stat}")

    def clean_history(self):
        """
         Clean previously collected data before running
          a new step
          """
        st_file = self.data_path.joinpath(f"butlerStat-{self.jira_ticket}.csv").absolute()
        if st_file.exists():
            os.remove(st_file)

    def run(self):
        """Run the program."""
        "First let's get old statistics information"
        self.get_old()
        "Now select collections with time stamp newer than old one "
        collections = self.search_collections()
        """Recreate Butler and registry """
        self.butler = Butler(self.repo_root, collections=collections)
        self.registry = self.butler.registry
        self.get_task_data(collections)
        """
        Process a list of datarefs, extracting the per-task resource usage
        info from the `*_metadata` yaml files.
        """
        verbose = True
        columns = ("detector", "tract", "patch", "band", "visit")
        """ create temporary file for parsing metadata yaml """
        if not os.path.exists(self.data_path.joinpath("tempTask.yaml")):
            my_file = open(self.data_path.joinpath("tempTask.yaml"), "w")
            test_dict = {"test": ""}
            yaml.dump(test_dict, my_file)
        for collection in collections:
            task_data = self.collection_data[collection]
            task_size = self.collection_size[collection]
            task_res = dict()
            ref_yaml = ''
            for task in task_data:
                data = defaultdict(list)
                data_refs = task_data[task]
                for i, data_ref in enumerate(data_refs):
                    if verbose:
                        if i % 100 == 0:
                            sys.stdout.write(".")
                            sys.stdout.flush()
                    try:
                        ref_yaml = self.butler.getURI(data_ref,
                                                      collections=collection)
                    except ValueError:
                        self.log.info(f"Yaml file {ref_yaml} not found - skipping")
                        continue
                    dest = ResourcePath(self.data_path.joinpath("tempTask.yaml"))
                    butler_uri = ResourcePath(ref_yaml)
                    if not butler_uri.exists():
                        print(f"The file {butler_uri} do not exists")
                        self.log.info(f"The file {butler_uri} do not exists")
                    data_id = dict(data_ref.dataId)
                    if "visit" not in data_id and "exposure" in data_id:
                        data_id["visit"] = data_id["exposure"]
                    for column in columns:
                        data[column].append(data_id.get(column, None))
                    """Copy metadata.yaml to local temp yaml """
                    dest.transfer_from(butler_uri, "copy", True)
                    """parse results """
                    results = self.parse_metadata_yaml(
                        yaml_file=self.data_path.joinpath("tempTask.yaml").absolute().name)
                    print("data from job")
                    print(results)
                    if (results.get("EndCpuTime", None) is None
                            and results.get("endCpuTime", None) is not None):
                        cpu_time = results.get("endCpuTime", None)
                    else:
                        cpu_time = results.get("EndCpuTime", None)
                    data["cpu_time"].append(cpu_time)
                    data["maxRSS"].append(results.get("MaxResidentSetSize", None))
                    if results.get("timestamp", None) is None:
                        data["startTime"].append(
                            strftime("%Y-%m-%d %H:%M:%S", gmtime())
                        )
                    else:
                        data["startTime"].append(results.get("timestamp", None))
                task_res[task] = data
                print("task results")
                print(task_res[task])
            key = self.collection_keys[collection]
            "Put old statistics in the workflow_res"
            self.workflow_res = deepcopy(self.old_stat)
            for task in task_res:
                self.workflow_res[f"{key}_{task}"] = self.make_sum(
                    task_size[task], task_res[task]
                )
            """Now create pandas frame to display results"""
        dt = dict()
        all_tasks = list()
        camp_cpu = 0.0
        camp_rss = 0.0
        camp_jobs = 0
        camp_cpu_per_task = 0.0
        for task in self.workflow_res:
            all_tasks.append(task)
            dt[task] = self.workflow_res[task]
            cpu_hours = self.workflow_res[task]['cpu-hours']
            self.log.info(f"cpu_hours {cpu_hours}")
            if isinstance(cpu_hours, str):
                if 'days' in str(cpu_hours):
                    days = int(cpu_hours.split('days,')[0])
                    hours = cpu_hours.split('days,')[1]
                    tokens = hours.split(':')
                elif 'day,' in str(cpu_hours):
                    days = int(cpu_hours.split('day,')[0])
                    hours = cpu_hours.split('day,')[1]
                    tokens = hours.split(':')
                else:
                    tokens = cpu_hours.split(':')
                    days = 0
                wall_time = datetime.timedelta(days=days, hours=int(tokens[0]), minutes=int(tokens[1]),
                                               seconds=int(float(tokens[2]))).total_seconds()
            else:
                wall_time = cpu_hours
            camp_cpu += float(wall_time)
            camp_jobs += self.workflow_res[task]["nQuanta"]
            if float(self.workflow_res[task]["MaxRSS GB"]) >= camp_rss:
                camp_rss = float(self.workflow_res[task]["MaxRSS GB"])
        all_tasks.append("Campaign")
        u_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        camp_data = {
            "nQuanta": int(camp_jobs),
            "startTime": u_time,
            "cpu sec/job": camp_cpu_per_task,
            "cpu-hours": float(camp_cpu),
            "MaxRSS GB": float(camp_rss),
        }
        dt["campaign"] = camp_data
        for t_type in dt:
            task = dt[t_type]
            if isinstance(task["cpu-hours"], float):
                task["cpu-hours"] = str(datetime.timedelta(seconds=task["cpu-hours"]))
            if isinstance(task["cpu sec/job"], float):
                task["cpu sec/job"] = round(task["cpu sec/job"], 2)
            task["MaxRSS GB"] = round(task["MaxRSS GB"], 2)
        pd.set_option("max_colwidth", 500)
        pd.set_option("display.precision", 1)
        _task_ids = dict()
        task_types = list()
        stat_list = list()
        """Let's sort entries by start time"""
        for task_type in dt:
            task = dt[task_type]
            u_time = task["startTime"]
            task["startTime"] = u_time
            tokens = u_time.split(".")
            u_time = tokens[0]
            task["startTime"] = u_time
            u_time = datetime.datetime.strptime(u_time, "%Y-%m-%d %H:%M:%S").timestamp()
            _task_ids[task_type] = u_time
        #
        for tt in dict(sorted(_task_ids.items(), key=lambda item: item[1])):
            task_types.append(tt)
            stat_list.append(dt[tt])

        data_frame = pd.DataFrame(stat_list, index=task_types)
        fig, ax = plt.subplots(figsize=(25, 35))  # set size frame
        ax.xaxis.set_visible(False)  # hide the x-axis
        ax.yaxis.set_visible(False)  # hide the y-axis
        ax.set_frame_on(False)  # no visible frame, uncomment if size is ok
        tabla = table(ax, data_frame, loc="upper right")
        tabla.auto_set_font_size(False)  # Activate set fontsize manually
        tabla.auto_set_column_width(col=list(range(len(data_frame.columns))))
        tabla.set_fontsize(12)  # if ++fontsize is necessary ++colWidths
        tabla.scale(1.2, 1.2)  # change size table
        plt.savefig(self.data_path.joinpath(f"butlerStat-{self.jira_ticket}.png"), transparent=True)
        plt.show()
        """ print the table """
        print('')
        print(tabulate(data_frame, headers="keys", tablefmt="fancy_grid"))
        " write HTML version of the table "
        html_buff = data_frame.to_html()
        html_file = open(self.data_path.joinpath(f"butlerStat-{self.jira_ticket}.html"), "w")
        try:
            html_file.write(html_buff)
            html_file.close()
        except IOError:
            self.log.warning("Failed to write html table")
            html_file.close()
        " Write results as csv file "
        data_file = self.data_path.joinpath(f"butlerStat-{self.jira_ticket}.csv")
        self.log.info(f"writing data frame {data_file}")
        data_frame.to_csv(data_file)
        cs_buf = data_frame.to_csv()
        table_name = "butlerStat"
        index_name = " Workflow Task "
        comment = f" Campaign Butler statistics {self.jira_ticket}"
        self.make_table_from_csv(cs_buf, table_name, index_name, comment)
