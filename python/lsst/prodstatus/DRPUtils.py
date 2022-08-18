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
import glob
import os
import sys
import re
import io
import yaml
from yaml import load, FullLoader
from appdirs import user_data_dir
from pathlib import Path

"from tempfile import TemporaryDirectory"
import datetime
import json
import numpy as np
import pandas as pd

from lsst.prodstatus.GetButlerStat import GetButlerStat
from lsst.prodstatus.GetPanDaStat import GetPanDaStat
from lsst.prodstatus.JiraUtils import JiraUtils

from lsst.prodstatus.StepN import StepN
from lsst.prodstatus.CampaignN import CampaignN
from lsst.prodstatus import LOG

# from lsst.ctrl.bps import BpsConfig

__all__ = ["DRPUtils"]


class DRPUtils:
    """Collection of DRP utilities."""

    # Create a JIRA class object instance for handling read/writes
    # to JIRA issues and JIRA issue creation
    def __init__(self):
        self.ju = JiraUtils()
        self.ajira, self.user_name = self.ju.get_login()

    @staticmethod
    def parse_yaml(bps_yaml_file, ts):
        """Extract elements of the BPS yaml file needed for other functions.

        Parameters
        ----------
        bps_yaml_file : `str`
            File name for yaml file with BPS connection data.
        ts: `str`
            TimeStamp in %Y%m%dT%H%M%SZ format, or "0" to use first
            available time.

        Returns
        -------
        bpsstr : `str`
            Description of the BPS connection data which will
            end up in the Jira issue description field.
        kwd : `dict` [`str`, `str`]
            Some values extracted from the yaml file, into a Key
            Word Dictionary.
        akwd : `dict`
            Additional data, mostly extracted from other files
            pointed to by the provided yaml file, entered into Another
            Key Word Dictionary.
        ts : `str`
            TimeStamp in %Y%m%dT%H%M%SZ format
        """
        # KeyWord list of keys extracted from the bps submit yaml which will
        # be displayed in the JIRA issue description
        # This is a subset of the important keywords for easy reference by
        # the JIRA issue viewer
        kwlist = [
            "campaign",
            "project",
            "payload",
            "pipelineYaml",
            "extraQgraphOptions",
        ]
        # 2nd level keywords important to display in the JIRA issue description
        kw = {
            "payload": [
                "payloadName",
                "butlerConfig",
                "dataQuery",
                "inCollection",
                "sw_image",
                "output",
            ]
        }
        # TBD:  use the BPS API to read this BPS yaml in rather than
        # direct yaml load.
        with open(bps_yaml_file, 'r') as f:
            d = yaml.load(f, Loader=yaml.FullLoader)
        kwd = dict()
        bpsstr = "BPS Submit Keywords:\n{code}\n"
        # Format the essential keywords from the BPS submit yaml
        # into readable form for the JIRA issue description
        for k, v in d.items():
            if k in kwlist:
                if k in kw:
                    for k1 in kw[k]:
                        kwd[k1] = v[k1]
                        bpsstr += str(k1) + ":" + str(v[k1]) + "\n"
                else:
                    kwd[k] = v
                    bpsstr += str(k) + ": " + str(v) + "\n"
        uniqid = f"./{os.path.dirname(bps_yaml_file)}/submit/{kwd['output']}"
        for k in kwd:
            v = kwd[k]
            uniqid = uniqid.replace("{" + str(k) + "}", v)
        print(uniqid)
        # find the 'long form' expanded bps submit yaml, with all includes
        # use the given timestamp if provided or else pick the most recent
        # one in the operator's
        # submit directory by sorting all timestamps in the directory
        if ts == "0":
            allpath = glob.glob(uniqid + "/*")
            allpath.sort()
            longpath = allpath[-1]
            ts = os.path.basename(longpath)
        else:
            # this needs to be upper case
            ts = ts.upper()
            longpath = uniqid + "/" + ts
        submittedyaml = kwd["output"] + "_" + ts
        for k in kwd:
            v = kwd[k]
            submittedyaml = submittedyaml.replace("{" + str(k) + "}", v)
        submittedyaml = submittedyaml.replace("/", "_")
        fullbpsyaml = longpath + "/" + submittedyaml + "_config.yaml"
        # print(fullbpsyaml)
        origyamlfile = longpath + "/" + os.path.basename(bps_yaml_file)
        bpsstr = bpsstr + "bps_submit_yaml_file: " + str(bps_yaml_file) + "\n"
        akwd = dict()
        # get unix file statistics (create time) for the original bps yaml file
        if os.path.exists(origyamlfile):
            (
                mode,
                ino,
                dev,
                nlink,
                uid,
                gid,
                size,
                atime,
                origyamlfilemtime,
                ctime,
            ) = os.stat(origyamlfile)
            # get unix file statistics (create time) for the expanded bps
            # yaml file
            if os.path.exists(fullbpsyaml):
                print(
                    "full bps yaml file exists -- updating start graph generation timestamp"
                )
                (
                    mode,
                    ino,
                    dev,
                    nlink,
                    uid,
                    gid,
                    size,
                    atime,
                    origyamlfilemtime,
                    ctime,
                ) = os.stat(fullbpsyaml)
                # print(origyamlfile,origyamlfilemtime,
                # time.ctime(origyamlfilemtime))
            # Submit KeyWords deemed important to be added to the JIRA issue
            # description for this workflow
            skwlist = ["bps_defined", "executionButler", "computeSite", "cluster"]
            # second level Submit KeyWords
            skw = {
                "bps_defined": ["operator", "uniqProcName"],
                "executionButler": ["queue"],
            }
            # TBD: use the BPS API to read this
            with open(fullbpsyaml, 'r') as f:
                d = load(f, Loader=FullLoader)
            # TBD: Consider using the logger here
            print(f"submityaml keys:{d}")
            for k, v in d.items():
                if k in skwlist:
                    if k in skw:
                        for k1 in skw[k]:
                            akwd[k1] = v[k1]
                            bpsstr += str(k1) + ":" + str(v[k1]) + "\n"
                    else:
                        akwd[k] = v
                        bpsstr += str(k) + ": " + str(v) + "\n"

            # TBD: Consider using the logger here
            print(f"akwd {akwd}")
            print(f"kwd {kwd}")
            print(bpsstr)
            # Get the unix filesystem stats (size, createtime) on the
            # qgraph file
            qgraphfile = longpath + "/" + submittedyaml + ".qgraph"
            (
                mode,
                ino,
                dev,
                nlink,
                uid,
                gid,
                qgraphfilesize,
                atime,
                mtime,
                ctime,
            ) = os.stat(qgraphfile)
            # TBD: use the logger there
            # print(qgraphfile, qgraphfilesize)
            # add the size of the quantum graph (in MB) to the essential
            # keyword list
            # info in the JIRA issue description
            bpsstr += (
                "qgraphsize:" + str("{:.1f}".format(qgraphfilesize / 1.0e6)) + "MB\n"
            )
            qgraphout = longpath + "/" + "quantumGraphGeneration.out"
            (
                mode,
                ino,
                dev,
                nlink,
                uid,
                gid,
                size,
                atime,
                qgraphoutmtime,
                ctime
            ) = os.stat(qgraphout)
            with open(qgraphout, 'r') as f:
                qgstat = f.read()
            # Parse the quantum graph output file and extract the number
            # of quanta, number of tasks for JIRA description
            m = re.search("QuantumGraph contains (.*) quanta for (.*) task", qgstat)
            if m:
                nquanta = m.group(1)
                ntasks = m.group(2)
                bpsstr += "nTotalQuanta:" + str("{:d}".format(int(nquanta))) + "\n"
                bpsstr += "nTotalPanDATasks:" + str("{:d}".format(int(ntasks))) + "\n"

            # example:
            # QuantumGraph contains 310365 quanta for 5 tasks
            # print(qgraphout,qgraphoutmtime,time.ctime(qgraphoutmtime))
            # determine the size and create time of the exec butler file
            execbutlerdb = longpath + "/EXEC_REPO-" + submittedyaml + "/gen3.sqlite3"
            (
                mode,
                ino,
                dev,
                nlink,
                uid,
                gid,
                butlerdbsize,
                atime,
                butlerdbmtime,
                ctime,
            ) = os.stat(execbutlerdb)
            # print(execbutlerdb,butlerdbsize,butlerdbmtime,time.ctime(butlerdbmtime))
            bpsstr += (
                "execbutlersize:"
                + str("{:.1f}".format(butlerdbsize / 1.0e6))
                + "MB"
                + "\n"
            )
            # compute the amount of time needed to create the qgraph
            timetomakeqg = qgraphoutmtime - origyamlfilemtime
            # compute the amount of time needed to create the exec butler
            timetomakeexecbutlerdb = butlerdbmtime - qgraphoutmtime
            # print(timetomakeqg,timetomakeexecbutlerdb)
            # add these keywords to the JIRA issue description
            bpsstr += (
                "timeConstructQGraph:"
                + str("{:.1f}".format(timetomakeqg / 60.0))
                + "min\n"
            )
            bpsstr += (
                "timeToFillExecButlerDB:"
                + str("{:.1f}".format(timetomakeexecbutlerdb / 60.0))
                + "min\n"
            )
            # condsider logging this info
            print(bpsstr)
        return bpsstr, kwd, akwd, ts

    @staticmethod
    def parse_drp(steppath, tocheck):
        """Build a list of step/task combinations for one or more steps.

        Parameters
        ----------
        steppath : `str`
            Path to the yaml file defining the steps and tasks.
        tocheck : `str`
            The comma delimited list of steps to check.

        Returns
        -------
        retdict : `list` [ `list` [`str`, `str`] ]
            A list of two-element lists. The elemements of the inner list
            are the step and task names.

        Notes
        -----

        If the DRP.yaml as put out by the Pipeline team changes
        -- this file should be updated.
        It is in  $OBS_LSST_DIR/pipelines/imsim/DRP.yaml
        """
        # TBD:  use the BPS API to parse this list of pipetasks within a step
        stepenvironsplit = steppath.split("}")
        if len(stepenvironsplit) > 1:
            envvar = stepenvironsplit[0][2:]
            restofpath = stepenvironsplit[1]
        else:
            envvar = ""
            restofpath = steppath
        print(envvar, restofpath)

        with open(os.environ.get(envvar) + restofpath) as drpfile:
            drpyaml = load(drpfile, Loader=FullLoader)

        # TBD: use the BPS API
        taskdict = dict()
        stepdict = dict()
        stepdesdict = dict()
        if 'subset' in drpyaml:
            subsets = drpyaml["subsets"]
            for k, v in subsets.items():
                stepname = k
                tasklist = v["subset"]
                tasklist.insert(0, "pipetaskInit")
                tasklist.append("mergeExecutionButler")
                # print(len(tasklist))
                # print('tasklist:',tasklist)
                taskdict["pipetaskInit"] = stepname
                for t in tasklist:
                    taskdict[t] = stepname
                taskdict["mergeExecutionButler"] = stepname
                stepdict[stepname] = tasklist
                stepdesdict[stepname] = v["description"]
        # assumes tasknames are unique
        # i.e. that there's not more than one step
        # with the same taskname
        # print("steps")
        # for k,v in stepdict.items():
        # print(k,v)
        # print(stepdesdict[k])
        steplist = tocheck.split(",")
        retdict = list()
        for i in steplist:
            if i in stepdict:
                for j in stepdict[i]:
                    retdict.append([i, j])
            elif i in taskdict:
                retdict.append([taskdict[i], i])
        return retdict

    def drp_stat_update(self, pissue, drpi):
        """Update the statistics in a jira issue.

        Parameters
        ----------
        pissue : `str`
            campaign defining ticket name, also in the butler output name
            (e.g. "PREOPS-973").
        drpi : `str`
            The data release processing issue name (e.g. "DRP-153").
        """
        #        ts = "0"
        # get summary from DRP ticket
        in_pars = dict()
        drp_issue = self.ajira.issue(drpi)
        summary = drp_issue.fields.summary
        print(f"summary is  {summary}")
        olddesc = drp_issue.fields.description
        print(f"old desc is {olddesc}")
        substr = "{code}"
        idx = olddesc.find(substr, olddesc.find(substr) + 1)
        print(idx)
        newdesc = olddesc[0:idx] + "{code}\n"
        print(f"new is {newdesc}")
        pattern0 = "(.*)#(.*)(20[0-9][0-9][0-9][0-9][0-9][0-9][Tt][0-9][0-9][0-9][0-9][0-9][0-9][Zz])"
        mts = re.match(pattern0, summary)
        if mts:
            what = mts.group(1)
            ts = mts.group(3)
        else:
            what = "0"
            ts = "0"

        print(ts, what)
        # run butler and/or panda stats for one timestamp.
        in_pars["Butler"] = "s3://butler-us-central1-panda-dev/dc2/butler-external.yaml"
        in_pars["Jira"] = str(pissue)
        in_pars["collType"] = ts.upper()
        in_pars["workNames"] = ""
        in_pars["maxtask"] = 100
        in_pars["start_date"] = "2021-01-01"
        in_pars["stop_date"] = datetime.datetime.now().isoformat()[:10]
        app_name = "ProdStat"
        app_author = os.environ.get('USERNAME')
        data_dir = user_data_dir(app_name, app_author)
        self.data_path = Path(data_dir)
        print("cleaning butler history")
        bh_file = self.data_path.joinpath(f"butlerStat-{str(pissue)}.csv").absolute()
        if bh_file.exists():
            os.remove(bh_file)
        get_butler_stat = GetButlerStat(**in_pars)
        get_butler_stat.run()
        butpath = self.data_path.absolute()
        butfn = f"/butlerStat-{str(pissue)}.txt"
        butfilename = str(butpath)+str(butfn)
        if os.path.exists(butfilename):
            with open(butfilename, 'r') as fbstat:
                butstat = fbstat.read()
        else:
            butstat = "\n"
        panfn = f"/pandaStat-{str(pissue)}.txt"
        panfilename = str(butpath)+str(panfn)
        in_pars["collType"] = ts.lower()
        print("cleaning panda history")
        wf_file = self.data_path.joinpath(f"pandaWfStat-{str(pissue)}.csv").absolute()
        st_file = self.data_path.joinpath(f"pandaStat-{str(pissue)}.csv").absolute()
        if wf_file.exists():
            os.remove(wf_file)
        if st_file.exists():
            os.remove(st_file)
        get_panda_stat = GetPanDaStat(**in_pars)
        get_panda_stat.run()
        panstatfilename = str(butpath)+str(f"/pandaWfStat-{str(pissue)}.csv")
        if os.path.exists(panfilename):
            with open(panfilename, 'r') as fpstat:
                statstr = fpstat.read()
            with open(panstatfilename, "r") as fstat:
                fstat.readline()
                line2 = fstat.readline()
                a = line2.split(",")
            # print(len(a),a)
            pstat = a[1]
            pntasks = int(a[2][:-2])
            pnfiles = int(a[3][:-2])
            pnproc = int(a[4][:-2])
            pnfin = int(a[6][:-2])
            pnfail = int(a[7][:-2])
            psubfin = int(a[8][:-2])
            curstat = f"Status:{str(pstat)} nTasks:{str(pntasks)} nFiles:{str(pnfiles)}"
            curstat += f" nRemain:{str(pnproc)} nProc: nFinish:{str(pnfin)} nFail:{str(pnfail)}"
            curstat += f" nSubFinish:{str(psubfin)}\n"
        else:
            statstr = "\n"
            curstat = "\n"
        # sys.exit(1)
        pupn = ts
        # print('pupn:',pupn)
        year = str(pupn[0:4])
        month = str(pupn[4:6])
        # day=str(pupn[6:8])
        day = str("01")
        print(f"year: {year}")
        print(f"year: {month}")
        print(f"year: {day}")
        link = f"https://panda-doma.cern.ch/tasks/?taskname=*{pupn.lower()}*&date_from={str(day)}"
        link += f"-{str(month)}-{str(year)}&days=62&sortby=time-ascending"
        print(f"link: {link}")
        linkline = f"PanDA link:{link}\n"

        issue_dict = {"description": newdesc + butstat + linkline + statstr + curstat}
        drp_issue.update(fields=issue_dict)
        print(f"issue:{str(drp_issue)}  Stats updated")

    @staticmethod
    def parse_issue_desc(jdesc, jsummary):
        """Extracts some information from DRP jira issue.

        Parameters
        ----------
        jdesc : `str`
            The content of the description field of a JIRA DRP issue.
        jsummary : `str`
            The content of the summary field of a JIRA DRP issue

        Returns
        -------
        ts : `str`
            Timestamp in %Y%m%dT%H%M%SZ format
        status : `list` [ `int` ]
            (T,Q,D,Fa,Sf), where:

                T
                    number of tasks
                Q
                    number of (high level) quanta
                D
                    Done (all 3 tasks finished completely)
                Fa
                    Failed
                Sf
                    Some Finished (task finished, but not everything
                                   with the task finished successfully,
                                   but moved on anyway)
        hilow : `str`
            Start and end tracts
        pandalink: `str`
            URL for the pandas task page
        what : `str`
            Which step the issue decribes.
        """
        pattern0 = "(.*)#(.*)(20[0-9][0-9][0-9][0-9][0-9][0-9][Tt][0-9][0-9][0-9][0-9][0-9][0-9][Zz])"
        mts = re.match(pattern0, jsummary)
        if mts:
            what = mts.group(1)
            ts = mts.group(3)
        else:
            what = "0"
            ts = "0"
        # print("ts:",ts)
        # print(jdesc)
        jlines = jdesc.splitlines()
        lm = iter(jlines)
        pattern1 = re.compile("(.*)tract in (.*)")
        pattern1a = re.compile("(.*)tract *=( *[0-9]*)")
        pattern1b = re.compile("(.*)tract *>=([0-9]*) and tract *<=( *[0-9]*)")
        pattern2 = re.compile("(.*)exposure >=( *[0-9]*) and exposure <=( *[0-9]*)")
        pattern2b = re.compile("(.*)visit *>=( *[0-9]*) and visit *<=( *[0-9]*)")
        pattern2a = re.compile(
            "(.*)detector>=( *[0-9]*).*exposure >=( *[0-9]*) and exposure <=( *[0-9]*)"
        )
        pattern3 = re.compile(
            "(.*)Status:.*nTasks:(.*)nFiles:(.*)nRemain.*nProc: nFinish:(.*) nFail:(.*) nSubFinish:(.*)"
        )
        # pattern3=re.compile("(.*)Status:(.*)")
        pattern4 = re.compile("(.*)PanDA.*link:(.*)")
        hilow = "()"
        status = [0, 0, 0, 0, 0]
        pandalink = ""
        for ls in lm:
            n1 = pattern1.match(ls)
            if n1:
                # print("Tract range:",n1.group(2),":end")
                hilow = n1.group(2)
                # print("hilow:",hilow)
            n1a = pattern1a.match(ls)
            if n1a:
                # print("Tract range:",n1.group(2),":end")
                hilow = f"({n1a.group(2)})"
                # print("hilow:",hilow)
            n1b = pattern1b.match(ls)
            if n1b:
                hilow = f"({str(int(n1b.group(2)))},{str(int(n1b.group(3)))})"
                # print("hilow:",hilow)
            n2 = pattern2.match(ls)
            if n2:
                hilow = f"({str(int(n2.group(2)))},{str(int(n2.group(3)))})"
                # print("hilow:",hilow)
            # else:
            n2b = pattern2b.match(ls)
            if n2b:
                hilow = f"({str(int(n2b.group(2)))},{str(int(n2b.group(3)))})"
            # print("no match to l",l)
            n2a = pattern2a.match(ls)
            if n2a:
                hilow = f"({str(int(n2a.group(3)))},{str(int(n2a.group(4)))})d{str(int(n2a.group(2)))}"
            n3 = pattern3.match(ls)
            if n3:
                statNtasks = int(n3.group(2))
                statNfiles = int(n3.group(3))
                statNFinish = int(n3.group(4))
                statNFail = int(n3.group(5))
                statNSubFin = int(n3.group(6))
                status = [statNtasks, statNfiles, statNFinish, statNFail, statNSubFin]
            m = pattern4.match(ls)
            if m:
                pandalink = m.group(2)
                # print("pandalink:",pandaline)

        # sys.exit(1)

        return ts, status, hilow, pandalink, what

    @staticmethod
    def _dict_to_table(in_dict):
        dictheader = ["Date", "PREOPS", "STATS", "(T,Q,D,Fa,Sf)", "PANDA", "DESCRIP"]

        table_out = "||"
        for i in dictheader:
            table_out += str(i) + "||"
        table_out += "\n"

        # sortbydescrip=sorted(in_dict[3])
        for i in sorted(in_dict.keys(), reverse=True):
            ts = i.split("#")[1]
            status = in_dict[i][2]
            nT = status[0]
            nFile = status[1]
            nFin = status[2]
            nFail = status[3]
            nSubF = status[4]
            statstring = f"{nT},{nFile},{nFin},{nFail},{nSubF}"
            scolor = "black"
            # print(statstring,nT,nFile,nFin,nFail,nSubF)
            if nFail > 0:
                scolor = "red"
            if nT == nFin + nSubF:
                scolor = "black"
            if nT == nFin:
                scolor = "green"
            if int(nFail) == 0 and int(nFile) == 0:
                scolor = "blue"
            if int(nT) > int(nFin) + int(nFail) + int(nSubF):
                scolor = "blue"

            longdatetime = ts
            shortyear = str(longdatetime[0:4])
            shortmon = str(longdatetime[4:6])
            shortday = str(longdatetime[6:8])
            # print(shortyear,shortmon,shortday)

            what = in_dict[i][4]
            if len(what) > 28:
                what = what[0:28]

            table_out += f"| {shortyear}-{shortmon}-{shortday} | ["
            table_out += f"{in_dict[i][0]}|https://jira.lsstcorp.org/browse/{in_dict[i][0]}] | "
            table_out += f"{in_dict[i][1]}|" + "{color:" + scolor + "}"
            table_out += f"{statstring}" + "{color}" + f"| [pDa|{in_dict[i][3]}] |{str(what)}|\n"

        return table_out

    @staticmethod
    def _dict_to_table1(in_dict):
        dictheader = ["Date", "PREOPS", "STATS", "(T,Q,D,Fa,Sf)", "PANDA", "DESCRIP"]

        table_out = "||"
        for i in dictheader:
            table_out += f"{str(i)}||"
        table_out += "\n"

        for i in sorted(in_dict.keys(), reverse=True):
            stepstring = in_dict[i][4]
            stepstart = stepstring[0:5]
            # print("stepstart is:",stepstart)
            if stepstart == "step1":
                ts = i.split("#")[1]
                status = in_dict[i][2]
                nT = status[0]
                nFile = status[1]
                nFin = status[2]
                nFail = status[3]
                nSubF = status[4]
                statstring = f"{nT},{nFile},{nFin},{nFail},{nSubF}"
                scolor = "black"
                if nFail > 0:
                    scolor = "red"
                if nT == nFin + nSubF:
                    scolor = "black"
                if nT == nFin:
                    scolor = "green"
                if nFail == 0 and nFile == 0:
                    scolor = "blue"

                longdatetime = ts
                shortyear = str(longdatetime[0:4])
                shortmon = str(longdatetime[4:6])
                shortday = str(longdatetime[6:8])
                # print(shortyear,shortmon,shortday)

                what = in_dict[i][4]
                if len(what) > 25:
                    what = what[0:25]
                table_out += f"| {shortyear}-{shortmon}-{shortday} | [{in_dict[i][0]}"
                table_out += f"|https://jira.lsstcorp.org/browse/{in_dict[i][0]}] | {in_dict[i][1]}"
                table_out += "|{color:" + scolor + "}"
                table_out += f"{statstring}" + "{color} | [pDa|"
                table_out += f"{in_dict[i][3]}] |{what}|\n"
        return table_out

    @staticmethod
    def map_drp_steps(map_yaml, stepissue, campaign_flag):
        """Update description of a step, by parsing the map yaml file.

        Parameters
        ----------
        map_yaml : `str`
          The yaml file name which maps DRP-XXXX tickets to
          bps submit yaml files

        stepissue : `str`
          The DRP-YYYY of the step to add the description table to

        campaign_flag: `int`
          If `0`:  This is a step table
          If `1':  This is a campaign table
        """
        print(campaign_flag, campaign_flag == '0')
        with open(map_yaml, "rt") as map_spec_io:
            map_spec = yaml.safe_load(map_spec_io)

        ju = JiraUtils()
        a_jira, user = ju.get_login()
        a_dict = {}
        for bps_yaml_name in map_spec.keys():
            drp_issue_name = map_spec[bps_yaml_name]
            if campaign_flag == '0':
                jissue = a_jira.issue(drp_issue_name)
                jdesc = jissue.fields.description
                jsummary = jissue.fields.summary
                ts, status, hilow, pandalink, what = DRPUtils.parse_issue_desc(jdesc, jsummary)
                a_dict[str(ts)] = [
                    str(bps_yaml_name),
                    str(jissue),
                    status,
                    str(hilow)
                ]
            else:
                a_dict[bps_yaml_name] = [
                    drp_issue_name[0],
                    drp_issue_name[1],
                    drp_issue_name[2],
                    drp_issue_name[3],
                    drp_issue_name[4]
                ]

        print("here")

        if campaign_flag == '0':
            newdesc = DRPUtils._dict_to_map_table(a_dict)
        else:
            newdesc = DRPUtils._dict_to_camp_table(a_dict)

        # print(newdesc)
        print(len(newdesc))

        sissue = a_jira.issue(stepissue)

        sissue.update(fields={"description": newdesc})
        print(f"description updated for: {sissue}")
        for attachment in sissue.fields.attachment:
            if os.path.basename(map_yaml) == attachment.filename:
                print("removing old attachment from issue")
                a_jira.delete_attachment(attachment.id)
        a_jira.add_attachment(str(sissue), attachment=str(map_yaml))
        print("added map_yaml attachment to issue")

    @staticmethod
    def _dict_to_camp_table(in_dict):
        dictheader = ["Step", "Issue", "Start", "End", "Core-hr", "Status"]

        table_out = "||"
        for i in dictheader:
            table_out += f"{str(i)}||"
        table_out += "\n"

        for i in in_dict.keys():
            stepname = i
            table_out += f"| {str(stepname)}| [{str(in_dict[i][0])}|https://jira.lsstcorp.org/browse/"
            table_out += f"{str(in_dict[i][0])}] | "
            table_out += f"{str(in_dict[i][1])}|{str(in_dict[i][2])}|{str(in_dict[i][3])}|"
            table_out += f"{str(in_dict[i][4])}| \n"

        return table_out

    @staticmethod
    def _dict_to_map_table(in_dict):
        dictheader = ["BPS_yaml", "Issue", "(T,Q,D,Fa,Sf)", "DESCRIP", "timestamp"]

        table_out = "||"
        for i in dictheader:
            table_out += f"{str(i)}||"
        table_out += "\n"

        # sortbydescrip=sorted(in_dict[3])
        # for i in sorted(in_dict.keys(), reverse=True):
        for i in in_dict.keys():
            status = in_dict[i][2]
            nT = status[0]
            nFile = status[1]
            nFin = status[2]
            nFail = status[3]
            nSubF = status[4]
            statstring = f"{str(nT)},{str(nFile)},{str(nFin)},{str(nFail)},{str(nSubF)}"
            scolor = "black"
            # print(statstring,nT,nFile,nFin,nFail,nSubF)
            if nFail > 0:
                scolor = "red"
            if nT == nFin + nSubF:
                scolor = "black"
            if nT == nFin:
                scolor = "green"
            if int(nFail) == 0 and int(nFile) == 0:
                scolor = "blue"
            if int(nT) > int(nFin) + int(nFail) + int(nSubF):
                scolor = "blue"

            # ts = i
            # longdatetime = ts
            # shortyear = str(longdatetime[0:4])
            # shortmon = str(longdatetime[4:6])
            # shortday = str(longdatetime[6:8])
            # print(shortyear,shortmon,shortday)

            what = in_dict[i][3]
            if len(what) > 28:
                what = what[0:28]

            table_out += f"| {str(in_dict[i][0])}| [{str(in_dict[i][1])}|https://jira.lsstcorp.org/browse/"
            table_out += f"{str(in_dict[i][1])}] | " + "{color:" + scolor + "}"
            table_out += f"{statstring}" + "{color} | " + str(what) + "|" + str(i) + "| \n"

        return table_out

    def drp_add_job_to_summary(
            self, first, pissue, jissue, frontend, frontend1, backend
    ):
        """Add a summary to a job summary tables in jira.````

        Parameters
        ----------
        first : `int`
            One of:

                ``0``
                    Add the new issue to existing content of the jira tickets
                ``1``
                    Erase the whole existing table, and replace it with
                    just the new job summary. Do not do this lightly!
                ``2``
                    Remove any existing content concerning this production
                    (PREOPS) and DRP issue with the new content.
        pissue : `str`
            The campaign defining ticket name (e.g. ``"PREOPS-973"``).
        jissue : `str`
            The data release processing issue name (e.g. ``"PREOPS-154"``).
        frontend : `str`
            Name of issue with table of all jobs (e.g. ``"DRP-53"``)
        frontend1 : `str`
            Name of issue with table of step 1 jobs (e.g. ``"DRP-55"``)
        backend : `str`
            Name of issue with serialization of jobs data (e.g. ``"DRP-54"``)
        """
        #        ju = JiraUtils()
        #        ajira, username = self.ju.get_login()
        backendissue = self.ajira.issue(backend)
        olddescription = backendissue.fields.description

        frontendissue = self.ajira.issue(frontend)
        frontendissue1 = self.ajira.issue(frontend1)

        jissue = self.ajira.issue(jissue)
        jdesc = jissue.fields.description
        jsummary = jissue.fields.summary
        print(f"summary is {jsummary}")
        ts, status, hilow, pandalink, what = self.parse_issue_desc(jdesc, jsummary)
        print(
            f"new entry (ts,status,hilow,pandalink,step) {ts}, {status}, {hilow}, {pandalink},{what}"
        )

        if first == 1:
            a_dict = dict()
        else:
            a_dict = json.loads(olddescription)

        if first == 2:
            print(f"removing PREOPS, DRP  {str(pissue)}, {str(jissue)}")
            for key, value in a_dict.items():
                # print("key",key,"value",value)
                if value[1] == str(jissue) and value[0] == str(pissue):
                    print("removing one key with: {str(jissue)}, {str(pissue)}")
                    del a_dict[key]
                    break
        else:
            a_dict[str(pissue) + "#" + str(ts)] = [
                str(pissue),
                str(jissue),
                status,
                pandalink,
                what + str(hilow),
            ]

        newdesc = self._dict_to_table(a_dict)
        frontendissue.update(fields={"description": newdesc})

        newdesc1 = self._dict_to_table1(a_dict)
        frontendissue1.update(fields={"description": newdesc1})

        newdict = json.dumps(a_dict)
        backendissue.update(fields={"description": newdict})
        print("Summary updated, see DRP-55 or DRP-53")

    @staticmethod
    def make_prod_groups(template, band, groupsize, skipgroups,
                         ngroups, explist):
        """Split a list of exposures into groups defined in yaml files.

        Parameters
        ----------
        template : `str`
            Template file with placeholders for start/end dataset/visit/tracts
            (optional .yaml suffix here will be added)
        band : `str`
            Which band to restrict to (or 'all' for no restriction, matches
            BAND in template if not 'all')
        groupsize : `int`
            How many visits (later tracts) per group (i.e. 500)
        skipgroups: `int`
            skip <skipgroups> groups (if others generating similar campaigns)
        ngroups : `int`
            how many groups (maximum)
        explist : `str`
            text file listing <band1> <exposure1> for all visits to use
        """
        template_base = os.path.basename(template)
        template_fname, template_ext = os.path.splitext(template_base)
        out_base = template_fname if template_ext == ".yaml" else template_base

        with open(template, "r") as template_file:
            template_content = template_file.read()

        exposures = pd.read_csv(explist, names=["band", "exp_id"], delimiter=r"\s+")
        exposures.sort_values("exp_id", inplace=True)
        if band not in ("all", "f"):
            exposures.query(f"band=='{band}'", inplace=True)

        # Add a new column to the DataFrame with group ids
        num_exposures = len(exposures)
        exposures["group_id"] = np.floor(np.arange(num_exposures) / groupsize).astype(
            int
        )

        for group_id in range(skipgroups, skipgroups + ngroups):
            group_exposures = exposures.query(f"group_id == {group_id}")
            min_exp_id = group_exposures.exp_id.min()
            max_exp_id = group_exposures.exp_id.max()

            # Add 1 to the group id, so it starts at 1, not 0
            group_num = group_id + 1
            out_content = (
                template_content.replace("GNUM", str(group_num))
                .replace("BAND", band)
                .replace("LOWEXP", str(min_exp_id))
                .replace("HIGHEXP", str(max_exp_id))
            )

            out_fname = f"{out_base}_{band}_{group_num}.yaml"
            with open(out_fname, "w") as out_file:
                out_file.write(out_content)

    def drp_issue_update(self, bpsyamlfile, pissue, drpi, ts):
        """Update or create a DRP issue.

        Parameters
        ----------
        bpsyamlfile : `str`
            File name for yaml file with BPS connection data.
        pissue : `str`
            The campaign defining ticket name (e.g. ``"PREOPS-973"``).
        drpi : `str`
            The data release processing issue name (e.g. "DRP-153").
        ts : `str`
            Timestamp in %Y%m%dT%H%M%SZ format
        """
        bpsstr, kwd, akwd, pupn = self.parse_yaml(bpsyamlfile, ts)
        print(f"pupn: {pupn}")
        year = str(pupn[0:4])
        month = str(pupn[4:6])
        # day=str(pupn[6:8])
        day = str("01")
        print(f"year:{year}")
        print(f"year:{month}")
        print(f"year:{day}")
        a_link = f"https://panda-doma.cern.ch/tasks/?taskname=*{pupn.lower()}*&date_from={str(day)}"
        a_link += f"-{str(month)}-{str(year)}&days=62&sortby=time-ascending"

        print(f"link:{a_link}")

        print(bpsstr, kwd, akwd)
        steppath = ''
        upn = kwd["campaign"] + "/" + pupn
        # upn.replace("/","_")
        # upn=d['bps_defined']['uniqProcName']
        stepname = kwd["pipelineYaml"]
        p = re.compile("(.*)#(.*)")
        m = p.match(stepname)
        print(f"stepname {stepname}")
        if m:
            steppath = m.group(1)
            stepcut = m.group(2)
        else:
            stepcut = ""

        print(f"steplist {stepcut}")
        print(f"steppath {steppath}")
        bpsstr += f"pipelineYamlSteps: {stepcut}" + "\n{code}\n"

        print(f"{upn} #{stepcut}")
        sl = self.parse_drp(steppath, stepcut)
        tasktable = (
            "Butler Statistics\n"
            + "|| Step || Task || Start || nQ || sec/Q || sum(hr) || maxGB ||"
            + "\n"
        )

        tasktable += "\n"
        print(tasktable)

        tasktable += "PanDA PREOPS: " + str(pissue) + " link:" + a_link + "\n"
        for s in sl:
            tasktable += f"|{s[0]}|{s[1]}| | | | | |\n"

        tasktable += "\n"
        print(tasktable)

        if drpi == "DRP0":
            issue = self.ajira.create_issue(
                project="DRP",
                issuetype="Task",
                summary="a new issue",
                description=bpsstr + tasktable,
                components=[{"name": "Test"}],
            )
        else:
            issue = self.ajira.issue(drpi)
        issue.update(
            fields={"summary": stepcut + "#" + upn, "description": bpsstr + tasktable}
        )
        print(f"issue:{str(issue)}")

    @staticmethod
    def update_campaign(campaign_yaml, campaign_issue, campaign_name):
        """Update or create a DRP campaign.

            Parameters
            ----------
            campaign_yaml : `str`
                File name for yaml file with BPS campaign data.
            campaign_issue : `str`
                The campaign issue ticket name (e.g. ``"DRP-186"``).
            campaign_name : `str`
                The name of campaign (e.g. "DRP-185"). If not specified can
                 be taken from campaign_yaml file
            """
        LOG.info(f" Campaign name: {campaign_yaml}")
        LOG.info(f"Campaign issue: {campaign_issue}")
        LOG.info(f"Input campaign name: {campaign_name}")
        """ Load yaml to dict to reserve possibility modify spec
        before creation of the campaign"""
        with open(campaign_yaml, "rt") as campaign_spec_io:
            campaign_spec = yaml.safe_load(campaign_spec_io)
        """ Read campaign specs from jira issue """
        " create jira for saving results "
        ju = JiraUtils()
        a_jira, user = ju.get_login()
        if campaign_issue is None and campaign_spec["issue"] is not None:
            campaign_issue = campaign_spec["issue"]
        if campaign_issue is not None:
            campaign = CampaignN.from_jira(campaign_issue, a_jira)
            if campaign is not None:
                jira_spec = campaign.to_dict()
                "keep old campaign issue in specs"
                if campaign_spec["issue"] != jira_spec["issue"]:
                    campaign_spec["issue"] = jira_spec["issue"]
                """ now create step issues from jira and update
                step specs in campaign specs
                """
                steps_dict = dict()
                steps = jira_spec["steps"]
                for step in steps:
                    name = step["name"]
                    issue_name = step["issue_name"]
                    steps_dict[name] = issue_name
                    steps_dict['campaign_issue'] = campaign_issue
                for step in campaign_spec["steps"]:
                    name = step["name"]
                    if name in steps_dict:
                        step["issue_name"] = steps_dict[name]
                        step["campaign_issue"] = campaign_issue

        " Now create campaign with updated specs"
        campaign = CampaignN.from_dict(campaign_spec, a_jira)
        print("Created campaign ")
        print(campaign)
        " Save campaign to jira "
        campaign_issue = campaign_spec["issue"]
        campaign.to_jira(a_jira, campaign_issue, replace=True, cascade=False)
        " At this point campaign issue should be created "
        campaign_issue = campaign.issue
        "Now create links between campaign and steps "
        link_type = "Relates"
        for step in campaign_spec["steps"]:
            step_issue = step["issue_name"]
            print(f"Creating link between {campaign_issue} and {step_issue}")
            a_jira.create_issue_link(link_type, campaign_issue, step_issue)
        LOG.info("Finish with update_campaign")

    @staticmethod
    def update_step(step_yaml, step_issue, campaign_issue, step_name):
        """Update or create a DRP step.

            Parameters
            ----------
            step_yaml : `str`
                File name for yaml file with BPS step data.
            step_issue : `str`
                The campaign issue ticket name (e.g. ``"DRP-186"``).
            campaign_issue : `str`
                The name of campaign (e.g. "DRP-185").  if specified then
            it should somehow attach this step.yaml to the
            campaign, it would be nice to allow that to specify
            the campaign by name rather than DRP number,
            but we can work on that later.
            step_name : `str`
            """
        LOG.info(f"Step yaml:{step_yaml}")
        LOG.info(f"Step issue: {step_issue}")
        LOG.info(f"Campaign name: {campaign_issue}")
        LOG.info(f"Input step name: {step_name}")
        step_dict = dict()
        " get data from input yaml"
        with open(step_yaml, 'r') as sf:
            in_step_dict = yaml.safe_load(sf)

        """" Lets check if step is in jira ang get step yaml
         if it is"""
        if step_issue is not None:
            ju = JiraUtils()
            step_dict = ju.get_yaml(step_issue, 'step.yaml')
            if len(step_dict) > 0:
                " If step exists with step.yaml "
                print("Get step data from jira")
                step_name = step_dict["name"]
                step_issue = step_dict["issue_name"]
                campaign_issue = step_dict["campaign_issue"]
                workflows = step_dict["workflows"]
                LOG.info(f"Step yaml:{step_yaml}")
                LOG.info(f"Step issue: {step_issue}")
                LOG.info(f"Campaign name: {campaign_issue}")
                LOG.info(f"Input step name: {step_name}")
                LOG.info("have jira issue -- read step specs")
            else:
                "if step exists but without step.yaml "
                "Get data from step_yaml"
                step_name = in_step_dict["name"]
                step_issue = in_step_dict["issue_name"]
                campaign_issue = in_step_dict["campaign_issue"]
                " workflow_base is a directory where workflow bps yamls are"
                workflows = in_step_dict["workflows"]
                LOG.info(f"step workflows {workflows}")
                LOG.info(f"Step yaml:{step_yaml}")
                LOG.info(f"Step issue: {step_issue}")
                LOG.info(f"Campaign name: {campaign_issue}")
                LOG.info(f"Input step name: {step_name}")

        else:
            " step does not exists - new one"
            "Get data from step_yaml"
            step_name = in_step_dict["name"]
            step_issue = in_step_dict["issue_name"]
            campaign_issue = in_step_dict["campaign_issue"]
            " workflow_base is a directory where workflow bps yamls are"
            workflows = in_step_dict["workflows"]
            LOG.info(f"step workflows {workflows}")
            LOG.info(f"Step yaml:{step_yaml}")
            LOG.info(f"Step issue: {step_issue}")
            LOG.info(f"Campaign name: {campaign_issue}")
            LOG.info(f"Input step name: {step_name}")
        "always update workflow base from input yaml "
        workflow_base = in_step_dict["workflow_base"]
        wf_path = Path(workflow_base)
        "Get workflows for the step from workflow_base"
        LOG.info("Updating workflows")
        for file_name in os.listdir(wf_path):
            if str(file_name).endswith('.yaml'):
                wf_name = str(file_name).split('.yaml')[0]
                bps_path = os.path.join(workflow_base, file_name)
                wf_data = dict()
                wf_data["name"] = wf_name
                wf_data["bps_dir"] = workflow_base
                wf_data["bps_config"] = str(bps_path)
                " if new workflow -  add to workflows "
                if wf_name not in workflows:
                    LOG.info("create new workflow")
                    workflows[wf_name] = wf_data

        step_dict["name"] = step_name
        step_dict["issue_name"] = step_issue
        step_dict["campaign_issue"] = campaign_issue
        step_dict["workflow_base"] = workflow_base
        step_dict["workflows"] = workflows
        LOG.info("Step dict")
        step = StepN.from_dict(step_dict)
        jira = JiraUtils()
        (auth_jira, user) = jira.get_login()
        step.to_jira(auth_jira, step_issue, replace=True)
        """
        tmp_dir = TemporaryDirectory()
        step.to_files(tmp_dir.name) """
        LOG.info("Finish with update_step")

    @staticmethod
    def create_step_yaml(step_yaml,
                         step_name,
                         step_issue,
                         campaign_issue,
                         workflow_dir):
        """Creates step yaml.
            \b
            Parameters
            ----------
            step_yaml : `str`
                A name of the step yaml with path
            step_name : `str`
                A name of the step.
            step_issue : `str`
                if specified  the step yaml will be loaded from the
                ticket and updated with input parameters
            campaign_issue : `str`
                Campaign jira ticket the step belongs to
            workflow_dir: `str`
                A name of the directory where workflow bps yaml files are,
                    including path
                """
        LOG.info("Start with create_step_yaml")
        LOG.info(f"step issue {step_issue}")
        LOG.info(f"campaign issue {campaign_issue}")
        LOG.info(f"step name {step_name}")
        LOG.info(f"step yaml {step_yaml}")
        LOG.info(f"Workflow_dir {workflow_dir}")
        step_template = dict()
        if step_issue is not None:
            "Read step yaml from ticket"
            ju = JiraUtils()
            auth_jira, username = ju.get_login()
            issue = ju.get_issue(step_issue)
            all_attachments = ju.get_attachments(issue)
            for aid in all_attachments:
                att_file = all_attachments[aid]
                if att_file == "step.yaml":
                    attachment = auth_jira.attachment(aid)  #
                    a_yaml = io.BytesIO(attachment.get()).read()
                    step_template = yaml.load(a_yaml, Loader=yaml.Loader)
        else:
            step_template['name'] = step_name
            step_template['issue_name'] = step_issue
            step_template['campaign_issue'] = campaign_issue
            step_template['workflow_base'] = workflow_dir
            step_template['workflows'] = dict()
            wf_path = Path(workflow_dir)
            "Get workflows for the step from workflow_base"
            for file_name in os.listdir(wf_path):
                # check the files which  start with step token
                if str(file_name).startswith(step_name):
                    wf_data = dict()
                    wf_name = str(file_name).split('.yaml')[0]
                    bps_path = os.path.join(workflow_dir, str(file_name))
                    LOG.info(f"wf_name {wf_name}")
                    LOG.info(f"bps_path {bps_path}")
                    wf_data['name'] = wf_name
                    wf_data['bps_name'] = None
                    wf_data['issue_name'] = None
                    wf_data['band'] = 'all'
                    wf_data['step_name'] = step_name
                    wf_data['bps_dir'] = bps_path
                    wf_data['step_issue'] = step_issue
                    step_template['workflows'][wf_name] = wf_data
        with open(step_yaml, 'w') as sf:
            yaml.dump(step_template, sf)

        LOG.info("Finish with create_step_yaml")

    @staticmethod
    def create_campaign_yaml(args):
        """Creates campaign yaml template.
        \b
        Parameters
        ----------
        args : `dict`
            A dictionary with arguments: campaign_name - `str`,
        campaign_yaml - `str`
            A yaml file name to which  campaign parameters
            will be written. Created yaml file should be
            considered as a template.
            It should be edited to add directory path where
            workflow files for each step are located.
        campaign_issue `str` issue name if already created
        """
        steps = ['step1', 'step2', 'step3', 'step4', 'step5',
                 'step6', 'step7']
        if "campaign_name" in args:
            campaign_name = args["campaign_name"]
        else:
            LOG.info("campaign_name should be provided -- aborting")
            sys.exit(-1)
        if "campaign_yaml" in args:
            campaign_yaml = args["campaign_yaml"]
        else:
            LOG.info("campaign_yaml was not provided using default campaign.yaml")
            campaign_yaml = "campaign.yaml"
        if "campaign_issue" in args:
            campaign_issue = args["campaign_issue"]
        else:
            campaign_issue = None
        LOG.info("Start with create_campaign_yaml")
        LOG.info(f"Campaign issue {campaign_issue}")
        LOG.info(f"Campaign name {campaign_name}")
        LOG.info(f"Campaign yaml {campaign_yaml}")
        campaign_template = dict()
        if campaign_issue is not None:
            "Read campaign yaml from ticket"
            ju = JiraUtils()
            auth_jira, username = ju.get_login()
            issue = ju.get_issue(campaign_issue)
            all_attachments = ju.get_attachments(issue)
            for aid in all_attachments:
                att_file = all_attachments[aid]
                if att_file == "campaign.yaml":
                    attachment = auth_jira.attachment(aid)  #
                    a_yaml = io.BytesIO(attachment.get()).read()
                    campaign_template = yaml.safe_load(a_yaml)
                    LOG.info(f"created campaign template yaml {campaign_template}")
        else:
            campaign_template['name'] = campaign_name
            campaign_template['issue'] = campaign_issue
            step_data = list()
            " create default steps "
            for step in steps:
                step_dict = dict()
                step_dict['issue_name'] = ''
                step_dict['name'] = step
                step_dict['split_bands'] = False
                step_dict['workflow_base'] = ''
                step_dict['campaign_issue'] = campaign_issue
                step_data.append(step_dict)
            campaign_template['steps'] = step_data
            with open(campaign_yaml, 'w') as cf:
                yaml.dump(campaign_template, cf)
        LOG.info("Finish with create_campaign_yaml")
