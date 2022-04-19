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
import glob
import os
import re
import io
import yaml
from yaml import load, FullLoader
import datetime
import json
import numpy as np
import pandas as pd
from lsst.prodstatus.GetButlerStat import GetButlerStat
from lsst.prodstatus.GetPanDaStat import GetPanDaStat
from lsst.prodstatus.JiraUtils import JiraUtils

from lsst.ctrl.bps import BpsConfig
from lsst.prodstatus.Workflow import Workflow
from lsst.prodstatus.Step import Step
from lsst.prodstatus.Campaign import Campaign
from lsst.prodstatus import LOG

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
            Description of the BPS connection data.
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
            d = load(f, Loader=FullLoader)
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
        uniqid = "./" + os.path.dirname(bps_yaml_file) + "/submit/" + kwd["output"]
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
        # print(longpath)
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
            print("submityaml keys:", d)
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
            print("akwd", akwd)
            print("kwd", kwd)
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
        print("summary is", summary)
        olddesc = drp_issue.fields.description
        print("old desc is", olddesc)
        substr = "{code}"
        idx = olddesc.find(substr, olddesc.find(substr) + 1)
        print(idx)
        newdesc = olddesc[0:idx] + "{code}\n"
        print("new is", newdesc)
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
        in_pars["start_date"] = "1970-01-01"
        in_pars["stop_date"] = datetime.datetime.now().isoformat()[:10]
        get_butler_stat = GetButlerStat(**in_pars)
        get_butler_stat.run()
        butfilename = "/tmp/butlerStat-" + str(pissue) + ".txt"
        if os.path.exists(butfilename):
            with open(butfilename, 'r') as fbstat:
                butstat = fbstat.read()
        else:
            butstat = "\n"
        panfilename = "/tmp/pandaStat-" + str(pissue) + ".txt"
        in_pars["collType"] = ts.lower()
        get_panda_stat = GetPanDaStat(**in_pars)
        get_panda_stat.run()
        if os.path.exists(panfilename):
            with open(panfilename, 'r') as fpstat:
                statstr = fpstat.read()
            with open("/tmp/pandaWfStat-" + str(pissue) + ".csv", "r") as fstat:
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
            curstat = (
                "Status:"
                + str(pstat)
                + " nTasks:"
                + str(pntasks)
                + " nFiles:"
                + str(pnfiles)
                + " nRemain:"
                + str(pnproc)
                + " nProc:"
                + " nFinish:"
                + str(pnfin)
                + " nFail:"
                + str(pnfail)
                + " nSubFinish:"
                + str(psubfin)
                + "\n"
            )
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
        print("year:", year)
        print("year:", month)
        print("year:", day)
        link = (
            "https://panda-doma.cern.ch/tasks/?taskname=*"
            + pupn.lower()
            + "*&date_from="
            + str(day)
            + "-"
            + str(month)
            + "-"
            + str(year)
            + "&days=62&sortby=time-ascending"
        )
        print("link:", link)
        linkline = "PanDA link:" + link + "\n"

        issue_dict = {"description": newdesc + butstat + linkline + statstr + curstat}
        drp_issue.update(fields=issue_dict)
        print("issue:" + str(drp_issue) + " Stats updated")

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
        pattern2 = re.compile("(.*)exposure >=([0-9]*) and exposure <=( *[0-9]*)")
        pattern2b = re.compile("(.*)visit >=([0-9]*) and visit <=( *[0-9]*)")
        pattern2a = re.compile(
            "(.*)detector>=([0-9]*).*exposure >=( *[0-9]*) and exposure <=( *[0-9]*)"
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
                hilow = "(" + n1a.group(2) + ")"
                # print("hilow:",hilow)
            n1b = pattern1b.match(ls)
            if n1b:
                print("tractlo:", n1b.group(2), " tracthigh:", n1b.group(3), ":end")
                hilow = (
                    "(" + str(int(n1b.group(2))) + "," + str(int(n1b.group(3))) + ")"
                )
                # print("hilow:",hilow)
            n2 = pattern2.match(ls)
            if n2:
                print("exposurelo:", n2.group(2), " exphigh:", n2.group(3), ":end")
                hilow = "(" + str(int(n2.group(2))) + "," + str(int(n2.group(3))) + ")"
                # print("hilow:",hilow)
            # else:
            n2b = pattern2b.match(ls)
            if n2b:
                print("visitlo:", n2b.group(2), " visthigh:", n2b.group(3), ":end")
                hilow = (
                    "(" + str(int(n2b.group(2))) + "," + str(int(n2b.group(3))) + ")"
                )
            # print("no match to l",l)
            n2a = pattern2a.match(ls)
            if n2a:
                print(
                    "detlo",
                    n2a.group(2),
                    "exposurelo:",
                    n2a.group(3),
                    " exphigh:",
                    n2a.group(4),
                    ":end",
                )
                hilow = (
                    "("
                    + str(int(n2a.group(3)))
                    + ","
                    + str(int(n2a.group(4)))
                    + ")d"
                    + str(int(n2a.group(2)))
                )
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
    def _dict_to_table(in_dict, sorton):
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
            statstring = (
                str(nT)
                + ","
                + str(nFile)
                + ","
                + str(nFin)
                + ","
                + str(nFail)
                + ","
                + str(nSubF)
            )
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

            table_out += (
                "| "
                + str(shortyear)
                + "-"
                + str(shortmon)
                + "-"
                + str(shortday)
                + " | ["
                + str(in_dict[i][0])
                + "|https://jira.lsstcorp.org/browse/"
                + str(in_dict[i][0])
                + "] | "
                + str(in_dict[i][1])
                + "|{color:"
                + scolor
                + "}"
                + statstring
                + "{color} | [pDa|"
                + in_dict[i][3]
                + "] |"
                + str(what)
                + "|\n"
            )
        return table_out

    @staticmethod
    def _dict_to_table1(in_dict, sorton):
        dictheader = ["Date", "PREOPS", "STATS", "(T,Q,D,Fa,Sf)", "PANDA", "DESCRIP"]

        table_out = "||"
        for i in dictheader:
            table_out += str(i) + "||"
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
                statstring = (
                    str(nT)
                    + ","
                    + str(nFile)
                    + ","
                    + str(nFin)
                    + ","
                    + str(nFail)
                    + ","
                    + str(nSubF)
                )
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
                table_out += (
                    "| "
                    + str(shortyear)
                    + "-"
                    + str(shortmon)
                    + "-"
                    + str(shortday)
                    + " | ["
                    + str(in_dict[i][0])
                    + "|https://jira.lsstcorp.org/browse/"
                    + str(in_dict[i][0])
                    + "] | "
                    + str(in_dict[i][1])
                    + "|{color:"
                    + scolor
                    + "}"
                    + statstring
                    + "{color} | [pDa|"
                    + in_dict[i][3]
                    + "] |"
                    + str(what)
                    + "|\n"
                )
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
        print("summary is", jsummary)
        ts, status, hilow, pandalink, what = self.parse_issue_desc(jdesc, jsummary)
        print(
            "new entry (ts,status,hilow,pandalink,step)",
            ts,
            status,
            hilow,
            pandalink,
            what,
        )

        if first == 1:
            a_dict = dict()
        else:
            a_dict = json.loads(olddescription)

        if first == 2:
            print("removing PREOPS, DRP", str(pissue), str(jissue))
            for key, value in a_dict.items():
                # print("key",key,"value",value)
                if value[1] == str(jissue) and value[0] == str(pissue):
                    print("removing one key with:", str(jissue), str(pissue))
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

        newdesc = self._dict_to_table(a_dict, -1)
        frontendissue.update(fields={"description": newdesc})

        newdesc1 = self._dict_to_table1(a_dict, -1)
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
            Template file with place holders for start/end dataset/visit/tracts
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
        explists : `str`
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

            # Add 1 to the group id so it starts at 1, not 0
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
        print("pupn:", pupn)
        year = str(pupn[0:4])
        month = str(pupn[4:6])
        # day=str(pupn[6:8])
        day = str("01")
        print("year:", year)
        print("year:", month)
        print("year:", day)
        a_link = (
            "https://panda-doma.cern.ch/tasks/?taskname=*"
            + pupn.lower()
            + "*&date_from="
            + str(day)
            + "-"
            + str(month)
            + "-"
            + str(year)
            + "&days=62&sortby=time-ascending"
        )

        print("link:", a_link)

        print(bpsstr, kwd, akwd)

        upn = kwd["campaign"] + "/" + pupn
        # upn.replace("/","_")
        # upn=d['bps_defined']['uniqProcName']
        stepname = kwd["pipelineYaml"]
        p = re.compile("(.*)#(.*)")
        m = p.match(stepname)
        print("stepname " + stepname)
        if m:
            steppath = m.group(1)
            stepcut = m.group(2)
        else:
            stepcut = ""

        print("steplist " + stepcut)
        print("steppath " + steppath)
        bpsstr += "pipelineYamlSteps: " + stepcut + "\n{code}\n"

        print(upn + "#" + stepcut)
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
            tasktable += (
                "|"
                + s[0]
                + "|"
                + s[1]
                + "|"
                + " "
                + "|"
                + " "
                + "|"
                + " "
                + "|"
                + " "
                + "|"
                + " "
                + "|"
                + "\n"
            )

        tasktable += "\n"
        print(tasktable)
        # (totmaxmem,totsumsec,nquanta,secperstep,sumtime,maxmem)=parsebutlertable(butstepfile)

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
        print("issue:" + str(issue))

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
        """ Workflows should be created from campaign.yaml
           if we need to create steps here or just take list of steps from
           the campaign_yaml """
        campaign_workflows = list()
        LOG.info(campaign_workflows)
        campaign_steps = list()

        campaign = Campaign(campaign_name, steps=campaign_steps)
        LOG.info(f"Campaign name: {campaign.name}")

    @staticmethod
    def update_step(step_yaml, step_issue, campaign_name, step_name):
        """Update or create a DRP step.

            Parameters
            ----------
            step_yaml : `str`
                File name for yaml file with BPS step data.
            step_issue : `str`
                The campaign issue ticket name (e.g. ``"DRP-186"``).
            campaign_name : `str`
                The name of campaign (e.g. "DRP-185").  if specified then
            it should somehow attach this step.yaml to the
            campaign, it would be nice to allow that to specify
            the campaign by name rather than DRP number,
            but we can work on that later.
            step_name : `str`
            """
        LOG.info(f"Step yaml:{step_yaml}")
        LOG.info(f"Step issue: {step_issue}")
        LOG.info(f"Campaign name: {campaign_name}")
        LOG.info(f"Input step name: {step_name}")
        "Get workflows for the step from step_yaml"
        workflows = list()

        step = Step(step_name, workflows)
        LOG.info(f"Step name: {step.name}")

    @staticmethod
    def update_workflow(workflow_yaml, workflow_issue, step_issue, workflow_name):
        """Creates workflow
         It overwrites the existing DRP-187 ticket
         (or makes a new one if --issue isn't given),
         it adds the workflow to the list of steps in the
          stepIssue.
          It looks reads the 'full bps yaml' with all includes
          and saves that as an attachment.

        Parameters
        ----------
        workflow_yaml : `str`
            A yaml file from which to get step parameters.
        workflow_issue : `str`
            if specified  it overwrite a pre-existing DRP ticket,
            if not, it creates a new JIRA issue.
        step_issue : `str`
        workflow_name : `str`
            """
        LOG.info(f"Workflow yaml: {workflow_yaml}")
        LOG.info(f"Workflow issue: {workflow_issue}")
        LOG.info(f"Step issue: {step_issue}")
        LOG.info(f"input workflow name: {workflow_name}")
        bps_config = BpsConfig(workflow_yaml)
        workflow = Workflow(bps_config, workflow_name)
        LOG.info(f"workflow name: {workflow.name}")

    @staticmethod
    def make_workflow_yaml(step_dir, step_name_base, workflow_yaml):
        """Creates/updates workflow.yaml for update_workflow command
           It read all step yaml files in a directory and creates new entry
           in the workflow yaml file
        \b
        Parameters
        ----------

        step_dir : `str`
            A directory path where the step yaml files are
        step_name_base : `str`
            A base name to create unique step names
        workflow_yaml : `str`
            A yaml file name where workflow names and step
            yaml files are stored
            If exists workflow parameters will be updated.
        """
        LOG.info("Start with make-workflow-yaml")
        LOG.info(f"Step dir:{step_dir}")
        LOG.info(f"Step base name {step_name_base}")
        LOG.info(f"Workflow yaml: {workflow_yaml}")
        if os.path.exists(workflow_yaml):
            with open(workflow_yaml) as wf:
                workflows = yaml.safe_load(wf)
                print(workflows)
            if workflows is None:
                workflows = dict()
        else:
            workflows = dict()
        " Get list of yaml files in step directory"
        step_files = list()
        for file in os.listdir(step_dir):
            # check the files which  start with step token
            if file.startswith("step"):
                # print path name of selected files
                step_files.append(file)
        for file_name in step_files:
            wf_dict = dict()
            wf_name = file_name.split('.yaml')[0]
            wf_dict['name'] = wf_name
            wf_dict['bps_name'] = ''
            wf_dict['issue_name'] = None
            wf_dict['band'] = 'all'
            wf_dict['step_name'] = step_name_base
            wf_dict['path'] = os.path.join(step_dir, file_name)
            if wf_name not in workflows:
                workflows[wf_name] = wf_dict
        LOG.info("created workflow")
        with open(workflow_yaml, 'w') as wf:
            yaml.dump(workflows, wf)
        print(workflows)
        LOG.info("Finish with update_workflow")

    @staticmethod
    def create_step_yaml(step_yaml, step_name, step_issue, workflow_dir):
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
                workflow_dir: `str`
                    A name of the directory where workflow bps yaml files are,
                    including path
                """
        LOG.info("Start with create_step_yaml")
        LOG.info(f"step issue {step_issue}")
        LOG.info(f"step name {step_name}")
        LOG.info(f"step yaml {step_yaml}")
        LOG.info(f"Workflow_dir {workflow_dir}")
        step_template = dict()
        if step_issue is not None:
            "Read step yaml from ticket"
            ju = JiraUtils()
            auth_jira, username = ju.get_login()
            issue = ju.get_issue(step_issue)
            print(issue)
            all_attachments = ju.get_attachments(issue)
            print(all_attachments)
            for aid in all_attachments:
                print(aid, all_attachments[aid])
                att_file = all_attachments[aid]
                if att_file == "step.yaml":
                    attachment = auth_jira.attachment(aid)  #
                    a_yaml = io.BytesIO(attachment.get()).read()
                    step_template = yaml.safe_load(a_yaml)
                    print(step_template)
        else:
            step_template['name'] = step_name
            step_template['issue'] = step_issue
            step_template['workflows'] = list()
        workflow_data = list()
        print(workflow_data)

    @staticmethod
    def create_campaign_yaml(campaign_name, campaign_yaml, campaign_issue, steps_list):
        """Creates or updates campaign.
        \b
        Parameters
        ----------
        campaign_name : `str`
            An arbitrary name of the campaign.
        campaign_yaml : `str`
            A yaml file to which  campaign parameters will be written.
        campaign_issue : `str`
            if specified  the campaing yaml will be loaded from the
            ticket and updated with input parameters
        """
        steps = ['step1', 'step2', 'step3', 'step4', 'step5',
                 'step6', 'step7']
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
            print(issue)
            all_attachments = ju.get_attachments(issue)
            print(all_attachments)
            for aid in all_attachments:
                print(aid, all_attachments[aid])
                att_file = all_attachments[aid]
                if att_file == "campaign.yaml":
                    attachment = auth_jira.attachment(aid)  #
                    a_yaml = io.BytesIO(attachment.get()).read()
                    campaign_template = yaml.safe_load(a_yaml)
                    print(campaign_template)
        else:
            campaign_template['name'] = campaign_name
            campaign_template['issue'] = campaign_issue
            campaign_template['steps'] = list()
        step_data = list()
        if steps_list is None:
            " create default steps "
            for step in steps:
                step_dict = dict()
                step_dict['issue'] = ''
                step_dict['name'] = step
                step_dict['split_bands'] = False
                step_dict['forkflow_dir']
                step_dict['workflows'] = list()
                step_data.append(step_dict)
        else:
            with open(steps_list, 'r') as sl:
                step_data = yaml.safe_load(sl)
        campaign_template['steps'] = step_data
        with open(campaign_yaml, 'w') as cf:
            yaml.dump(campaign_template, cf)
        LOG.info("Finish with create_campaign_yaml")
