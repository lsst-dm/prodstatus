#
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
"""Interface for managing and reporting on data processing campaigns."""

# imports
from dataclasses import dataclass
import sys
import os
import io
from typing import Optional
from pathlib import Path
from tempfile import TemporaryDirectory
from copy import deepcopy
import yaml

from lsst.ctrl.bps import BpsConfig
from lsst.prodstatus.WorkflowN import WorkflowN
from lsst.prodstatus import LOG
from lsst.prodstatus.JiraUtils import JiraUtils

# constants

STEP_SPEC_FNAME = "step.yaml"
STEP_KEYWORDS = ("name", "issue_name", "campaign_issue",
                 "workflow_base", "workflows")
ALL_STEP_FNAMES = [STEP_SPEC_FNAME]

# classes


@dataclass
class StepN:
    """API for managing and reporting on data processing campaigns.

    Parameters
    ----------
    name : `str`
        An identifier for the step.
    issue_name : `str`
        A jira ticket for given step
    campaign_issue : `str`
        A jira ticket of campaign the step belongs to
    workflow_base : `str`
        A directory path containing bps submit files of workflows
    workflows : `dict`
        A dictionary containing workflow names as keys and workflow
        parameters as dictionary.

    """
    name: str
    issue_name: Optional[str] = None
    campaign_issue: Optional[str] = None
    workflow_base: Optional[str] = None
    workflows: Optional[dict] = None

    " create jira for saving results "
    ju = JiraUtils()
    a_jira, user = ju.get_login()

    @classmethod
    def generate_new(
            cls,
            name,
            issue_name=None,
            campaign_issue=None,
            workflow_base=None,
            workflows=None,
    ):
        """Generate a new step, constructing its workflows.

        Parameters
        ----------
        name : `str`
            The name of the step.
        issue_name: `str`
            jira ticket issue for given step is exists
        campaign_issue : `str`
            campaign jira ticket if exists
        workflow_base : `str`
            directory containing bps submit files for step workflows
        workflows : `dict`
            a dictionary containing workflow name as key and workflow
            parameters as dictionary

        Returns
        -------
        step : `Step`
            A new step with workflow instances.
        """
        step = cls(name, issue_name, campaign_issue, workflow_base, workflows)
        if workflow_base is not None:
            cls.workflows = dict()
            LOG.info(f"Before generate_workflows {workflow_base} {name}")
            step._generate_workflows(workflow_base, name)
        else:
            step.workflows = dict()
        return step

    def _generate_workflows(self, workflow_base, name):
        """Generate the workflows for this step.

        Parameters
        ----------
        workflow_base : `str`
            directory path where BPS configuration files
            for  workflows are.
        name : `str`
            A name of the step workflows belong to
        Returns
        -------
        workflows : `dict`
            A dictionary containing workflows

        """
        "scan workflow base for bps yaml files "
        LOG.info(f"in generate_workflows workflow_base {workflow_base}")
        if self.workflows is not None:
            workflows = deepcopy(self.workflows)
        else:
            workflows = dict()
        LOG.info(f" step name {name}")
        if workflow_base is None or workflow_base == '':
            return workflows
        for file_name in os.listdir(workflow_base):
            LOG.info(f" file name {file_name}")
            # check the files which  start with step token
            if file_name.startswith(name) and \
                    file_name.endswith('.yaml'):
                wf_name = file_name.split('.yaml')[0]
                wf_data = dict()
                wf_data["name"] = wf_name
                wf_data["bps_dir"] = workflow_base
                wf_data["issue_name"] = None
                wf_data["band"] = 'all'
                wf_data["step_issue"] = self.issue_name
                wf_data["bps_name"] = None
                bps_file = Path(workflow_base).joinpath(wf_name + '.yaml')
                wf_data["bps_config"] = BpsConfig(bps_file)
                if wf_name not in workflows:
                    workflow = WorkflowN.from_dict(wf_data)
                    LOG.info(f" Created new workflow {workflow}")
                    wf_issue = None
                    wf_data["issue_name"] = wf_issue
                    wf_data["step_issue"] = self.issue_name
                    workflows[wf_name] = wf_data

        return workflows

    def to_files(self, temp_dir):
        """Save step data to files in a directory.


        Parameters
        ----------
        temp_dir : `pathlib.Path`
            Directory into which to save files.
        """
        t_dir = Path(temp_dir)
        """ This loop is wrong need to work on workflows"""
        wf_spec = deepcopy(self.workflows)
        step_spec = {
            "name": self.name,
            "issue_name": self.issue_name,
            "campaign_issue": self.campaign_issue,
            "workflow_base": self.workflow_base,
            "workflows": wf_spec
        }

        if self.issue_name is not None:
            step_spec["issue"] = self.issue_name
        step_spec_path = t_dir.joinpath(STEP_SPEC_FNAME)
        step_spec_io = open(step_spec_path, "wt")
        yaml.dump(step_spec, step_spec_io, indent=4)
        LOG.info(f"Wrote {step_spec_path}")

    @classmethod
    def from_files(cls, temp_dir, name=None):
        """Load workflow data from files in a directory.

        Parameters
        ----------
        temp_dir : `pathlib.Path`
            Directory into which to save files.
        name : `str`
            The name of the step (used to determine the subdirectory)

        Returns
        -------
        step : `StepN`
            An initialized instance of a step.
        """
        t_dir = Path(temp_dir)
        if name is not None:
            t_dir = t_dir.joinpath(name)

        step_spec_path = t_dir.joinpath(STEP_SPEC_FNAME)
        with open(step_spec_path, "rt") as step_spec_io:
            step_spec = yaml.safe_load(step_spec_io)
            LOG.debug(f"Read {step_spec_path}")
        step = cls.from_dict(step_spec)

        return step

    def to_jira(self, jira=None, issue_name=None, replace=True):
        """Create jira issue, a yaml file with  step data and save it
         into the jira issue.

        Parameters
        ----------
        jira : `jira.JIRA`,
            The connection to Jira.
        issue_name : `str`, optional
            This issue in which to save step data.
            If None, a new issue will be created.
        replace : `bool`
            Remove existing jira attachments before adding new ones?

        Returns
        -------
        issue_name: `str`
            The issue name to which the workflow was written.
        """
        # raise NotImplementedError("This code is untested")
        print(f"Issue name {issue_name}")
        if issue_name is None and self.issue_name is not None:
            issue = jira.issue(self.issue_name)
        elif issue_name is not None and len(issue_name) > 0:
            issue = jira.issue(issue_name)
        else:   # if None
            issue = jira.create_issue(
                project="DRP",
                issuetype="Task",
                summary=f"Step {self.name}",
                description=f"Step {self.name}",
                components=[{"name": "Test"}],
            )
            LOG.info(f"Created issue {issue}")

        self.issue_name = str(issue)
        " Now create yaml file with step data "
        with TemporaryDirectory() as staging_dir:
            s_dir = Path(staging_dir)
            step_file = s_dir.joinpath("step.yaml")
            LOG.info(f"Creating step yaml {step_file}")
            step_spec = self.to_dict()
            with open(step_file, 'w') as sf:
                yaml.dump(step_spec, sf)
            for file_name in ALL_STEP_FNAMES:
                full_file_path = s_dir.joinpath(file_name)
                if full_file_path.exists():
                    for attachment in issue.fields.attachment:
                        if file_name == attachment.filename:
                            if replace:
                                LOG.warning(
                                    f"removing old attachment {file_name} from {self.issue_name}"
                                )
                                jira.delete_attachment(attachment.id)
                            else:
                                LOG.warning(
                                    f"{file_name} already exists in {self.issue_name}; not saving."
                                )
                    LOG.info(f" Full file path {full_file_path}")
                    jira.add_attachment(issue, attachment=str(full_file_path))
                    LOG.info(f"Added {file_name} to {issue}")

        return self.issue_name

    @classmethod
    def from_jira(cls, issue_name, jira):
        """Load campaign data from a jira issue.


        Parameters
        ----------
        jira : `jira.JIRA`,
            The connection to Jira.
        issue_name : `str`
            This issue name from which to load campaign data.

        Returns
        -------
        campaign : `Campaign`
            An initialized instance of a campaign.
        """
        step = None
        issue = jira.issue(issue_name)
        for attachment in issue.fields.attachment:
            att_file = attachment.filename
            if att_file == "step.yaml":
                a_yaml = io.BytesIO(attachment.get()).read()
                step_spec = yaml.load(a_yaml, Loader=yaml.Loader)
                LOG.info("Read yaml specs")
                step = cls.from_dict(step_spec)
                step.issue_name = str(issue)
                return step
            else:
                step = None
        return step

    @classmethod
    def from_dict(cls, par_dict):
        """Load campaign data from a jira issue.


        Parameters
        ----------
        par_dict : `dict`,
            The dictionary containing the step parameters

        Returns
        -------
        step : `StepN`
            An initialized instance of a step.
        """
        print("step parameters")
        print(par_dict)
        if "name" in par_dict:
            name = par_dict["name"]
        else:
            LOG.warning("name should be provided - exiting")
            sys.exit(-1)
        if "issue_name" in par_dict:
            issue_name = par_dict["issue_name"]
        else:
            issue_name = None
        if "campaign_issue" in par_dict:
            campaign_issue = par_dict["campaign_issue"]
        else:
            campaign_issue = cls.campaign_issue
        if "workflow_base" in par_dict:
            workflow_base = par_dict["workflow_base"]
            LOG.info(f'Workflow_base {workflow_base}')
        else:
            workflow_base = None
        if workflow_base is not None:
            LOG.info(f"workflow base {workflow_base}")
            print(" before _generate_workflows ", workflow_base)
            workflows = dict()
            step = cls(name, issue_name, campaign_issue,
                       workflow_base, workflows)
            step._generate_workflows(workflow_base, name)
#            step._generate_workflows(cls, workflow_base, name)
        else:
            workflows = dict()
            step = cls(name, issue_name, campaign_issue,
                       workflow_base, workflows)
        return step

    def to_dict(self):
        """ Create dictionary with step data

        Parameters
        ----------

        Returns
        -------
        step_spec : `dict`
            A dictionary containing step data.
        """
        step_spec = dict()
        step_spec["name"] = self.name
        step_spec["issue_name"] = self.issue_name
        step_spec["campaign_issue"] = self.campaign_issue
        step_spec["workflow_base"] = self.workflow_base
        step_spec["workflows"] = self.workflows

        return step_spec

    def __str__(self):
        output = f"""{self.__class__.__name__}
name: {self.name}
issue name: {self.issue_name}
workflows:"""

        for wf in self.workflows:
            pars = self.workflows[wf]
            output += f"\n - {pars['name']} (issue {pars['issue_name']})"

        return output
