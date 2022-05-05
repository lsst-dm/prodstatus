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

"""Interface for managing and reporting on data processing workflows."""

# imports
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml
import numpy as np

from lsst.ctrl.bps import BpsConfig
from lsst.prodstatus import LOG

# constants

BPS_CONFIG_FNAME = "bps_config.yaml"
WORKFLOW_FNAME = "workflow.yaml"
WORKFLOW_KEYWORDS = ("bps_dir", "name", "bps_name", "step_name", "issue_name",
                     "step_issue", "bps_config")
ALL_WORKFLOW_FNAMES = [WORKFLOW_FNAME]

# exception classes


class NoExposuresException(Exception):
    """Raised when exposures values are needed, but not set."""

    pass


# interface functions

# classes


@dataclass
class WorkflowN:
    """API for managing and reporting on data processing campaigns.

    Parameters
    ----------
    bps_dir: `str`
        The directory where bps yaml files are
    name : `str`, optional
        An identifier for the workflow.
        The default is None.
    bps_name : `str`, optional
        The name of workflow as created by bps, including time stamp
    step_name : `str`, None
        The step name that workflow is a part of.
    issue_name : `str`
        The jira issue name for the issue that tracks this workflow.
    step_issue : `str`
        The jira issue name os the step the workflow belongs to.
    bps_config : BpsConfig
        The BPS configuration for this workflow.

    """
    bps_dir: str
    name: Optional[str] = None
    bps_name: Optional[str] = None
    step_name: Optional[str] = None
    issue_name: Optional[str] = None
    step_issue: Optional[str] = None
    bps_config: Optional[BpsConfig] = None

    @classmethod
    def from_dict(cls, par_dict):
        """Create WorkflowN object using parameters from the dictionary

                Parameters
                ----------
                par_dict : `dictionary`
                    The dictionary containing class parameters

                """
        if "bps_dir" in par_dict:
            bps_dir = par_dict["bps_dir"]
        else:
            bps_dir = None
        LOG.info(f"bps dir {bps_dir}")
        if "name" in par_dict:
            name = par_dict["name"]
        else:
            name = None
        LOG.info(f" workflow name {name}")
        if "bps_name" in par_dict:
            bps_name = par_dict["bps_name"]
        else:
            bps_name = None
        if "step_name" in par_dict:
            step_name = par_dict["step_name"]
        else:
            step_name = None
        LOG.info(f"step name {step_name}")
        if "issue_name" in par_dict:
            issue_name = par_dict["issue_name"]
        else:
            issue_name = None
        if "step_issue" in par_dict:
            step_issue = par_dict["step_issue"]
        else:
            step_issue = None

        if bps_dir is not None and name is not None:
            bps_file = Path(bps_dir).joinpath(name + '.yaml')
        else:
            bps_file = None
        LOG.info(f"bps_file {bps_file}")
        if bps_file is not None:
            bps_config = BpsConfig(bps_file)
        else:
            bps_config = None

        LOG.info(f"Creating WorkflowN with name {name}")
        LOG.info(f" bps_name {bps_name} step_name {step_name}")
        LOG.info(f" step issue {step_issue} bps_dir {bps_dir}")
        workflow = cls(bps_dir, name, bps_name, step_name, issue_name, step_issue, bps_config)
        LOG.debug(workflow)
        return workflow

    def to_yaml(self, yaml_file):
        """Create yaml file representing the workflow

                Parameters
                ----------
                yaml_file : `str`
                    The file name to be created

                """
        par_dict = dict()
        par_dict["bps_dir"] = self.bps_dir
        par_dict["name"] = self.name
        par_dict["bps_name"] = self.bps_name
        par_dict["step_name"] = self.step_name
        par_dict["issue_name"] = self.issue_name
        par_dict["step_issue"] = self.step_issue
        par_dict["bps_config"] = self.bps_config
        tmp_dir = TemporaryDirectory()
        LOG.info(f" tmp dir is {tmp_dir.name}")
        workflow_path = Path(tmp_dir.name).joinpath(yaml_file)
        with open(workflow_path, "wt") as workflow_io:
            yaml.dump(par_dict, workflow_io)
            LOG.debug(f"Wrote {workflow_path}")

    def to_dict(self):
        """Return workflow parameters as dictionary

                        """
        par_dict = dict()
        par_dict["bps_dir"] = self.bps_dir
        par_dict["name"] = self.name
        par_dict["bps_name"] = self.bps_name
        par_dict["step_name"] = self.step_name
        par_dict["issue_name"] = self.issue_name
        par_dict["step_issue"] = self.step_issue
        par_dict["bps_config"] = self.bps_config
        return par_dict

    def to_files(self, tmp_dir):
        """Save workflow data to files in a directory.


        Parameters
        ----------
        tmp_dir : `str`
            Directory into which to save files.

        Returns
        -------
        None.
        """
#        tmp_dir = Path(tmp_dir)
        if self.name is not None:
            print(f"self name {self.name}")
            tmp_dir = Path(tmp_dir)
            tmp_dir = tmp_dir.joinpath(self.name)
            print(f"tmp dir {str(tmp_dir)}")
            tmp_dir.mkdir(exist_ok=True)
        bps_config_path = tmp_dir.joinpath(BPS_CONFIG_FNAME)
        print(f"bps_config_path {bps_config_path}")
        if self.bps_config is not None:
            with open(bps_config_path, "wt") as bps_config_io:
                self.bps_config.dump(bps_config_io)
                LOG.info(f"Wrote {bps_config_path}")

        workflow_params = {
            k: getattr(self, k)
            for k in WORKFLOW_KEYWORDS
            if getattr(self, k) is not None
        }
        print(f" workflow params {workflow_params}")
        workflow_path = tmp_dir.joinpath(WORKFLOW_FNAME)
        print(f"workflow_path {workflow_path}")
        with open(workflow_path, "wt") as workflow_io:
            yaml.dump(workflow_params, workflow_io)
            LOG.debug(f"Wrote {workflow_path}")

    @classmethod
    def from_files(cls, tmp_dir, name=None):
        """Load workflow data from files in a directory.

        Parameters
        ----------
        tmp_dir : `pathlib.Path`
            Directory into which files were saved.
        name : `str`
            The name of the workflow (which deterimenes the subdirectory
            of dir from which the workflows is to be read). Defaults to None,
            in which case the workflow is read from dir itself.

        Returns
        -------
        workflow : `Workflow`
            An initialized instance of a campaign.
        """
        tmp_dir = Path(tmp_dir)
        if name is not None:
            tmp_dir = tmp_dir.joinpath(name)

        workflow_path = tmp_dir.joinpath(WORKFLOW_FNAME)
        if workflow_path.exists():
            with open(workflow_path, "rt") as workflow_io:
                workflow_params = yaml.load(workflow_io, yaml.Loader)
                workflow = cls.from_dict(workflow_params)
                LOG.debug(f"Read {workflow_path}")
        return workflow

    def to_jira(self, jira=None, issue_name=None, replace=True):
        """Save workflow data into a jira issue.

        Parameters
        ----------
        jira : `jira.JIRA`,
            The connection to Jira.
        issue_name : `str`, optional
            This issue in which to save workflow data.
            If None, a new issue will be created.
        replace : `bool`
            if True replace values in jira ticket
        Returns
        -------
        issue : `jira.resources.Issue`
            The issue to which the workflow was written.
        """
        if issue_name is None and self.issue_name is not None:
            issue = jira.issue(self.issue_name)
        "if new issue "
        if issue_name is None:
            issue = jira.create_issue(
                project="DRP",
                issuetype="Task",
                summary=f"Workflow {self.name}",
                description=f"Workflow {self.name}",
                components=[{"name": "Test"}],
            )
            LOG.info(f"Created issue {issue}")

        self.issue_name = str(issue)
        wf_dict = self.to_dict()
        with TemporaryDirectory() as staging_dir:
            tmp_dir = Path(staging_dir)
            if self.name is not None:
                tmp_dir = tmp_dir.joinpath(self.name)
            for file_name in ALL_WORKFLOW_FNAMES:
                full_file_path = tmp_dir.joinpath(file_name)
                " Create yaml file with workflow data"
                with open(full_file_pat, 'w') as wf:
                    yaml.dump(wf_dict, wf)
                if full_file_path.exists():
                    for attachment in issue.fields.attachment:
                        if file_name == attachment.filename:
                            if replace:
                                LOG.warning(
                                    f"removing old attachment {file_name} from {issue}"
                                )
                                jira.delete_attachment(attachment.id)
                            else:
                                LOG.warning(
                                    f"{file_name} already exists in {issue}; not saving."
                                )

                    jira.add_attachment(issue, attachment=str(full_file_path))
                    LOG.debug(f"Added {file_name} to {issue}")

        return issue

    @classmethod
    def from_jira(cls, issue_name, jira=None):
        """Load workflow data from a jira issue.


        Parameters
        ----------
        issue_name : `str`
            This issue name from which to load campaign data.
        jira : `jira.JIRA`,
            The connection to Jira.

        Returns
        -------
        workflow : `Workflow`
            An initialized instance of a workflow.
        """
        issue = jira.issue(issue_name) if isinstance(issue_name, str) else issue
        workflow = None
        for attachment in issue.fields.attachment:
            if attachment.filename in ALL_WORKFLOW_FNAMES:
                a_yaml = io.BytesIO(attachment.get()).read()
                workflow_specs = yaml.load(a_yaml, Loader=yaml.Loader)
                workflow = cls.from_dict(workflow_specs)
                workflow.issue_name = str(issue)
        return workflow

    def __str__(self):
        result = f"""{self.__class__.__name__}

bps_dir: {self.bps_dir}
name: {self.name}
bps_name: {self.bps_name}
step_name: {self.step_name}
issue name: {self.issue_name}
step_issue: {self.step_issue}
"""
#        # Strip lead
        return result

# internal functions & classes
