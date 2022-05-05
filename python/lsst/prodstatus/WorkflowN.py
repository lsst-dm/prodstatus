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
ALL_WORKFLOW_FNAMES = (BPS_CONFIG_FNAME, WORKFLOW_FNAME)

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

    def split_by_exposure(self, group_size=None, skip_groups=0, num_groups=None):
        """Split the workflow by exposure number.

        Parameters
        ----------
        group_size : `int` optional
            The approximate size of the group. The default is None, which
            causes the method to return a single workflow with all
            exposures.
        skip_groups : `int` optional
            The number of groups to skip. The default is 0 (no skipped groups).
        num_groups : `int` optional
            The maximum number for groups. The default is None,
            for all groups

        Returns
        -------
        workflows : `List[Workflow]`
            A list of workflows.
        """
        if self.exposures is None:
            raise NoExposuresException
        exp_ids = self.exposures["exp_id"].values

        # If we do not need to split the workflow, just return a list
        # containing only this workflow.
        if group_size is None or not (0 < group_size < len(self.exposures)):
            return [self]

        workflows = []
        base_query = self.bps_config["payload"]["dataQuery"]
        num_subgroups = np.ceil(len(exp_ids) / group_size).astype(int)
        exp_id_subgroups = np.array_split(np.sort(exp_ids), num_subgroups)
        for subgroup_idx, these_exp_ids in enumerate(exp_id_subgroups):
            min_exp_id = min(these_exp_ids)
            max_exp_id = max(these_exp_ids)
            data_query = f"({base_query}) and (exposure >= {min_exp_id}) and (exposure <= {max_exp_id})"
            this_bps_config = self.bps_config.copy()
            this_bps_config.update({"payload": {"dataQuery": data_query}})

            this_band = self.band
            these_exposures = self.exposures.query(
                f"(exp_id >= {min_exp_id}) and (exp_id <= {max_exp_id})"
            ).copy()
            this_workflow = WorkflowN(
                this_bps_config,
                band=this_band,
                exposures=these_exposures,
                step=self.step,
                name=f"{self.name}_{subgroup_idx+1}",
            )
            workflows.append(this_workflow)

        if len(workflows) <= skip_groups:
            return []

        workflows = workflows[skip_groups:]
        if num_groups is not None and len(workflows) > num_groups:
            workflows = workflows[:num_groups]

        return workflows

    def split_by_band(self, bands="ugrizy"):
        """Split the workflow by band.

        Parameters
        ----------
        bands : `Iterable[ str ]`
            The bands by which to divide exposures

        Returns
        -------
        workflows : `List[Workflow]`
            A list of workflows.
        """
        workflows = []
        base_query = self.bps_config["payload"]["dataQuery"]
        for band in bands:
            data_query = f"({base_query}) and (band == '{band}')"
            this_bps_config = self.bps_config.copy()
            this_bps_config.update({"payload": {"dataQuery": data_query}})

            if self.exposures is not None:
                these_exposures = self.exposures.query(f"band=='{band}'").copy()
            else:
                these_exposures = None

            this_workflow = WorkflowN(
                this_bps_config,
                band=band,
                exposures=these_exposures,
                step=self.step,
                name=f"{self.name}_{band}",
            )
            workflows.append(this_workflow)

        return workflows

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
        print(workflow)
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

    def to_jira(self, jira=None, issue=None, replace=True):
        """Save workflow data into a jira issue.

        Parameters
        ----------
        jira : `jira.JIRA`,
            The connection to Jira.
        issue : `jira.resources.Issue`, optional
            This issue in which to save workflow data.
            If None, a new issue will be created.
        replace : `bool`
            if True replace values in jira ticket
        Returns
        -------
        issue : `jira.resources.Issue`
            The issue to which the workflow was written.
        """
        if issue is None and self.issue_name is not None:
            issue = jira.issue(self.issue_name)

        if issue is None:
            issue = jira.create_issue(
                project="DRP",
                issuetype="Task",
                summary=f"Workflow {self.name}",
                description=f"Workflow {self.name}",
                components=[{"name": "Test"}],
            )
            LOG.info(f"Created issue {issue}")

        self.issue_name = str(issue)

        with TemporaryDirectory() as staging_dir:
            self.to_files(staging_dir)

            tmp_dir = Path(staging_dir)
            if self.name is not None:
                tmp_dir = tmp_dir.joinpath(self.name)

            for file_name in ALL_WORKFLOW_FNAMES:
                full_file_path = tmp_dir.joinpath(file_name)
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
    def from_jira(cls, issue, jira=None):
        """Load workflow data from a jira issue.


        Parameters
        ----------
        issue : `jira.resources.Issue`
            This issue from which to load campaign data.
        jira : `jira.JIRA`,
            The connection to Jira.

        Returns
        -------
        workflow : `Workflow`
            An initialized instance of a workflow.
        """
        issue = jira.issue(issue) if isinstance(issue, str) else issue

        with TemporaryDirectory() as staging_dir:
            tmp_dir = Path(staging_dir)
            for attachment in issue.fields.attachment:
                if attachment.filename in ALL_WORKFLOW_FNAMES:
                    file_content = attachment.get()
                    LOG.debug(f"Read {attachment.filename} from {issue}")
                    f_name = tmp_dir.joinpath(attachment.filename)
                    with f_name.open("wb") as file_io:
                        file_io.write(file_content)
                        LOG.debug(f"Wrote {f_name}")

            workflow = cls.from_files(staging_dir)
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
