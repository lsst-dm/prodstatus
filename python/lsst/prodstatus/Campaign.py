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
import dataclasses
import os
from typing import Optional, Mapping
from tempfile import TemporaryDirectory
import contextlib
from pathlib import Path

import yaml


import pandas as pd

from lsst.ctrl.bps import BpsConfig
from lsst.prodstatus.Workflow import Workflow
from lsst.prodstatus import LOG

# constants

CAMPAIGN_KEYWORDS = ("name", "issue_name")
BPS_CONFIG_BASE_FNAME = "bps_config_base.yaml"
CAMPAIGN_SPEC_FNAME = "campaign.yaml"
STEP_METADATA_FNAME = "steps.yaml"
ALL_CAMPAIGN_FNAMES = (BPS_CONFIG_BASE_FNAME, CAMPAIGN_SPEC_FNAME, STEP_METADATA_FNAME)

# exception classes

# interface functions

# classes


@dataclasses.dataclass
class Campaign:
    """API for managing and reporting on data processing campaigns."""

    name: str
    bps_config_base: Optional[BpsConfig] = None
    workflows: Mapping[str, Workflow] = dataclasses.field(default_factory=dict)
    campaign_spec: Optional[dict] = None
    issue_name: Optional[str] = None
    step_issue_names: Mapping[str, str] = dataclasses.field(default_factory=dict)

    @classmethod
    def create_from_yaml(cls, campaign_yaml_path):
        """Create a campaign using parameters read from a file

        Parameters
        ----------
        campaign_yaml_path : `str` or `pathlib.Path`
            File from which to load the yaml

        Returns
        -------
        campaign : `Campaign`
            The new campaign.
        """
        with open(campaign_yaml_path, "rt") as campaign_spec_io:
            campaign_spec = yaml.safe_load(campaign_spec_io)

        name = campaign_spec["name"]
        if "issue_name" in campaign_spec:
            issue_name = campaign_spec["issue_name"]
        else:
            issue_name = None

        base_bps_config = BpsConfig(campaign_spec["bps_config_base"])

        if "steps" in campaign_spec:
            step_specs = campaign_spec["steps"]

            if "exposures" in campaign_spec:
                exposures_path = campaign_spec["exposures"]
                exposures = pd.read_csv(
                    exposures_path, names=["band", "exp_id"], delimiter=r"\s+"
                )
                exposures.sort_values("exp_id", inplace=True)

            all_workflows = Workflow.create_many(
                base_bps_config, step_specs, exposures, base_name=name
            )

        workflows = {}
        for workflow in all_workflows:
            step = workflow.step
            if step not in workflows:
                workflows[step] = []
            workflows[step].append(workflow)

        campaign = cls(name, base_bps_config, workflows, campaign_spec, issue_name)

        return campaign

    def to_files(self, dir):
        """Save workflow data to files in a directory.


        Parameters
        ----------
        dir : `pathlib.Path`
            Directory into which to save files.
        """
        dir = Path(dir)
        if self.name is not None:
            dir = dir.joinpath(self.name)
            dir.mkdir(exist_ok=True)

        bps_config_path = dir.joinpath(BPS_CONFIG_BASE_FNAME)
        with open(bps_config_path, "wt") as bps_config_io:
            self.bps_config_base.dump(bps_config_io)

        campaign_spec_path = dir.joinpath(CAMPAIGN_SPEC_FNAME)
        with open(campaign_spec_path, "wt") as campaign_spec_io:
            yaml.dump(self.campaign_spec, campaign_spec_io, indent=4)

        step_metadata = {}
        for step in self.workflows:
            step_metadata[step] = {"workflows": []}
            if step in self.step_issue_names:
                step_metadata[step]["issue"] = self.step_issue_names[step]
                for workflow in self.workflows[step]:
                    step_metadata[step]["workflows"]["name"] = workflow.name
                    step_metadata[step]["workflows"]["issue"] = workflow.issue
        step_metadata_path = dir.joinpath(STEP_METADATA_FNAME)
        with open(step_metadata_path, "wt") as step_metadata_io:
            yaml.dump(step_metadata, step_metadata_io, indent=4)

        for step_workflows in self.workflows.values():
            for workflow in step_workflows:
                step_dir = dir.joinpath(workflow.step)
                step_dir.mkdir(exist_ok=True)
                workflow.to_files(step_dir)

    @classmethod
    def from_files(cls, dir, name=None):
        """Load workflow data from files in a directory.

        Parameters
        ----------
        dir : `pathlib.Path`
            Directory into which to save files.
        name : `str`
            The name of the campaign (used to determine the subdirectory)

        Returns
        -------
        campaign : `Campaign`
            An initialized instance of a campaign.
        """
        dir = Path(dir)
        if name is not None:
            dir = dir.joinpath(name)

        campaign_spec_path = dir.joinpath(CAMPAIGN_SPEC_FNAME)
        with open(campaign_spec_path, "rt") as campaign_spec_io:
            campaign_spec = yaml.safe_load(campaign_spec_io)

        name = name if name is not None else campaign_spec["name"]
        if "issue_name" in campaign_spec:
            issue_name = campaign_spec["issue_name"]
        else:
            issue_name = None

        bps_config_base_path = dir.joinpath(BPS_CONFIG_BASE_FNAME)
        bps_config_base = BpsConfig(bps_config_base_path)

        step_metadata_path = dir.joinpath(STEP_METADATA_FNAME)
        with open(step_metadata_path, "rt") as step_metadata_io:
            step_metadata = yaml.safe_load(step_metadata_io)

        workflows = {}
        step_issue_names = {}
        for step in step_metadata:
            step_dir = dir.joinpath(step)
            workflows[step] = []
            if "issue" in workflows[step]:
                step_issue_names[step] = workflows[step]["issue"]

            for workflow_elem in step_metadata[step]["workflows"]:
                workflow_name = workflow_elem["name"]
                workflow = Workflow.from_files(step_dir, workflow_name)
                workflows[workflow_name].append(workflow)

        campaign = cls(
            name,
            bps_config_base,
            workflows,
            campaign_spec,
            issue_name,
            step_issue_names,
        )

        return campaign

    def to_jira(self, jira=None, issue=None, replace=False):
        """Save campaign data into a jira issue.

        Parameters
        ----------
        jira : `jira.JIRA`,
            The connection to Jira.
        issue : `jira.resources.Issue`, optional
            This issue in which to save campaign data.
            If None, a new issue will be created.
        replace : `bool`
            Remove existing jira attachments before adding new ones?

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
                summary="a new issue",
                description="A workflow",
                components=[{"name": "Test"}],
            )
            LOG.info(f"Created issue {issue}")

        self.issue_name = str(issue)

        with TemporaryDirectory() as staging_dir:
            self.to_files(staging_dir)

            dir = Path(staging_dir)
            if self.name is not None:
                dir = dir.joinpath(self.name)

            for file_name in ALL_CAMPAIGN_FNAMES:
                full_file_path = dir.joinpath(file_name)
                if full_file_path.exists():
                    for attachment in issue.fields.attachment:
                        if file_name == attachment.filename:
                            if replace:
                                LOG.warning(f"replacing {file_name}")
                                jira.delete_attachment(attachment.id)
                            else:
                                LOG.warning(f"{file_name} already exists; not saving.")

                    jira.add_attachment(issue, attachment=str(full_file_path))

            for step in self.workflows:
                for workflow in self.workflows[step]:
                    # the workflow object supplies the issue name, if it exists
                    # and is known.
                    workflow.to_jira(jira)

        return issue

    @classmethod
    def from_jira(cls, issue):
        """Load campaign data from a jira issue.


        Parameters
        ----------
        issue : `jira.resources.Issue`
            This issue from which to load campaign data.

        Returns
        -------
        campaign : `Campaign`
            An initialized instance of a campaign.
        """
        raise NotImplementedError


# internal functions & classes


@contextlib.contextmanager
def _this_cwd(new_cwd):
    start_dir = os.getcwd()
    try:
        os.chdir(new_cwd)
        yield
    finally:
        os.chdir(start_dir)
