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
from typing import List, Dict, Optional, Mapping
import contextlib
import csv
from pathlib import Path

import yaml


import pandas as pd

from lsst.ctrl.bps import BpsConfig
from lsst.prodstatus.Workflow import Workflow

# constants

CAMPAIGN_KEYWORDS = ("name",)
BPS_CONFIG_BASE_FNAME = "bps_config_base.yaml"
CAMPAIGN_SPEC_FNAME = "campaign.yaml"
WORKFLOW_NAMES_FNAME = "workflow_names.txt"

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

        campaign = cls(name, base_bps_config, workflows, campaign_spec)

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
            yaml.dump(self.campaign_spec, campaign_spec_io)

        workflow_names_path = dir.joinpath(WORKFLOW_NAMES_FNAME)
        with workflow_names_path.open("wt") as workflow_names_io:
            for step in self.workflows:
                for workflow in self.workflows[step]:
                    workflow_names_io.write(f"{step} {workflow.name}\n")

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

        bps_config_base_path = dir.joinpath(BPS_CONFIG_BASE_FNAME)
        bps_config_base = BpsConfig(bps_config_base_path)

        # Find the steps and workflows, so we know what directory
        # to read the workflows from.
        workflow_names_path = dir.joinpath(WORKFLOW_NAMES_FNAME)
        with workflow_names_path.open("rt") as workflow_names_io:
            step_workflow_reader = csv.reader(workflow_names_io, delimiter=" ")

            # Load the file into a list of tuples,
            # where each tuple is a step, workflow name pair.
            step_wfname_pairs = [sw for sw in step_workflow_reader]

        # Load the workflows
        workflows = {}
        for step, workflow_name in step_wfname_pairs:
            step_dir = dir.joinpath(step)
            if workflow_name not in workflows:
                workflows[workflow_name] = []
            workflow = Workflow.from_files(step_dir, workflow_name)
            workflows[workflow_name].append(workflow)

        campaign = cls(name, bps_config_base, workflows, campaign_spec)

        return campaign

    def to_jira(self, issue, jira=None):
        """Save workflow data into a jira issue.

        Parameters
        ----------
        issue : `jira.resources.Issue`, optional
            This issue in which to save campaign data.
        jira : `jira.JIRA`, optional
            The connection to Jira. The default is None.
            If create is true, jira must not be None.

        Note
        ----
        If issue is None, jira must not be none.
        """
        assert (jira is not None) or (issue is not None)
        raise NotImplementedError

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
