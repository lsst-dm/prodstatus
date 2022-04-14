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
from typing import Optional, List
from tempfile import TemporaryDirectory
import contextlib
from pathlib import Path
from copy import deepcopy

import yaml
import pandas as pd

from lsst.ctrl.bps import BpsConfig
from lsst.prodstatus.Step import Step
from lsst.prodstatus import LOG
from lsst.prodstatus.Workflow import EXPLIST_FNAME

# constants

CAMPAIGN_KEYWORDS = ("name", "issue_name")
CAMPAIGN_SPEC_FNAME = "campaign.yaml"
ALL_CAMPAIGN_FNAMES = (CAMPAIGN_SPEC_FNAME, EXPLIST_FNAME)

# exception classes

# interface functions

# classes


@dataclasses.dataclass
class Campaign:
    """API for managing and reporting on data processing campaigns."""

    name: str
    steps: List[Step] = dataclasses.field(default_factory=list)
    exposures: Optional[pd.DataFrame] = None
    issue_name: Optional[str] = None

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

        steps = []
        if "steps" in campaign_spec:
            if "exposures" in campaign_spec:
                exposures_path = campaign_spec["exposures"]
                exposures = pd.read_csv(
                    exposures_path, names=["band", "exp_id"], delimiter=r"\s+"
                )
                exposures.sort_values("exp_id", inplace=True)
            else:
                exposures = None

            for step_name, step_specs in campaign_spec["steps"].items():
                step_workflow_base_name = f"{name}"
                base_bps_config = BpsConfig(step_specs["base_bps_config"])

                # spec_kwargs should be the same as step_specs, except
                # that the filename of the BPS config file is replaced
                # by the BpsConfig instance.
                step_spec_kwargs = deepcopy(step_specs)
                step_spec_kwargs["base_bps_config"] = base_bps_config
                step = Step.generate_new(
                    step_name,
                    exposures=exposures,
                    workflow_base_name=step_workflow_base_name,
                    **step_spec_kwargs,
                )
                steps.append(step)

        campaign = cls(name, steps, exposures, issue_name)

        return campaign

    def to_files(self, dir):
        """Save campaign data to files in a directory.


        Parameters
        ----------
        dir : `pathlib.Path`
            Directory into which to save files.
        """
        dir = Path(dir)
        if self.name is not None:
            dir = dir.joinpath(self.name)
            dir.mkdir(exist_ok=True)

        campaign_spec = {
            "name": self.name,
            "steps": {
                s.name: {
                    "issue": s.issue_name,
                    "split_bands": s.split_bands,
                    "exposure_groups": s.exposure_groups,
                }
                for s in self.steps
            },
        }

        if self.issue_name is not None:
            campaign_spec["issue"] = self.issue_name

        campaign_spec_path = dir.joinpath(CAMPAIGN_SPEC_FNAME)
        with open(campaign_spec_path, "wt") as campaign_spec_io:
            yaml.dump(campaign_spec, campaign_spec_io, indent=4)
            LOG.debug(f"Wrote {campaign_spec_path}")

        steps_path = dir.joinpath("steps")
        steps_path.mkdir(exist_ok=True)
        for step in self.steps:
            step.to_files(steps_path)

        if self.exposures is not None:
            explist_path = dir.joinpath(EXPLIST_FNAME)
            self.exposures.to_csv(explist_path, header=False, index=False, sep=" ")
            LOG.debug(f"Wrote {explist_path}")

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
            LOG.debug(f"Read {campaign_spec_path}")

        name = name if name is not None else campaign_spec["name"]
        if "issue_name" in campaign_spec:
            issue_name = campaign_spec["issue_name"]
        else:
            issue_name = None

        steps_path = dir.joinpath("steps")

        steps = []
        for step_name in campaign_spec["steps"]:
            step = Step.from_files(steps_path, name=step_name)
            steps.append(step)

        explist_path = dir.joinpath(EXPLIST_FNAME)
        if explist_path.exists():
            exposures = pd.read_csv(
                explist_path, names=["band", "exp_id"], delimiter=r"\s+"
            )
            LOG.debug(f"Read {explist_path}")
            exposures.sort_values("exp_id", inplace=True)
        else:
            exposures = None

        campaign = cls(name, steps, exposures, issue_name)

        return campaign

    def to_jira(self, jira=None, issue=None, replace=False, cascade=False):
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
        cascade : `bool`
            Write dependent issues (steps and workflows) as well?

        Returns
        -------
        issue : `jira.resources.Issue`
            The issue to which the workflow was written.
        """
        # raise NotImplementedError("This code is untested")
        if issue is None and self.issue_name is not None:
            issue = jira.issue(self.issue_name)

        if issue is None:
            issue = jira.create_issue(
                project="DRP",
                issuetype="Task",
                summary=f"Campaign {self.name}",
                description=f"Campaign {self.name}",
                components=[{"name": "Test"}],
            )
            LOG.info(f"Created issue {issue}")

        self.issue_name = str(issue)

        with TemporaryDirectory() as staging_dir:

            # Write dependent issues first, so references to them can be
            # written to the campaing issue itself later.
            if cascade:
                for step in self.steps:
                    if step.issue_name is not None:
                        step_issue = jira.issue(step.issue_name)
                    else:
                        step_issue = None

                    step.to_jira(jira, step_issue, replace=replace, cascade=cascade)

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
    def from_jira(cls, issue, jira):
        """Load campaign data from a jira issue.


        Parameters
        ----------
        issue : `jira.resources.Issue`
            This issue from which to load campaign data.
        jira : `jira.JIRA`,
            The connection to Jira.

        Returns
        -------
        campaign : `Campaign`
            An initialized instance of a campaign.
        """
        # raise NotImplementedError("This code is untested")
        issue = jira.issue(issue) if isinstance(issue, str) else issue

        with TemporaryDirectory() as staging_dir:
            dir = Path(staging_dir)
            for attachment in issue.fields.attachment:
                if attachment.filename in ALL_CAMPAIGN_FNAMES:
                    file_content = attachment.get()
                    LOG.debug(f"Read {attachment.filename} from {issue}")
                    fname = dir.joinpath(attachment.filename)
                    with fname.open("wb") as file_io:
                        file_io.write(file_content)
                        LOG.debug(f"Wrote {fname}")

            campaign_spec_path = dir.joinpath(CAMPAIGN_SPEC_FNAME)
            with campaign_spec_path.open("rt") as file_io:
                campaign_spec = yaml.safe_load(file_io)
                LOG.debug(f"Read {campaign_spec_path}")

            step_path = dir.joinpath("steps")
            step_path.mkdir(exist_ok=True)
            for step_name, step_spec in campaign_spec["steps"].items():
                if "issue" in step_spec and step_spec["issue"] is not None:
                    step_issue_name = step_spec["issue"]
                    step_issue = jira.issue(step_issue_name)
                    step = Step.from_jira(step_issue, jira)
                    step.to_files(step_path)
                else:
                    LOG.warning(f"Could not load {step_name} from jira (no issue name)")

            campaign = cls.from_files(staging_dir)
            campaign.issue_name = str(issue)

        return campaign

    def __str__(self):
        output = f"""{self.__class__.__name__}
name: {self.name}
issue name: {self.issue_name}
steps:"""

        for step in self.steps:
            output += f"\n - {step.name} (issue {step.issue_name}) with {len(step.workflows)} workflows"

        return output


# internal functions & classes


@contextlib.contextmanager
def _this_cwd(new_cwd):
    start_dir = os.getcwd()
    try:
        os.chdir(new_cwd)
        yield
    finally:
        os.chdir(start_dir)
