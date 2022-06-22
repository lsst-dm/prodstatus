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
import io
from typing import Optional, List
from tempfile import TemporaryDirectory
import contextlib
from pathlib import Path

import yaml

from lsst.prodstatus.StepN import StepN
from lsst.prodstatus import LOG

# constants

CAMPAIGN_KEYWORDS = ("name", "issue", "steps")
CAMPAIGN_SPEC_FNAME = "campaign.yaml"
ALL_CAMPAIGN_FNAMES = [CAMPAIGN_SPEC_FNAME]

# exception classes

# interface functions

# classes


@dataclasses.dataclass
class CampaignN:
    """API for managing and reporting on data processing campaigns."""

    name: str
    issue: Optional[str] = None
    steps: List[StepN] = dataclasses.field(default_factory=list)

    @classmethod
    def generate_new(
            cls,
            name,
            issue=None,
            steps=None,
    ):
        """Generate a new campaign.

        Parameters
        ----------
        name : `str`
            The name of the step.
        issue: `str`
            jira ticket issue for given campaign is exists
        steps : `list`
            a list of steps

        Returns
        -------
        step : `CampaignN`
            A new campaign with step instances.
        """
        campaign = cls(name, issue, steps)
        campaign.steps = list()
        return campaign

    @classmethod
    def create_from_yaml(cls, campaign_yaml_path, jira):
        """Create a campaign using parameters read from a file

        Parameters
        ----------
        campaign_yaml_path : `str` or `pathlib.Path`
            File from which to load the yaml
        jira : `jira instance`
            The jira instance to save generated steps to jira

        Returns
        -------
        campaign : `Campaign`
            The new campaign.
        """
        with open(campaign_yaml_path, "rt") as campaign_spec_io:
            campaign_spec = yaml.safe_load(campaign_spec_io)

        name = campaign_spec["name"]
        if "issue" in campaign_spec:
            issue_name = campaign_spec["issue"]
        else:
            issue_name = None
        campaign_issue = issue_name
        steps = list()
        if "steps" in campaign_spec:

            for step_specs in campaign_spec["steps"]:
                name = step_specs["name"]
                step_specs["campaign_issue"] = campaign_issue   # campaign issue
                step = StepN.from_dict(step_specs)
                step_issue = step_specs["issue_name"]
                step_specs['issue_name'] = step.to_jira(jira, step_issue)
                steps.append(step_specs)
        LOG.info(f"Campaign specs {campaign_spec}")
        campaign = cls(name, issue_name, steps)

        return campaign

    @classmethod
    def from_dict(cls, campaign_spec, jira):
        """Create a campaign using parameters read from a file

        Parameters
        ----------
        campaign_spec : `dict` o
            A dictionary with  campaign parameters
        jira : `jira.JIRA`
            A jira instance used to save campaign steps to jira
        Returns
        -------
        campaign : `Campaign`
            The new campaign.
        """
        name = campaign_spec["name"]
        if "issue" in campaign_spec:
            issue_name = campaign_spec["issue"]
        else:
            issue_name = None
        campaign_issue = issue_name
        steps = list()
        if "steps" in campaign_spec:
            for step_specs in campaign_spec["steps"]:
                step = StepN.from_dict(step_specs)
                step_issue = step_specs["issue_name"]
                step_specs["campaign_issue"] = campaign_issue
                if jira is not None:
                    step_specs['issue_name'] = step.to_jira(jira, step_issue)
                steps.append(step_specs)

        campaign = cls(name, issue_name, steps)
        return campaign

    def to_dict(self):
        """ Create campaign spec dictionary
        Parameters
        ----------

        Returns
        -------
        campaign_spec : `dict`
            The new campaign_spec dictionary.
        """
        campaign_spec = dict()
        campaign_spec["name"] = self.name
        campaign_spec["issue"] = self.issue
        campaign_spec["steps"] = self.steps
        return campaign_spec

    def to_files(self, temp_dir):
        """Save campaign data to files in a directory.


        Parameters
        ----------
        temp_dir : `str`
            Directory into which to save files.
        """
        t_dir = Path(temp_dir)
        if self.name is not None:
            t_dir = t_dir.joinpath(self.name)
            t_dir.mkdir(exist_ok=True)
        step_list = list()
        for s in self.steps:
            _issue = s['issue_name']
            _name = s['name']
            _split = s['split_bands']
            _workdir = s['workflow_base']
            step_specs = dict()
            step_specs['issue_name'] = _issue
            step_specs['name'] = _name
            step_specs['split_bands'] = _split
            step_specs['workflow_base'] = _workdir
            step_list.append(step_specs)
        campaign_spec = {
            "name": self.name,
            "issue": self.issue,
            "steps": step_list
        }

        if self.issue is not None:
            campaign_spec["issue"] = self.issue

        campaign_spec_path = t_dir.joinpath(CAMPAIGN_SPEC_FNAME)
        with open(campaign_spec_path, "wt") as campaign_spec_io:
            yaml.dump(campaign_spec, campaign_spec_io, indent=4)
            LOG.debug(f"Wrote {campaign_spec_path}")

    @classmethod
    def from_files(cls, temp_dir, name=None, jira=None):
        """Load workflow data from files in a directory.

        Parameters
        ----------
        temp_dir : `pathlib.Path`
            Directory into which to save files.
        name : `str`
            The name of the campaign (used to determine the subdirectory)
        jira : `jira.JIRA`
            A jira instance, used to save campaign steps to jira
        Returns
        -------
        campaign : `Campaign`
            An initialized instance of a campaign.
        """
        t_dir = Path(temp_dir)
        if name is not None:
            t_dir = t_dir.joinpath(name)

        campaign_spec_path = t_dir.joinpath(CAMPAIGN_SPEC_FNAME)
        if not os.path.exists(campaign_spec_path):
            LOG.info(f"The file {campaign_spec_path} do not exists")
            return None
        with open(campaign_spec_path, "rt") as campaign_spec_io:
            campaign_spec = yaml.safe_load(campaign_spec_io)
            LOG.debug(f"Read {campaign_spec_path}")
        campaign = cls.from_dict(campaign_spec, jira)
        return campaign

    def to_jira(self, jira=None, issue_name=None, replace=True, cascade=False):
        """Save campaign data into a jira issue.
        Parameters
        ----------
        jira : `jira.JIRA`,
            The connection to Jira.
        issue_name : `str`,
            This issue in which to save/update campaign data.
            If None, a new issue will be created.
        replace : `bool`
            Remove existing jira attachments before adding new ones?
        cascade : `bool`
            Write dependent issues (steps ) as well?

        Returns
        -------
        issue_name : `str`
            The issue name to which the workflow was written.
        """
        # raise NotImplementedError("This code is untested")
        if issue_name is None and self.issue is not None:
            issue = jira.issue(self.issue)
        else:
            issue = jira.issue(issue_name)
        " Create an issue if not exists "
        if issue is None:
            issue = jira.create_issue(
                project="DRP",
                issuetype="Task",
                summary=f"Campaign {self.name}",
                description=f"Campaign {self.name}",
                components=[{"name": "Test"}],
            )
            print(f"Created issue {issue}")
        "if issue is created "
        if issue is not None:
            self.issue = str(issue)
            LOG.info(f"Issue name {self.issue}")
        " Now create yaml file with campaign data "
        with TemporaryDirectory() as staging_dir:
            """ Write dependent issues first, so references to them can be
             written to the campaign issue itself later. """
            if cascade:
                for step_specs in self.steps:
                    step = StepN.from_dict(step_specs)
                    if step_specs["campaign_issue"] is None:
                        step_specs["campaign_issue"] = self.issue
                    if step_specs["issue_name"] is not None:
                        step_issue = step_specs["issue_name"]
                    else:
                        step_issue = step.to_jira(jira, None)
                        step_specs["issue_name"] = step_issue
                    step.to_jira(jira, step_issue, replace=replace, cascade=cascade)
            " Now steps are created or updated make new yaml file"
            s_dir = Path(staging_dir)
            campaign_file = s_dir.joinpath("campaign.yaml")
            LOG.info(f"Creating campaign yaml {campaign_file}")
            campaign_spec = self.to_dict()
            with open(campaign_file, 'w') as cf:
                yaml.dump(campaign_spec, cf)
            "Now write the yaml as an attachment "
            for file_name in ALL_CAMPAIGN_FNAMES:
                full_file_path = s_dir.joinpath(file_name)
                if full_file_path.exists():
                    for attachment in issue.fields.attachment:
                        if file_name == attachment.filename:
                            " should we replace the attachment?"
                            if replace:
                                LOG.warning(
                                    f"removing old attachment {file_name} from {issue}"
                                )
                                jira.delete_attachment(attachment.id)
                            else:
                                LOG.warning(
                                    f"{file_name} already exists in {issue}; not saving."
                                )
                    LOG.info(f"attachment file {full_file_path}")
                    jira.add_attachment(issue, attachment=str(full_file_path))
                    LOG.debug(f"Added {file_name} to {issue}")

        return str(issue)

    @classmethod
    def from_jira(cls, issue_name, jira):
        """Load campaign data from a jira issue.


        Parameters
        ----------
        issue_name : `str`
            This issue name from which to load campaign data.
        jira : `jira.JIRA`,
            The connection to Jira.

        Returns
        -------
        campaign : `Campaign`
            An initialized instance of a campaign.
        """
        #
        issue = jira.issue(issue_name)
        campaign = None
        for attachment in issue.fields.attachment:
            att_file = attachment.filename
            if att_file == "campaign.yaml":
                a_yaml = io.BytesIO(attachment.get()).read()
                campaign_spec = yaml.load(a_yaml, Loader=yaml.Loader)
                LOG.info("Read yaml specs")
                campaign = cls.from_dict(campaign_spec, jira)
                campaign.issue_name = str(issue)
                return campaign
            else:
                campaign = None
        return campaign

    def __str__(self):
        output = f"""{self.__class__.__name__}
name: {self.name}
issue name: {self.issue}
steps:"""

        for step in self.steps:
            output += f"\n - {step['name']} (issue {step['issue_name']})" \
                      f" with workflows from {step['workflow_base']} "
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
