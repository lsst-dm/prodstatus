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
from typing import List, Dict, Optional
import contextlib

import yaml


import pandas as pd

from lsst.ctrl.bps import BpsConfig
from lsst.prodstatus.Workflow import Workflow

# constants

# exception classes

# interface functions

# classes


@dataclasses.dataclass
class Campaign:
    """API for managing and reporting on data processing campaigns."""

    name: str
    bps_config_base: Optional[BpsConfig] = None
    workflows: List[Workflow] = dataclasses.field(default_factory=list)
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

            workflows = Workflow.create_many(
                base_bps_config, step_specs, exposures, base_name=name
            )
        else:
            workflows = []

        campaign = cls(name, base_bps_config, workflows, campaign_spec)

        return campaign

    def to_files(self, dir):
        """Save workflow data to files in a directory.


        Parameters
        ----------
        dir : `pathlib.Path`
            Directory into which to save files.
        """
        raise NotImplementedError

    @classmethod
    def from_files(cls, dir):
        """Load workflow data from files in a directory.

        Parameters
        ----------
        dir : `pathlib.Path`
            Directory into which to save files.

        Returns
        -------
        workflow : `Workflow`
            An initialized instance of a campaign.
        """
        raise NotImplementedError

    def to_jira(self, issue, jira=None):
        """Save workflow data into a jira issue.

        Parameters
        ----------
        issue : `jira.resources.Issue`, optional
            This issue in which to save campaign data.
        jira : `jira.JIRA`, optional
            The connection to Jira. The default is None.
            If create is true, jira must not be None.

        Returns
        -------
        None.

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
