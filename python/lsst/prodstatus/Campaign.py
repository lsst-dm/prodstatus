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
from typing import List, Dict, Optional

import pandas as pd

from lsst.ctrl.bps import BpsConfig
from prodstatus.Workflow import Workflow

# constants

# exception classes

# interface functions

# classes


@dataclass
class Campaign:
    """API for managing and reporting on data processing campaigns."""

    campaign_id: str
    bps_config_base: Optional[BpsConfig] = None
    workflows: List[Workflow] = []

    @classmethod
    def create(cls, campaign_spec_path):
        """Create a workflow, creating a jira or storage directory if necessary.

        Parameters
        ----------
        campaign_spec_path: `str`, `pathlib.Path`
            Path from which to load campaign specifications.

        Returns
        -------
        campaign : `lsst.prodstatus.Campaign`
            The instantiated campaign.

        """
        with open(campaign_spec_path, "rt") as campaign_spec_in:
            campaign_spec = yaml.safe_load(campaign_spec_io)

        campaign_id = campaign_spec["campaign_id"]

        bps_config_base = BpsConfig(campaign_spec["bps_config"])
        campaign = cls(campaign_spec, bps_config_base, id)

        exposures_path = campaign_spec["exposures"]
        exposures = pd.read_csv(
            exposures_path, names=["band", "exp_id"], delimiter=r"\s+"
        )
        exposures.sort_values("exp_id", inplace=True)

        bps_config = bps_config_base.copy()

        campaign = cls(campaign_id, bps_config_base, [])

        if "steps" in campaign_spec:
            self.workflows = Workflows.create_many(
                bps_config_base, campaign_spec["steps"], exposures
            )

        return campaign

    def to_files(self, dir):
        """Save workflow data to files in a directory.


        Parameters
        ----------
        dir : `pathlib.Path`
            Directory into which to save files.

        Returns
        -------
        None.
        """
        dir = Path(dir)

        bps_config_base_path = dir.joinpath(bps_config_base_NAME)
        with open(bps_config_base_path, "wt") as bps_config_base_io:
            self.bps_config_base.dump(bps_config_base_io)

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
        dir = Path(dir)

        id = dir.name

        bps_config_base_path = dir.joinpath(bps_config_base_NAME)
        bps_config_base = BpsConfig(bps_config_base_path)
        campaign = cls(bps_config_base, id)
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
