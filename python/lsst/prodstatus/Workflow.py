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
from typing import List, Dict, Optional
from pathlib import Path

import yaml
import numpy as np
import pandas as pd

from lsst.ctrl.bps import BpsConfig


# constants

BPS_CONFIG_FNAME = "bps_config.yaml"
WORKFLOW_FNAME = "workflow.yaml"
WORKFLOW_KEYWORDS = ("name", "step", "band")
EXPLIST_FNAME = "explist.csv"

# exception classes


class NoExposuresException(Exception):
    """Raised when exposures values are needed, but not set."""

    pass


# interface functions

# classes


@dataclass
class Workflow:
    """API for managing and reporting on data processing campaigns.

    Parameters
    ----------
    bps_config : `lsst.control.bps.BpsConfig`
        The BPS configuration for this workflow.
    name : `str`, optional
        An identifier for the workflow.
        The default is None.
    step : `str`, None
        The step thes workflow is a part of.
    band : `str`
        The band processed by this workflow (or 'all')
    exposures : `pandas.DataFrame`, None
        A pd.DataFrame with the following columns:
        ``"band"``
            The filter for the exposure.
        ``"exp_id"``
            The exposures id

    """

    bps_config: BpsConfig
    name: Optional[str] = None
    step: Optional[str] = None
    band: str = "all"
    exposures: Optional[pd.DataFrame] = None

    def split_by_exposure(self, group_size=None, skip_groups=0, num_groups=None):
        """Split the workflow by exposure number.

        Parameters
        ----------
        group_size : `int` optional
            The approximate size of the group. The default is None, which causes
            the method to return a single workflow with all exposures.
        skip_groups : `int` optional
            The number of groups to skip. The default is 0 (no skipped groups).
        num_groups : `int` optional
            The maximum number for groups. The default is None, for all groups

        Returns
        -------
        workflows : `List[Workflow]`
            A list of workflows.
        """
        if self.exposures is None:
            raise NoExposuresException
        exp_ids = self.exposures["exp_id"].values

        # If we do not need to split the workflow, just return a list containing
        # only this workflow.
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
            this_bps_config["payload"]["dataQuery"] = data_query

            this_band = self.band
            these_exposures = self.exposures.query(
                f"(exp_id >= {min_exp_id}) and (exp_id <= {max_exp_id})"
            ).copy()
            this_workflow = Workflow(
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
            this_bps_config["payload"]["dataQuery"] = data_query

            if self.exposures is not None:
                these_exposures = self.exposures.query(f"band=='{band}'").copy()
            else:
                these_exposures = None

            this_workflow = Workflow(
                this_bps_config,
                band=band,
                exposures=these_exposures,
                step=self.step,
                name=f"{self.name}_{band}",
            )
            workflows.append(this_workflow)

        return workflows

    @classmethod
    def create_many(cls, base_bps_config, step_specs, exposures, base_name=""):
        """Create workflows for a set of steps and exposures.

        Parameters
        ----------
        base_bps_config : `lsst.control.bps.BpsConfig`
            BPS configuration for the workflow.
        step_specs : `dict` [`str` `dict`]
            The keys of this dictionaries are the step names.
            The values are themselves dictionaries with keys:
            ``"split_bands"``
                Split the workflows in this step by band? (`bool`)
            ``"exposure_groups"``
                Keyword arguments to Workflow.split_by_exposure (`dict`)
        exposures : `pandas.DataFrame`
            A DataFrame with the following columns:
            ``"band"``
                The filter for the exposure.
            ``"exp_id"``
                The exposures id
        base_name : `str`
            The base for the name of the workflows.
        """

        # Make a list of workflows, where each workflow completes a step
        step_workflows = []
        for step, step_spec in step_specs.items():
            bps_config = base_bps_config.copy()
            bps_config["pipelineYaml"] = f"{bps_config['pipelineYaml']}#{step}"
            workflow = cls(
                bps_config, exposures=exposures, step=step, name=f"{base_name}_{step}"
            )
            step_workflows.append(workflow)

        # Build a list of workflows split up by band when requested
        step_band_workflows = []
        for workflow in step_workflows:
            step_spec = step_specs[workflow.step]
            if step_spec["split_bands"]:
                workflows = workflow.split_by_band()
                step_band_workflows.extend(workflows)
            else:
                step_band_workflows.append(workflow)

        # build a list of workflows split by groups of exposures when requested
        split_workflows = []
        for workflow in step_band_workflows:
            step_spec = step_specs[workflow.step]
            if "exposure_groups" in step_spec:
                split_by_exposure_kwargs = step_spec["exposure_groups"]
                workflows = workflow.split_by_exposure(**split_by_exposure_kwargs)
                split_workflows.extend(workflows)
            else:
                split_workflows.append(workflow)

        return split_workflows

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
        if self.name is not None:
            dir = dir.joinpath(self.name)
            dir.mkdir(exist_ok=True)

        bps_config_path = dir.joinpath(BPS_CONFIG_FNAME)
        with open(bps_config_path, "wt") as bps_config_io:
            self.bps_config.dump(bps_config_io)

        workflow_params = {
            k: getattr(self, k)
            for k in WORKFLOW_KEYWORDS
            if getattr(self, k) is not None
        }
        workflow_path = dir.joinpath(WORKFLOW_FNAME)
        with open(workflow_path, "wt") as workflow_io:
            yaml.dump(workflow_params, workflow_io)

        if self.exposures is not None:
            explist_path = dir.joinpath(EXPLIST_FNAME)
            self.exposures.to_csv(explist_path, header=False, index=False, sep=" ")

    @classmethod
    def from_files(cls, dir, name=None):
        """Load workflow data from files in a directory.

        Parameters
        ----------
        dir : `pathlib.Path`
            Directory into which to save files.
        name : `str`
            The name of the workflow (which deterimenes the subdirectory
            of dir from which the workflows is to be read). Defaults to None,
            in which case the workflow is read from dir itself.

        Returns
        -------
        workflow : `Workflow`
            An initialized instance of a campaign.
        """
        dir = Path(dir)
        if name is not None:
            dir = dir.joinpath(name)

        bps_config_path = dir.joinpath(BPS_CONFIG_FNAME)
        bps_config = BpsConfig(bps_config_path)
        workflow = cls(bps_config)

        workflow_path = dir.joinpath(WORKFLOW_FNAME)
        if workflow_path.exists():
            with open(workflow_path, "rt") as workflow_io:
                workflow_params = yaml.load(workflow_io, yaml.Loader)

            for keyword in WORKFLOW_KEYWORDS:
                if keyword in workflow_params:
                    setattr(workflow, keyword, workflow_params[keyword])

        explist_path = dir.joinpath(EXPLIST_FNAME)
        if explist_path.exists():
            workflow.exposures = pd.read_csv(
                explist_path, names=["band", "exp_id"], delimiter=r"\s+"
            )
            workflow.exposures.sort_values("exp_id", inplace=True)

        return workflow

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
        If issue is None, jira must not be None.
        """
        raise NotImplementedError

    @classmethod
    def from_jira(cls, issue):
        """Load workflow data from a jira issue.


        Parameters
        ----------
        issue : `jira.resources.Issue`
            This issue from which to load campaign data.

        Returns
        -------
        workflow : `Workflow`
            An initialized instance of a workflow.
        """
        raise NotImplementedError


# internal functions & classes
