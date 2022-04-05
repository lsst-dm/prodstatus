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
from typing import Mapping, List, Optional
from pathlib import Path
from tempfile import TemporaryDirectory
import yaml
import pandas as pd
import re

from lsst.prodstatus.Workflow import Workflow
from lsst.prodstatus import LOG
from lsst.prodstatus.Workflow import EXPLIST_FNAME

STEP_SPEC_FNAME = "step.yaml"
ALL_STEP_FNAMES = (STEP_SPEC_FNAME, EXPLIST_FNAME)


@dataclasses.dataclass
class Step:
    """API for managing and reporting on data processing campaigns.

    Parameters
    ----------
    name : `str`
        An identifier for the step.
    split_bands : `bool`
        Workflows in the step are split by band?
    exposure_groups : `dict`
        Parameterns for splitting workflows in the step by
        exposure number.
        (Keyword arguments to Workflow.split_by_exposure.)
    workflows : `list` ['lsst.prodstatus.Workflow']
        A list of workflows in the step.
    issue_name : `str`
        The name of the jira issue that tracks this step.

    """

    name: str
    split_bands: bool = False
    exposure_groups: Optional[Mapping[str, int]] = None
    workflows: List[Workflow] = dataclasses.field(default_factory=list)
    issue_name: Optional[str] = None
    exposures: Optional[pd.DataFrame] = None

    @classmethod
    def generate_new(
        cls,
        name,
        base_bps_config,
        split_bands,
        exposure_groups,
        exposures,
        workflow_base_name="",
    ):
        """Generate a new step, constructing its workflows.

        Parameters
        ----------
        name : `str`
            The name of the step.
        base_bps_config : `lsst.control.bps.BpsConfig`
            BPS configuration for the workflow.
        split_bands : `bool`
            Split workflows by band.
        exposure_groups : `dict`
            Keyword arguments to Workflow.split_by_exposure.
        exposures : `pandas.DataFrame`
            A DataFrame with the following columns:
            ``"band"``
                The filter for the exposure.
            ``"exp_id"``
                The exposures id
        base_name : `str`
            The base for the name of the workflows.

        Returns
        -------
        step : `Step`
            A new step with workflow instances.
        """
        step = cls(name, split_bands, exposure_groups)
        step._generate_workflows(base_bps_config, exposures, workflow_base_name)
        return step

    def _generate_workflows(
        self, base_bps_config, exposures, base_name, drop_empty=True
    ):
        """Generate the workflows for this step.

        Parameters
        ----------
        base_bps_config : `lsst.control.bps.BpsConfig`
            BPS configuration for the workflow.
        exposures : `pandas.DataFrame`
            A DataFrame with the following columns:
            ``"band"``
                The filter for the exposure.
            ``"exp_id"``
                The exposures id
        base_name : `str`
            The base for the names of the workflows.
        drop_empty : `bool`
            Exclude workflows with no assigned exposures.
        """

        # Begin by making one workflows with does everything in this step.
        bps_config = base_bps_config.copy()
        bps_config["pipelineYaml"] = f"{bps_config['pipelineYaml']}#{self.name}"
        monolithic_workflow = Workflow(
            bps_config,
            exposures=exposures,
            step=self.name,
            name=f"{base_name}_{self.name}",
        )

        # If this step is to split the workflows by band, do so
        if self.split_bands:
            band_workflows = monolithic_workflow.split_by_band()
        else:
            band_workflows = [monolithic_workflow]

        # If this step is te split the workflows by exposure number, go through
        # each workflow (already split by band if requested) and spit it
        # further by exposure id, and add them to the instances list of
        # workflows.
        if self.exposure_groups is not None:
            for workflow in band_workflows:
                split_workflows = workflow.split_by_exposure(**self.exposure_groups)
                for workflow in split_workflows:
                    if len(workflow.exposures) > 0 or not drop_empty:
                        self.workflows.append(workflow)
        else:
            if drop_empty and exposures is not None:
                for workflow in band_workflows:
                    if len(workflow.exposures) > 0:
                        self.workflows.append(workflow)
            else:
                self.workflows.extend(band_workflows)

    def wflist_to_table(in_dict,sorton):

        table_out="|Name| Issue|\n"
        print("indict:",in_dict)
        lendict=len(in_dict)
        for i in range(0,lendict):
          elem=in_dict[i]
          table_out += "|"+elem['name']+"|"+elem['issue_name']+"|\n"

        return table_out

    def to_files(self, dir):
        """Save step data to files in a directory.


        Parameters
        ----------
        dir : `pathlib.Path`
            Directory into which to save files.
        """
        dir = Path(dir)
        if self.name is not None:
            dir = dir.joinpath(self.name)
            dir.mkdir(exist_ok=True)

        print("in to files",self.workflows)
        for w in self.workflows:
          print(w['name'],w['issue_name'])
        step_spec = {
            "name": self.name,
            "split_bands": self.split_bands,
            #"workflows": [
                #{"name": w.name, "issue": w.issue_name} for w in self.workflows
            #],
            "workflows": [
                {"name": w['name'], "issue": w['issue_name']} for w in self.workflows
            ],
        }

        if self.exposure_groups is not None:
            step_spec["exposure_groups"] = self.exposure_groups

        if self.issue_name is not None:
            step_spec["issue"] = self.issue_name

        step_spec_path = dir.joinpath(STEP_SPEC_FNAME)
        with open(step_spec_path, "wt") as step_spec_io:
            yaml.dump(step_spec, step_spec_io, indent=4)
            LOG.debug(f"Wrote {step_spec_path}")

        if self.exposures is not None:
            explist_path = dir.joinpath(EXPLIST_FNAME)
            self.exposures.to_csv(explist_path, header=False, index=False, sep=" ")
            LOG.debug(f"Wrote {explist_path}")

        #workflows_path = dir.joinpath("workflows")
        #workflows_path.mkdir(exist_ok=True)
        #for workflow in self.workflows:
            #workflow.to_files(workflows_path)

    @classmethod
    def from_files(cls, dir, name=None, load_workflows=True):
        """Load workflow data from files in a directory.

        Parameters
        ----------
        dir : `pathlib.Path`
            Directory into which to save files.
        name : `str`
            The name of the campaign (used to determine the subdirectory)
        load_workflows : `bool`
            Load the workflows themselves?

        Returns
        -------
        campaign : `Campaign`
            An initialized instance of a campaign.
        """
        dir = Path(dir)
        if name is not None:
            dir = dir.joinpath(name)

        step_spec_path = dir.joinpath(STEP_SPEC_FNAME)
        with open(step_spec_path, "rt") as step_spec_io:
            step_spec = yaml.safe_load(step_spec_io)
            LOG.debug(f"Read {step_spec_path}")

        name = name if name is not None else step_spec["name"]
        split_bands = step_spec["split_bands"]

        if "exposure_groups" in step_spec:
            exposure_groups = step_spec["exposure_groups"]
        else:
            exposure_groups = {}

        if "issue" in step_spec:
            issue_name = step_spec["issue"]
        else:
            issue_name = None

        step = cls(name, split_bands, exposure_groups, [], issue_name)
        workflows_path = dir.joinpath("workflows")
        for workflow_spec in step_spec["workflows"]:
            workflow = Workflow.from_files(workflows_path, name=workflow_spec["name"])
            step.workflows.append(workflow)

        explist_path = dir.joinpath(EXPLIST_FNAME)
        if explist_path.exists():
            step.exposures = pd.read_csv(
                explist_path, names=["band", "exp_id"], delimiter=r"\s+"
            )
            LOG.debug(f"Read {explist_path}")
            step.exposures.sort_values("exp_id", inplace=True)

        return step

    def to_jira(self, jira=None, issue=None, replace=False, cascade=False):
        """Save step data into a jira issue.

        Parameters
        ----------
        jira : `jira.JIRA`,
            The connection to Jira.
        issue : `jira.resources.Issue`, optional
            This issue in which to save step data.
            If None, a new issue will be created.
        replace : `bool`
            Remove existing jira attachments before adding new ones?
        cascade : `bool`
            Write dependent issues (workflows) as well?

        Returns
        -------
        issue : `jira.resources.Issue`
            The issue to which the workflow was written.
        """
        # raise NotImplementedError("This code is untested")
        print("issue",issue,self.issue_name,"wf:",self.workflows)
        #if issue is None and self.issue_name is not None:
        wf=self.workflows
        print(type(wf))
        print("wf:",wf)
        tableout="|Name| Issue|WF|\n"
        for i in range(0,len(wf)):
          elem=wf[i]
          wfissue=jira.issue(elem['issue_name'])
          wfdesc=str(wfissue.raw['fields']['description'])
          dlines=iter(wfdesc.splitlines())
          pattern=re.compile('bps_submit_yaml_file:(.*).yaml')
          pattern2=re.compile('uniqProcName:(.*)')
          bpsyamlname=elem['name']
          uniqProcName='place'
          for aline in dlines:
           m=re.match(pattern,aline)
           if m:
            bpsyamlname=m.group(1)
           print(aline)
           m=re.match(pattern2,aline)
           if m:
            print("match")
            uniqProcName=m.group(1)
            
          tableout += "|"+bpsyamlname+"|"+elem['issue_name']+"|"+uniqProcName+"|\n"
        if self.issue_name is not None:
            issue = jira.issue(self.issue_name)
            issue.update(fields={"description": tableout, "summary": "Step: "+str(self.name)})

        if issue is None:
            print(self.name)
            issue = jira.create_issue(
                project="DRP",
                issuetype="Task",
                summary=f"Step: {self.name}",
                description=tableout,
                components=[{"name": "Test"}],
            )
            LOG.info(f"Created issue {issue}")

        self.issue_name = str(issue)

        with TemporaryDirectory() as staging_dir:
            # Write the workflows first, so that the issue names
            # can be included when the step issue itself is created.
            if cascade:
                for workflow in self.workflows:
                    if workflow.issue_name is not None:
                        workflow_issue = jira.issue(workflow.issue_name)
                    else:
                        workflow_issue = None

                    workflow.to_jira(jira, workflow_issue, replace=replace)

            self.to_files(staging_dir)

            dir = Path(staging_dir)
            if self.name is not None:
                dir = dir.joinpath(self.name)

            for file_name in ALL_STEP_FNAMES:
                full_file_path = dir.joinpath(file_name)
                if full_file_path.exists():
                    print("issue.fields.attachment:",issue)
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
        jira : `jira.JIRA`,
            The connection to Jira.
        issue : `jira.resources.Issue`
            This issue from which to load campaign data.

        Returns
        -------
        campaign : `Campaign`
            An initialized instance of a campaign.
        """
        issue = jira.issue(issue) if isinstance(issue, str) else issue

        with TemporaryDirectory() as staging_dir:
            dir = Path(staging_dir)
            for attachment in issue.fields.attachment:
                if attachment.filename in ALL_STEP_FNAMES:
                    file_content = attachment.get()
                    LOG.debug(f"Read {attachment.filename} from {issue}")
                    fname = dir.joinpath(attachment.filename)
                    with fname.open("wb") as file_io:
                        file_io.write(file_content)
                        LOG.debug(f"Wrote {fname}")

            fname = dir.joinpath(STEP_SPEC_FNAME)
            with fname.open("rt") as file_io:
                step_spec = yaml.safe_load(file_io)
                LOG.debug(f"Read {fname}")

            workflows_path = dir.joinpath("workflows")
            workflows_path.mkdir(exist_ok=True)
            LOG.debug(f"Created {workflows_path}")
            for workflow_params in step_spec["workflows"]:
                if "issue" in workflow_params and workflow_params["issue"] is not None:
                    workflow_issue_name = workflow_params["issue"]
                    workflow_issue = jira.issue(workflow_issue_name)
                    workflow = Workflow.from_jira(workflow_issue, jira)
                    workflow.to_files(workflows_path)
                else:
                    LOG.warning(
                        f"Could not load {workflow_params['name']} from jira (no issue name)"
                    )

            step = cls.from_files(staging_dir)
            step.issue_name = str(issue)

        return step

    def __str__(self):
        output = f"""{self.__class__.__name__}
name: {self.name}
issue name: {self.issue_name}
split bands: {self.split_bands}
exposure groups: {str(self.exposure_groups)}
workflows:"""

        #for wf in self.workflows:
            #output += f"\n - {wf.name} (issue {wf.issue_name})"
            #output += f" with dataQuery {wf.bps_config['payload']['dataQuery']}"

        return output
