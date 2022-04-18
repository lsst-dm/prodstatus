#!/usr/bin/env python
# This file is part of ctrl_bps.
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
"""Subcommand definitions.
"""
import click
import yaml
import os
import io
from lsst.daf.butler.cli.utils import MWCommand

from tempfile import TemporaryDirectory
from lsst.prodstatus import DRPUtils
from lsst.prodstatus.JiraUtils import JiraUtils
from lsst.prodstatus.GetButlerStat import GetButlerStat
from lsst.prodstatus.GetPanDaStat import GetPanDaStat
from lsst.prodstatus.ReportToJira import ReportToJira
from lsst.prodstatus.MakePandaPlots import MakePandaPlots
from lsst.prodstatus.Workflow import Workflow
from lsst.prodstatus.Step import Step
from lsst.prodstatus.Campaign import Campaign
from lsst.ctrl.bps import BpsConfig


class ProdstatusCommand(MWCommand):
    """Command subclass with prodst-command specific overrides."""

    extra_epilog = "See 'prodstat --help' for more options."


@click.command(cls=ProdstatusCommand)
@click.argument("param_file", type=click.Path(exists=True))
def get_butler_stat(param_file):
    """Build production statistics tables using Butler metadata.

    Parameters
    ----------
    param_file: `str`
        name of the input yaml file.
        The file should provide following parameters:

        \b
        Butler : `str`
            URL of the Butler storage
        Jira : `str`
            Jira ticket identifying production campaign used
            to select campaign workflows
        CollType : `str`
            token that with jira ticket will uniquely define campaign workflows
        startTime : `str`
            time to start selecting workflows from in Y-m-d format
        stopTime : `str`
            time to stop selecting workflows in Y-m-d format
        maxtask : `int`
            maximum number of task files to analyse
    """

    click.echo("Start with GetButlerStat")
    with open(param_file) as p_file:
        in_pars = yaml.safe_load(p_file)
    butler_stat = GetButlerStat.GetButlerStat(**in_pars)
    butler_stat.run()
    click.echo("End with GetButlerStat")


@click.command(cls=ProdstatusCommand)
@click.argument("bps_submit_fname", type=str)
@click.argument("production_issue", type=str)
@click.argument("drp_issue", required=False, default="DRP0", type=str)
@click.option("--ts", default="0", type=str)
def update_issue(bps_submit_fname, production_issue, drp_issue, ts):
    """Update or create a DRP issue.

    \b
    Parameters
    ----------
    bps_submit_fname : `str`
        The file name for the BPS submit file (yaml).
        Should be sitting in the same dir that bps submit was done,
        so that the submit/ dir can be searched for more info
    production_issue : `str`
        PREOPS-938 or similar production issue for this group of
        bps submissions
    drp_issue : `str`
        DRP issue created to track prodstatus for this bps submit
    ts : `str`
        unknown
    """
    drp = DRPUtils.DRPUtils()
    drp.drp_issue_update(bps_submit_fname, production_issue, drp_issue, ts)


@click.command(cls=ProdstatusCommand)
@click.argument("production_issue", type=str)
@click.argument("drp_issue", type=str)
@click.option("--reset", default=False, type=bool)
@click.option("--remove", default=False, type=bool)
def add_job_to_summary(production_issue, drp_issue, reset, remove):
    """Add a summary to a job summary table.

    \b
    Parameters
    ----------
    production_issue : `str`
        campaign defining ticket, also in the butler output name
    drp_issue : `str`
        the issue created to track prodstatus for this bps submit
    reset : `bool`
        erase the whole table (don't do this lightly)
    remove : `bool`
        remove one entry from the table with the DRP/PREOPS number
    """
    if reset and remove:
        click.echo("Either reset or remove can be set, but not both.")

    if reset:
        first = 1
    elif remove:
        first = 2
    else:
        first = 0

    frontend = "DRP-53"
    frontend1 = "DRP-55"
    backend = "DRP-54"
    drp = DRPUtils.DRPUtils()
    drp.drp_add_job_to_summary(
        first, production_issue, drp_issue, frontend, frontend1, backend
    )


@click.command(cls=ProdstatusCommand)
@click.argument("production_issue", type=str)
@click.argument("drp_issue", required=False, default="DRP0", type=str)
def update_stat(production_issue, drp_issue):
    """Update issue statistics.

    \b
    Parameters
    ----------
    production_issue : `str`
        campaign defining ticket, also in the butler output name
    drp_issue : `str`
        leave off if you want a new issue generated, to redo,
        include the DRP-issue generated last time
    """
    drp_utils = DRPUtils.DRPUtils()
    drp_utils.drp_stat_update(production_issue, drp_issue)


@click.command(cls=ProdstatusCommand)
@click.argument("param_file", type=click.Path(exists=True))
def get_panda_stat(param_file):
    """Build production statistics tables using PanDa database queries.

    Parameters
    ----------
    param_file: `str`
        name of the input yaml file.
        The file should provide following parameters:

        \b
        Jira : `str`
            Jira ticket identifying production campaign used
            to select campaign workflows
        CollType : `str`
            token that with jira ticket will uniquely define campaign workflows
        startTime : `str`
            time to start selecting workflows from in Y-m-d format
        stopTime : `str`
            time to stop selecting workflows in Y-m-d format
        maxtask : `int`
            maximum number of task files to analyse
    """
    click.echo("Start with GetPandaStat")
    with open(param_file, "r") as p_file:
        in_pars = yaml.safe_load(p_file)
    panda_stat = GetPanDaStat(**in_pars)
    panda_stat.run()
    click.echo("End with GetPanDaStat")


@click.command(cls=ProdstatusCommand)
@click.argument("param_file", type=click.Path(exists=True))
def report_to_jira(param_file):
    """Report production statistics to a Jira ticket

    Parameters
    ----------
    param param_file: `str`
        name of the parameter yaml file with path

    Notes
    -----
    The yaml file should provide following parameters:

    \b
    project: 'Pre-Operations'
        project name
    Jira: `str`
        jira ticket like PREOPS-905
    comments: `list`
        list of comment files with path
        each file entry contains list of tokens to identify comment
        to be replaced
    attachments: `list`
        list of attachment files with path
    :return:
    """
    click.echo("Start with ReportToJira")
    report = ReportToJira.ReportToJira(param_file)
    report.run()
    click.echo("End with ReportToJira")


@click.command(cls=ProdstatusCommand)
@click.argument("param_file", type=click.Path(exists=True))
def prep_timing_data(param_file):
    """Create  timing data of the campaign jobs

    Parameters
    ----------
    param_file : `str`
        A file from which to read  parameters

    Notes
    -----
    The yaml file should provide following parameters::

    \b
        Jira: `str`
            campaign jira ticket for which to select data
        collType: `str`
            token to help select data, like 2.2i or sttep2
        job_names: `list`
            list of task names for which to collect data
            - 'pipetaskInit'
            - 'mergeExecutionButler'
            - 'visit_step2'
        bin_width: `float`
            bin width in seconds
        start_at: `float`
            start of the plot in hours from first quanta
        stop_at: `float`
            end of the plot in hours from first quanta
    """

    click.echo("Start with MakePandaPlots")
    with open(param_file, "r") as p_file:
        params = yaml.safe_load(p_file)
    panda_plot_maker = MakePandaPlots.MakePandaPlots(**params)
    panda_plot_maker.prep_data()
    click.echo("Finish with prep_timing_data")


@click.command(cls=ProdstatusCommand)
@click.argument("param_file", type=click.Path(exists=True))
def plot_data(param_file):
    """Create timing data of the campaign jobs.

    Parameters
    ----------
    param_file : `str`
        A yaml file from which to read  parameters

    Notes
    -----
    The yaml file should provide following parameters:

    \b
        Jira: `str`
            campaign jira ticket for which to select data
        collType: `str`
            token to help select data, like 2.2i or sttep2
        job_names: `list`
            list of task names for which to collect data
            - 'pipetaskInit'
            - 'mergeExecutionButler'
            - 'visit_step2'
        bin_width: `float`
            bin width in seconds
        start_at: `float`
            start of the plot in hours from first quanta
        stop_at: `float`
            end of the plot in hours from first quanta
    """
    click.echo("Start with plot_data")
    with open(param_file, "r") as p_file:
        params = yaml.safe_load(p_file)
    panda_plot_maker = MakePandaPlots.MakePandaPlots(**params)
    panda_plot_maker.plot_data()
    click.echo("Finish with plot_data")


@click.command(cls=ProdstatusCommand)
@click.argument("campaign_name", type=str)
@click.argument("campaign_yaml", type=click.Path())
@click.option('--campaign_issue', required=False, type=str, default="")
@click.option('--steps_list', required=False, type=str, default=None)
def create_campaign_yaml(campaign_name, campaign_yaml, campaign_issue, steps_list):
    """Creates or updates campaign.
    \b
    Parameters
    ----------
    campaign_name : `str`
        An arbitrary name of the campaign.
    campaign_yaml : `str`
        A yaml file to which  campaign parameters will be written.
    campaign_issue : `str`
        if specified  the campaing yaml will be loaded from the
        ticket and updated with input parameters
    """
    steps = ['step1', 'step2', 'step3', 'step4', 'step5',
             'step6', 'step7']
    click.echo("Start with create_campaign_yaml")
    click.echo(f"Campaign issue {campaign_issue}")
    click.echo(f"Campaign name {campaign_name}")
    click.echo(f"Campaign yaml {campaign_yaml}")
    campaign_template = dict()
    if campaign_issue is not None:
        "Read campaign yaml from ticket"
        ju = JiraUtils()
        auth_jira, username = ju.get_login()
        issue = ju.get_issue(campaign_issue)
        print(issue)
        all_attachments = ju.get_attachments(issue)
        print(all_attachments)
        for aid in all_attachments:
            print(aid, all_attachments[aid])
            att_file = all_attachments[aid]
            if att_file == "campaign.yaml":
                attachment = auth_jira.attachment(aid)  #
                a_yaml = io.BytesIO(attachment.get()).read()
                campaign_template = yaml.safe_load(a_yaml)
                print(campaign_template)
    else:
        campaign_template['name'] = campaign_name
        campaign_template['issue'] = campaign_issue
        campaign_template['steps'] = list()
    step_data = list()
    if steps_list is None:
        " create default steps "
        for step in steps:
            step_dict = dict()
            step_dict['issue'] = ''
            step_dict['name'] = step
            step_dict['split_bands'] = False
            step_dict['forkflow_dir']
            step_dict['workflows'] = list()
            step_data.append(step_dict)
    else:
        with open(steps_list, 'r') as sl:
            step_data = yaml.safe_load(sl)
    campaign_template['steps'] = step_data
    with open(campaign_yaml, 'w') as cf:
        yaml.dump(campaign_template, cf)
    click.echo("Finish with create_campaign_yaml")


@click.command(cls=ProdstatusCommand)
@click.argument("campaign_yaml", type=click.Path(exists=True))
@click.option('--campaign_issue', required=False, type=str, default="")
@click.option('--campaign_name', required=False, type=str, default="")
def update_campaign(campaign_yaml, campaign_issue, campaign_name):
    """Creates or updates campaign.
    \b
    Parameters
    ----------
    campaign_yaml : `str`
        A yaml file from which to get campaign parameters.
    campaign_issue : `str`
        if specified  it overwrite a pre-existing DRP ticket,
        if not, it creates a new JIRA issue.
    campaign_name : `str`
        it can take the name from the campaign name from the
        campaign.yaml file, or perhaps look inside the yaml
        file for a keyword campaignName.
    """
    click.echo("Start with update_campaign")
    click.echo(f"Campaign yaml {campaign_yaml}")
    click.echo(f"Campaign issue {campaign_issue}")
    click.echo(f"Campaign name {campaign_name}")
    campaign = Campaign.create_from_yaml(campaign_yaml)
    print(campaign)
    dest = TemporaryDirectory()
    campaign.to_files(dest)
    click.echo("Finish with update_campaign")


@click.command(cls=ProdstatusCommand)
@click.argument("step_yaml", type=click.Path())
@click.argument("step_name", type=str)
@click.option('--step_issue', required=False, type=str, default="")
@click.option('--workflow_dir', required=False, type=str, default=None)
def create_step_yaml(step_yaml, step_name, step_issue, workflow_dir):
    """Creates step yaml.
        \b
        Parameters
        ----------
        step_yaml : `str`
            A name of the step yaml with path

        step_name : `str`
            A name of the step.
        step_issue : `str`
            if specified  the step yaml will be loaded from the
            ticket and updated with input parameters
        workflow_dir: `str`
            A name of the directory where workflow bps yaml files are,
            including path
        """
    click.echo("Start with create_step_yaml")
    click.echo(f"step issue {step_issue}")
    click.echo(f"step name {step_name}")
    click.echo(f"step yaml {step_yaml}")
    click.echo(f"Workflow_dir {workflow_dir}")
    step_template = dict()
    if step_issue is not None:
        "Read step yaml from ticket"
        ju = JiraUtils()
        auth_jira, username = ju.get_login()
        issue = ju.get_issue(step_issue)
        print(issue)
        all_attachments = ju.get_attachments(issue)
        print(all_attachments)
        for aid in all_attachments:
            print(aid, all_attachments[aid])
            att_file = all_attachments[aid]
            if att_file == "step.yaml":
                attachment = auth_jira.attachment(aid)  #
                a_yaml = io.BytesIO(attachment.get()).read()
                step_template = yaml.safe_load(a_yaml)
                print(step_template)
    else:
        step_template['name'] = step_name
        step_template['issue'] = step_issue
        step_template['workflows'] = list()
    workflow_data = list()96
    print(workflow_data)


@click.command(cls=ProdstatusCommand)
@click.argument("step_yaml", type=click.Path(exists=True))
@click.option('--step_issue', required=False, type=str, default="")
@click.option('--campaign_name', required=False, type=str, default="")
@click.option('--step_name', required=False, type=str, default="")
def update_step(step_yaml, step_issue, campaign_name, step_name):
    """Creates/updates step.
    \b
    Parameters
    ----------
    step_yaml : `str`
        A yaml file from which to get step parameters.
    step_issue : `str`
        if specified  it overwrite a pre-existing DRP ticket,
        if not, it creates a new JIRA issue.
    campaign_name : `str`
        jira ticket name of the campaign, if specified then
        it should somehow attach this step.yaml to the
        campaign, it would be nice to allow that to specify
        the campaign by name rather than DRP number,
        but we can work on that later.
    step_name : `str`
    """
    click.echo("Start with update_step")
    click.echo(f"Step issue {step_issue}")
    click.echo(f"Campaign name {campaign_name}")
    click.echo(f"Step yaml {step_yaml}")
    click.echo(f"Step name {step_name}")
    split_bands = False
    base_bps_config = '~/devel/DATA/step1_detectors0_62_all_1.yaml'
    bps_config = BpsConfig(base_bps_config)
    step = Step.generate_new(step_name, bps_config, split_bands, None, None, "")
    print(step)
    jira = JiraUtils()
    (auth_jira, user) = jira.get_login()
    if step_issue is not None:
        issue = auth_jira.issue(step_issue)
        print(f"Issue is: {issue}")
    step.to_jira(auth_jira, issue, replace=True, cascade=True)
    click.echo("Finish with update_step")


@click.command(cls=ProdstatusCommand)
@click.argument("workflow_yaml", type=click.Path(exists=True))
@click.option("--step_name", required=False, type=str, default="")
@click.option('--workflow_issue', required=False, type=str, default=None)
@click.option('--step_issue', required=False, type=str, default=None)
@click.option('--workflow_name', required=False, type=str, default=None)
def update_workflow(workflow_yaml, step_name, workflow_issue, step_issue, workflow_name):
    """Creates/updates workflow.
        It overwrites the existing DRP ticket
        (or makes a new one if --workflow_issue isn't given),.
        It reads the 'full bps yaml' with all includes
        and saves that as an attachment.
    \b
    Parameters
    ----------

    workflow_yaml : `str`
        A yaml file from which to get step parameters.
    step_name : `str`
        A name of the step the workflow belong to
    workflow_issue : `str`
        if specified  it overwrite a pre-existing DRP ticket,
        if not, it creates a new JIRA issue.
    step_issue : `str`
    workflow_name : `str`
    """
    click.echo("Start with update_workflow")
    click.echo(f"Workflow issue:{workflow_issue}")
    click.echo(f"Step issue {step_issue}")
    jira = JiraUtils()
    (auth_jira, user) = jira.get_login()
    temp_dir = TemporaryDirectory()
    with open(workflow_yaml, 'r') as wf:
        workflows = yaml.safe_load(wf)
    " Update workflow.yaml with one particular issue "
    if workflow_issue is not None:
        issue = auth_jira.issue(workflow_issue)
        print(f"Issue is: {issue}")
        all_attachments = jira.get_attachments(issue)
        print(all_attachments)
        for aid in all_attachments:
            print(aid, all_attachments[aid])
            att_file = all_attachments[aid]
            if att_file == "workflow.yaml":
                attachment = auth_jira.attachment(aid)  #
                a_yaml = io.BytesIO(attachment.get()).read()
                wf_sub = yaml.safe_load(a_yaml)
                print(wf_sub)
                if workflows is not None:
                    for workflow_name in workflows:
                        wf_dict = workflows[workflow_name]
                        if wf_sub['name'] == workflow_name:
                            wf_name = wf_sub['name']
                            bps_config = BpsConfig(wf_dict['path'])
                            wf_issue = wf_sub['issue_name']
                        workflow = Workflow(bps_config, wf_name,
                                            step_name, step_issue,
                                            issue_name=wf_issue)
                        workflow.to_files(temp_dir)
    else:
        if workflows is not None:
            for workflow_name in workflows:
                wf_dict = workflows[workflow_name]
                bps_config = BpsConfig(wf_dict['path'])
                wf_name = wf_dict['name']
                wf_issue = wf_dict['issue_name']
                workflow = Workflow(bps_config, wf_name, step_name, step_issue, issue_name=wf_issue)
                workflow.to_files(temp_dir)

    click.echo("Finish with update_workflow")


@click.command(cls=ProdstatusCommand)
@click.argument("step_dir", type=click.Path(exists=True))
@click.argument("step_name_base", type=str, default="")
@click.argument("workflow_yaml", type=click.Path(exists=False))
def make_workflow_yaml(step_dir, step_name_base, workflow_yaml):
    """Creates/updates workflow.yaml for update_workflow command
       It read all step yaml files in a directory and creates new entry
       in the workflow yaml file
    \b
    Parameters
    ----------

    step_dir : `str`
        A directory path where the step yaml files are
    step_name_base : `str`
        A base name to create unique step names
    workflow_yaml : `str`
        A yaml file name where workflow names and step yaml files are stored
        If exists workflow parameters will be updated.
    """
    click.echo("Start with make-workflow-yaml")
    click.echo(f"Step dir:{step_dir}")
    click.echo(f"Step base name {step_name_base}")
    click.echo(f"Workflow yaml: {workflow_yaml}")
    if os.path.exists(workflow_yaml):
        with open(workflow_yaml) as wf:
            workflows = yaml.safe_load(wf)
            print(workflows)
        if workflows is None:
            workflows = dict()
    else:
        workflows = dict()
    " Get list of yaml files in step directory"
    step_files = list()
    for file in os.listdir(step_dir):
        # check the files which  start with step token
        if file.startswith("step"):
            # print path name of selected files
            step_files.append(file)
    for file_name in step_files:
        wf_dict = dict()
        wf_name = file_name.split('.yaml')[0]
        wf_dict['name'] = wf_name
        wf_dict['bps_name'] = ''
        wf_dict['issue_name'] = None
        wf_dict['band'] = 'all'
        wf_dict['step_name'] = step_name_base
        wf_dict['path'] = os.path.join(step_dir, file_name)
        if wf_name not in workflows:
            workflows[wf_name] = wf_dict
    click.echo("created workflow")
    with open(workflow_yaml, 'w') as wf:
        yaml.dump(workflows, wf)
    print(workflows)
    click.echo("Finish with update_workflow")
