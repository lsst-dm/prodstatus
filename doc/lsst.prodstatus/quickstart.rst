

prodstatus
==========

``prodstat`` provides scripts which are used  to organize DP0.2 production and collect production statistics.
Collected statistics in form of plots and tables can be reported to corresponding Jira tickets.

Obtaining the package -- initial setup
======================================

::

   setup lsst_distrib

   git clone https://github.com/lsst-dm/prodstatus.git

   cd prodstatus

   setup prodstatus -r .

   scons

Notes on Compiling the package the first time and Running tests
---------------------------------------------------------------

::

   scons
   -----

This makes copies the prodstat from ``bin.src`` to ``bin`` dir and compiles anything that needs
compiling (in this case its pure python so no compiling), it also makes sure the python
is setup in the right place this only needs to be run once after checking out or updating

Not all tests may pass, but proceed for now.

After this initial 'scons', for subsequent runs of prodstat commands,
it is sufficient to run::

  setup lsst_distrib;
  cd prodstatus;
  setup prodstatus -r .

  in prodstatus directory

If you haven't changed any binaries or added any new python files, you don't
have to run scons again.

Set up the package -- subsequent setups
=======================================
See :doc:`prodstatus-install`.

::

   setup lsst_distrib ; # if you have not done so already

   cd prodstatus; #if you are not there already

   setup prodstatus -r .

If you wont to avoid `cd prodstatus`,
you can also do `setup prodstatus -r <mypathtoprodstatus>`

where it will find the `ups/prodstatus.table` file to complete the EUPS setup of the product.

Using the package
-----------------

Get help on the command line interface for prodstatus:

::

   `prodstat --help`

Usage:

::

  `prodstat [OPTIONS] COMMAND [ARGS]...`

Options:

::

  `--help  Show this message and exit.`

Commands:

::

 `add-job-to-summary`:     Add a summary to a job summary table.
 `get-butler-stat`:        Build production statistics tables using Butler.
 `get-panda-stat`:         Build production statistics tables using PanDa.
 `make-prod-groups`:       Split a list of exposures into groups defined in yaml.
 `map-drp-steps`:          Update description of a step, by parsing the map...
 `plot-data`:              Create timing data of the campaign jobs.
 `prep-timing-data`:       Create timing data of the campaign jobs Parameters.
 `report-to-jira`:         Report production statistics to a Jira ticket.
 `update-issue`:           Update or create a DRP issue.
 `update-stat`:            Update issue statistics.
 `create-campaign-yaml`:  Creates campaign yaml template.
 `create-step-yaml`:      Creates step yaml.
 `update-campaign`:       Creates or updates campaign.
 `update-step`:           Creates/updates step.
 `create-campaign-yaml`:  Creates campaign yaml template.
 `create-step-yaml`:      Creates step yaml.
 `update-campaign`:       Creates or updates campaign.
 `update-step`:           Creates/updates step.

Obtaining help on command
-------------------------

::

   `prodstat COMMAND --help`

Organizing production
=====================

::

  setup lsst_distrib
  mkdir mywork
  cd mywork
  git clone https://github.com/lsst-dm/prodstatus.git
  cd prodstatus
  setup prodstatus -r .
  cd ../

it is also useful to have the https://github.com/lsst-dm/dp02-processing package checked out 
which has the DC0.2 exposure list ``explist`` and some 
sample template ``bps submit`` scripts and
auxillary bps includes like memoryRequest.yaml and clustering.yaml::

  git clone https://github.com/lsst-dm/dp02-processing.git


Sample DP0.2 tract list, explist, templates, and clustering yaml memoryRequest yaml are in:

https://github.com/lsst-dm/dp02-processing/tree/main/full/rehearsal/PREOPS-938

On your data-int.lsst.cloud node, to enable running scripts, like ``update-issue``, etc
one needs to install Jira locally in you home area and add a login credential .netrc file.
To install Jira do this::

  `pip install jira`

If a local install of Jira is not an option,
You may also be able to find the Jira packages in the standard lsst_distrib stack eventually,
or with an additional setup beyond setup lsst_distrib.

Until tokens are enabled for Jira access, one can use a .netrc file for Jira authentication.
Please ask for help if you need it here for Jira authentication.  Note that if you
fail to login correctly a few times, Jira will require you to use a captcha to get back in.
The easiest way to to this is to use the web-browser JIRA interface to log in correctly
one time and answer the captcha correctly, then the python API interface with .netrc (updated
if necesssary) will work again.

submit a job to bps, record it in an issue
------------------------------------------

Do this:

::

  `bps submit clusttest-all-1.yaml`
  `prodstat issue-update clusttest-all-1.yaml PREOPS-XXX`

(this will return a new DRP-YYY issue number -- make a note of the DRP-YYY number issued)

clusttest-all-1.yaml is a bps submit yaml file which contains enough information to generate a quantum
graph and execution butler (if applicable) to run a set of pipetasks on an input collection,
resulting in an output collection in the butler.  It describes one bps unit of data production.

The prodstat issue-update ... command will search through your submit directory (if accessible)
for the 'expanded version' of the bps submit yaml file and generate a new JIRA DRP-YYY ticket
containing key keywords extracted from the bps yaml file(s).  The new JIRA DRP-YYY ticket will
referfence the overriding campaign description ticket (PREOPS-XXX in this example),
which is assumed to be pre-existing.

By default it will pick the most recent timestamp that it can find with that PREOPS-XXX in your
submit directory tree.

or:

::

  `prodstat update-issue clusttest-all-1.yaml PREOPS-XXX DRP0 [--ts 20211225T122512Z]`

The ``--ts TIMESTAMP`` option allows one to create new DRP-YYY issues for a bps submit yaml
long after the initial bps submit is done.  One should search through the submit/ directory
tree to find a directory with the timestamp ``TIMESTAMP`` that contains a copy 
the clusttest-all-1.yaml submit file to make sure these are in sync.

One may also find the timestamps on the wfprogress panDa workflow status page.
(for DP0.2, this was at: https://panda-doma.cern.ch/idds/wfprogress)

Note:
Generally the update-issue command should be run by the person who run production where
access to bps files is available.

Update Butler, Panda Stats when job is partially complete and again when done:

When job completes, or while still running (panDa workflow shows it in a 'transforming' state),
you can update the stats table in the DRP-YYY ticket with this call::

  `prodstat update-stat PREOPS_XXX DRP-YYY`

this will take several minute to query the butler, panda and generate the updated stats


Commands
========

update-issue
------------

Update or create a DRP issue::

   `prodstat update-issue BPS_SUBMIT_FNAME PRODUCTION_ISSUE [DRP_ISSUE] [--ts TIMESTAMP]`


Parameters:

::

   bps_submit_fname : `str`
     The file name for the BPS submit file (yaml).
     Should be sitting in the same dir that bps submit was done,
     so that the submit/ dir tree can be searched for more info
   production_issue : `str`
     PREOPS-938 or similar production issue for this group of
     bps submissions
   drp_issue : `str`
     DRP-YYY issue created to track prodstatus for this bps submit
     if this is left off or is the special string DRP0, then a
     new issue will be created and assigned (use this newly created number
     for future prodstat update-stat and prodstat add-job-to-summary calls.
   --ts : `str`
     TimeStamp of the form YYYYMMDDTHHMMSSZ (i.e. 20220107T122421Z)

Options:

::

 --ts TEXT  timestamp
 --help     Show this message and exit.

Example::

  `prodstat update-issue ../dp02-processing/full/rehearsal/PREOPS-938/clusttest.yaml PREOPS-938 DRP0 --ts 20211225T122522Z`

or::

  `prodstat update-issue ../dp02-processing/full/rehearsal/PREOPS-938/clusttest.yaml PREOPS-938`

this will use the latest timestamp in the submit subdir, and so if you've done any bps submits since
this one, you should instead hunt down the correct ``TIMESTAMP`` and pass it with ``--ts TIMESTAMP``.

This will return a new DRP-YYY issue where the  prodstats for the PREOPS-938 issue step will be stored
and updated later.

make-prod-groups
----------------

Split a list of exposures into groups defined in yaml files::

  `prodstat make-prod-groups [OPTIONS] TEMPLATE [all|f|u|g|r|i|z|y] GROUPSIZE SKIPGROUPS NGROUPS EXPLIST`


Parameters:

::

  template : `str`
    Template file with place holders for start/end dataset/visit/tracts
    If these variables are present in a template file:
    GNUM (group number 1--N for splitting a set of visits/tracts),
    LOWEXP (first visit/exposure or tract number in a range)
    HIGHEXP (last visit/exposure or tract number in a range)
    They will be substituted for with the values drawn from the explist/tractlist file
    (an optional .yaml suffix here will be added to each generated bps submit yaml in the group)
  band : `str`
        Which band to restrict to (or 'all' for no restriction, matches BAND
        in template if not 'all'). Currently all is always used instead of
        separating by band
  groupsize : `int`
      How many visits (later tracts) per group (i.e. 500)
  skipgroups: `int`
      skip <skipgroups> groups (if others generating similar campaigns)
  ngroups : `int`
      how many groups (maximum)
  explists : `str`
      text file listing <band1> <exposure1> for all visits to use
      this may alternatively be a file listing tracts instead of exposures/visits.
      valid bands are: ugrizy for exposures/visits and all for tracts (or if the
      band is not needed to be known)

get-butler-stat
----------------

Call::

  `prodstat get-butler-stat inpfile.yaml`

After the task is finished the information in butler metadata will be scanned and corresponding tables will
be created in  user_data_dir (~/.local/share/ProdStat/ on Linux) directory.

The inpfile.yaml has following format:

::

   Butler: s3://butler-us-central1-panda-dev/dc2/butler.yaml ; or butler-external.yaml on LSST science platform
   Jira: PREOPS-905 ; jira ticket information for which will be selected.
                    This can be replaced by any other token that will help to uniquely
                    identify the data collection.
   collType: 2.2i ; a token which help to uniquely recognize required data collection
   maxtask: 30 ; maximum number of tasks to be analyzed to speed up the process
   start_date: '2022-01-30' ; dates to select data, which will help to skip previous production steps
   stop_date: '2022-02-02'


This program will scan butler registry to select _metadata files for
tasks in given workflow. Those metadata files will be copied one by
one into ``/tmp/tempTask.yaml`` file from which maxRss and CPU time usage
will be extracted.  The program collects these data for each task type
and calculates total CPU usage for all tasks of the type. At the end
total CPU time used by all workflows and maxRss will be calculated and
resulting table will be created as `<user_data_dir>`/butlerStat-PREOPS-XXX.png
file. The text version of the table used to put in Jira comment is
also created as `<user_data_dir>`/butlerStat-PREOPS-XXX.txt

Options:

::

  --clean_history True/False. Default False
  This option permits to collect statistics in steps for different subsets of
   the data set, or present statistics just for one subset.

get-panda-stat
--------------

Call::

  `prodstat get-panda-stat  inpfile.yaml`

The input file format is exactly same as for get-butler-stat command.

The program will query PanDa web logs to select information about workflows,
tasks and jobs whose status is either finished, sub-finished, running or transforming.
It will produce 2 sorts of tables.

The first one gives the status of the campaign production showing each
workflow status as `<user_data_dir>`/pandaWfStat-PREOPS-XXX.txt.  A styled html
table also is created as `<user_data_dir>`/pandaWfStat-PREOPS-XXX.html

The second table type lists completed tasks, number of quanta in each,
time spent for each job, total time for all quanta and wall time
estimate for each task. This information permit us to estimate rough
number of parallel jobs used for each task, and campaign in whole.
The table names created as `<user_data_dir>`/pandaStat-PREOPS-XXX.png and
pandaStat-PREOPS-XXX.txt.

Here PREOPS-XXX tokens represent Jira ticket the statistics is collected for.

Options:

::

  --clean_history True/False. Default False.
  This option permits to collect statistics in steps for different subsets of
  the data set, or present statistics just for one subset.

prep-timing-data
-----------------

Call::

  `prodstat prep-timing-data ./inp_file.yaml`

The input yaml file should contain following parameters::

  Jira: "PREOPS-905" - jira ticket corresponding given campaign.
  collType: "2.2i" - a token to help identify campaign workflows.
  bin_width: 3600. - the width of the plot bin in sec.
  job_names - a list of job names
   - 'measure'
   - 'forcedPhotCoad'
   - 'mergeExecutionButler'
  start_at: 0. - plot starts at hours from first quanta
  stop_at: 72. - plot stops at hours from first quanta
  start_date: '2022-02-04' ; dates to select data, which will help to skip previous production steps
  stop_date: '2022-02-07'

The program scan panda idds database to collect timing information for all job names in the list.
Please note the list format for job_names, and the quotes are required around start_date, stop_date.
This can take a long time if there are lots of quanta involved.
Note that the querying of the panDA IDDS can be optimized further in the future.
It creates then timing information in `user_data_dir` directory with file names like::

  panda_time_series_<job_name>.csv

Options:

::

     --clean_history True/False. Default False
     This option permits to collect timing data in steps for different time slices,
     or select just individual time slice.

plot-data
---------

Call::

  `prodstat plot-data inp_file.yaml`

The program reads timing data created by prep-timing-data command and
build plots for each type of jobs in given time boundaries.
each type of jobs in given time boundaries.
One may change the start_at/stop_at limits to make a zoom in
plot without rerunning prep-timing-data.

report-to-jira
--------------

Call::

   `prodstat report-to-jira report.yaml`

The report.yaml file provide information about comments and attachments that need to be added or
replaced in given jira ticket.
The structure of the file looks like following:

::

    project: 'Pre-Operations'
    Jira: PREOPS-905
    comments:
    - file: ~/.local/shared/ProdStat/pandaStat-PREOPS-905.txt
    tokens:        tokens to uniquely identify the comment to be replaced
      - 'pandaStat'
      - 'campaign'
      - 'PREOPS-905'
    - file: ~/.local/shared/ProdStat/butlerStat-PREOPS-905.txt
    tokens:
      - 'butlerStat'
      - 'PREOPS-905'
   attachments:
     - ~/.local/shared/ProdStat/pandaWfStat-PREOPS-905.html
     - ~/.local/shared/ProdStat/pandaStat-PREOPS-905.html
     - ~/.local/shared/ProdStat/timing_detect_deblend.png
     - ~/.local/shared/ProdStat/timing_makeWarp.png
     - ~/.local/shared/ProdStat/timing_measure.png
     - ~/.local/shared/ProdStat/timing_patch_coaddition.png

create-campaign-yaml
------------------------

Call::

  `create-campaign-yaml  campaign-name campaign.yaml`

This creates campaign yaml template.
Here campaign_name is an arbitrary name of the campaign;
campaign_yaml is yaml file to which  campaign parameters will be written.
The file should be treated as a template. It should be edited to
add workflow base directories for each active step.
The template will contain fields describing the campaign and related 7 steps.

Options:

::

   --campaign_issue : a string containing the campaign jira ticket.
    If specified the campaign yaml will be loaded from the
    ticket and steps information will be updated with input parameters.

Example of the campaign.yaml:

::

  `issue: DRP-465`
  `name: w_2022_27_preops-1248`
  `steps: `
  `- campaign_issue: null`
     `issue_name: DRP-457`
     `name: step1`
     `split_bands: false`
     `workflow_base: <path to step data>/step1/`
  `- campaign_issue: null`
     `issue_name: DRP-458`
     `name: step2`
     `split_bands: false`
     `workflow_base: <path to step data>/step2/`
  `- campaign_issue: null`
     `issue_name: DRP-459`
     `name: step3`
     `split_bands: false`
     `workflow_base: <path to step data>/step3/`
  `- campaign_issue: null`
     `issue_name: DRP-460`
     `name: step4`
     `split_bands: false`
     `workflow_base: <path to step data>/step4/`
  `- campaign_issue: null`
     `issue_name: DRP-461`
     `name: step5`
     `split_bands: false`
     `workflow_base: <path to step data>/step5/`
  `- campaign_issue: null`
     `issue_name: DRP-462`
     `name: step6`
     `split_bands: false`
     `workflow_base: <path to step data>/step6/`
  `- campaign_issue: null`
     `issue_name: DRP-463`
     `name: step7`
     `split_bands: false`
     `workflow_base: <path to step data>/step7/`

In this example `workflow_base` indicate directory where the step workflow yaml files
are located. This makes reasonable to use this command by the person who run production.

update-campaign
---------------

Call::

  `prodstat update-campaign [OPTIONS] CAMPAIGN_YAML`

The command creates new or updates existing campaign.
Here CAMPAIGN_YAML is a yaml file created from template yaml file created
in previous command. The command will scan associated steps and update information
in steps looking in corresponding workflow directories.
The updated campaign.yaml file will be stored in the campaign jira ticket attachments
as well as updated step.yaml files will be stored in corresponding step jira tickets.


Options:

::

   `--campaign_issue` if specified will   overwrite campaign issue in input yaml file.
   `--campaign_name` if specified will change campaign name in the jira ticket.

create-step-yaml
-------------------------

This command is used when one need to create or update information for a
particular step. The step.yaml file will be created as a template.

Call::

  `prodstat create-step-yaml [OPTIONS] step.yaml`

Options:

::

`--step_issue` if provided the step jira ticket will be added to the template
`--campaign_issue` if provided the campaign jira ticket will be added to then
template.

The step.yaml need to be edited to create or update information stored in jira
 ticket for given step.

update-step
-----------
The command is used to create step jira ticket, or update information in
the ticket.

Call::

  `prodstat update-step [OPTIONS] step.yaml`

Options:

::

`--step_issue` if specified it updates existing step jira ticket.
`--campaign_name` is a campaign jira ticket the step belongs to.

If specified the step ticket will be linked to the campaign ticket.
`step_name` is a step name like `step5`. If specified it will overwrite
the name provided in the step.yaml.

Note:
It is recommended to use campaign commands to create steps related to the campaign,
and to create cross links between campaign and steps jira tickets.



map-drp-steps
-------------

This command is used to make a one-to-one linkage between a workflow DRP-YYYY JIRA issue
and a BPS submit yaml file and update this linkage on a campaign or step level JIRA issue.

Call::

 `prodstat map-drp-steps MAP_YAML STEP_ISSUE CAMPAIGN_FLAG`

The MAP_YAML has the form (in the step case):

::

  cat step2map.yaml

   {
   step2_all_14 : DRP-142 ,
   step2_all_13 : DRP-143 ,
   step2_all_12 : DRP-141 ,
   step2_all_11 : DRP-139 ,
   step2_all_10 : DRP-476 ,
   step2_all_9 : DRP-475 ,
   step2_all_8 : DRP-474 ,
   step2_all_7 : DRP-138 ,
   step2_all_6 : DRP-137 ,
   step2_all_5 : DRP-136 ,
   step2_all_4 : DRP-134 ,
   step2_all_3 : DRP-133 ,
   step2_all_2 : DRP-132 ,
   step2_all_1 : DRP-131
   }

This MAP_YAML file is currently constructed by hand after a set of
bps submit have been done.

Eventually it could be automatically generated as part
of the update-issue procedure.

STEP_ISSUE is the (preexisting) name of the DRP-ZZZZ issue (created by
create/update-campaign (recursively) or create/update-step),
in the case where CAMPAIGN_FLAG is 0.

CAMPAIGN_FLAG is 0 if this is a STEP_ISSUE, or CAMPAIGN_FLAG is 1 if this is a CAMPAIGN MAP,
in which case STEP_ISSUE is in fact a pre-existing CAMPAIGN_ISSUE.

The syntax of the CAMPAIGN MAP (which links STEP JIRA issues to step
names and rollup statistics about a step:

::

   cat camp17.yaml

   {
   step1 : [DRP-466,'2021-12-18','2022-01-12',166000,Complete],
   step2 : [DRP-467,'2022-01-20','2022-01-24',22000,Complete],
   step3 : [DRP-468,'2022-02-18','2022-03-25',1100000,Complete],
   step4 : [DRP-469,'2022-04-01','2022-04-30',1100000,Complete],
   step5 : [DRP-470,'2022-05-03','2022-05-12',66000,Complete],
   step6 : [DRP-471,'2022-05-12','2022-05-16',16000,Complete],
   step7 : [DRP-472,'2022-05-01','2022-05-01',10,Complete]
   }



