
prodstatus
==========

``prodstat`` provides scripts which are used  to organize DP0.2 production and collect production statistics.
Collected statistics in form of plots and tables can be reported to corresponding Jira tickets.

For a "quick start" guide, see: `doc/lsst.prodstatus/quickstart.rst. <doc/lsst.prodstatus/quickstart.rst/>`_

Quick reminders
---------------

Check out the product::

  git clone https://github.com/lsst-dm/prodstatus.git
  cd prodstatus
  scons

Setup the environment::

  setup lsst_distrib
  setup -r <prodstatus dir> prodstatus

Get a list of commands::

  prodstat --help

Get help on a command::

  prodstat COMMAND --help

Split a list of exposures into groups for processing, creating BPS submit files::

  prodstat make-prod-groups BPS_SUBMIT_TEMPLATE_FNAME [all|f|u|g|r|i|z|y] GROUPSIZE SKIPGROUPS NGROUPS EXPLIST_FNAME

The `bps submit` command from the `ctrl_bps` product can be used here to submit the just created BPS sumbit files::

  bps submit BPS_SUBMIT_FNAME

Create a new Jira ticket to track a processing job (create a "DRP" issue)::

  prodstat update-issue BPS_SUBMIT_FNAME PRODUCTION_ISSUE

Update an existing Jira ticket that tracks a processing job (update a "DRP" issue)::

  prodstat update-issue BPS_SUBMIT_FNAME PRODUCTION_ISSUE DRP_ISSUE

Update statistics on a job in the Jira issues that track it::

  prodstat update-stat PRODUCTION_ISSUE DRP_ISSUE

Create a plot with timing data::

  prodstat prep-timing-data PARAM_FILE
  prodstat plot-data PARAM_FILE

Create template yaml for a campaign::

  prodstat create-campaign-yaml campaign.yaml

Create or update campaign::

  prodstat update-campaign campaign.yaml

Create template yaml for a step::

  prodstat create-step-yaml step.yaml

Create or update step::

  prodstat update-step step.yaml

Map bps submit yaml files to specific DRP workflow issue tickets::

  prodstat map-drp-steps map-bps-steps.yaml step_issue campaign_flag

See `doc/lsst.prodstatus/quickstart.rst. <doc/lsst.prodstatus/quickstart.rst/>`
for descriptions of the parameters and other options.
