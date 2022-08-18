

Sample prodstatus usage
=======================


The Jira issue tracking this example campaign may be followed at:
`DRP-491 <https://jira.lsstcorp.org/browse/DRP-491>` and linked
issues.

In this example usage, a simple two step campaign is produced. The first step of
the campaign `DRP-489 <https://jira.lsstcorp.org/browse/DRP-489>` 
runs 'step1' pipelines on detectors 0-6 of 2 z-band exposures. 
This step is divided into 2 groups, the first of which 
`DRP-486 <https://jira.lsstcorp.org/browse/DRP-486>` specifies running
step1 pipetasks on detectors 0-3 (if present) and the second group bps submit
`DRP-487 <https://jira.lsstcorp.org/browse/DRP-487>` on detectors 4-6 if present 
for the two exposures (visits) in question (1472, 1474).
For step2, 
`DRP-490 <https://jira.lsstcorp.org/browse/DRP-490>`
only one group is present in this case, 
`DRP-488 <https://jira.lsstcorp.org/browse/DRP-488>` which runs step2 pipetasks on all
outputs from the two groups from step1.

The ``prodstat`` utility ``make-prod-groups`` can be used to fill in 
starting and stopping visits (exposures) or tracts into smaller groups.
This utility works with a template bps submit yaml file for a given
step and an input list of exposures or tracts.

For this simple sample example, templates bps submit yaml
files available in the ``dp02-processing`` repository.


The two step1 bps submit groups are named:
`step1_1248.yaml`
`step1_1248_b.yaml` 
A copy of these bps submit yamls are attached to the Jira issue
`DRP-489` are part of the ``prodstat update-campaign`` step.
The `1248` in the name refers to issue `PREOPS-1248` which describes a 
request for test campaign to be run.

The step2 bps submit is named:
`step2_1248.yaml`
And a copy of it is attached to issue `DRP-490`.

The sequence of processing and using the ``prodstat`` product
to track processing flows in this typical sequence:

::

  # setup processing stack
  setup lsst_distrib

  # setup prodstatus (see installation instructions)
  setup -r prodstatus .

  # make work area to store bps submit files and qGraphs
  mkdir work
  cd work
  mkdir step1 step2
  # copy sample templates from dp02-processing repo 
  # into step1, step2 subdirs, use make-prod-groups if desired
  
  # submit step1 bps jobs -- this may require authentication 

  cd step1
  bps submit step1_1248.yaml

  # this will create a submit subdir with a timestamp subdir 
  # that timestamp of the form YYMMDDTHHMMSSZ is a unique key 
  # which panDA and the butler use to identify a workflow.
  # Initiate an issue in Jira corresponding this single 
  # bps workflow:

  prodstat update-issue step1_1248.yaml PREOPS-1248 DRP0 

  # update-issue will search in the submit/ subdir for the most
  # recent timestamp and use that to id the workflow.
  # if one runs update-issue at a later time, or if it
  # doesn't find the workflow timestamp associated with the
  # bps submit yaml file, one may look it up on
  # panDa wfprogress page, or in the submit/ subdir and
  # specify it on the command line as an optional --ts argument:

  prodstat update-issue step1_1248.yaml PREOPS-1248 DRP0 --ts 20220728T192631Z
  
  # The PREOPS-1248 is a campaign description issue which describes
  # the campaign at hand.

  # The DRP0 spec says to create a new Jira Issue.  If instead, one
  # has a pre-existing Jira issue for this workflow, one may give it
  # and it will overwrite.  In this case the Jira issue assigned is
  # DRP-486.

  # After the workflow completes, one may update butler and panda statistics
  # for that workflow with the prodstat update-stat command:

  prodstat update-stat PREOPS-1248 DRP-486

  # This queries the DRP-486 ticket to extract the unique timestamp
  # and uses that to query the Butler and PanDA to extract
  # information about quanta run for this step, cpu-time, 
  # wallclock time, and status of the step are computed and
  # appended to the description field of the DRP-486 issue.
  # one may run update-stat multiple times while the workflow is
  # still in progress, but it should be run at least once once
  # the workflow completes.

  #submit the second group for step1 processing:

  bps submit step1_1248_b.yaml
  prodstat update-issue step1_1248_b.yaml PREOPS-1248 DRP0 
  # or, looking up the timestamp:
  prodstat update-issue step1_1248_b.yaml PREOPS-1248 DRP0 --ts 20220729T001725Z
  # this returns DRP-487 as the workflow issue associated with this run.
  # when complete:
  prodstat update-stat PREOPS-1248 DRP-487
  
  # now submit step2
  cd ../step2
  # bps submit step2_1248.yaml
  prodstat update-issue step2_1248.yaml PREOPS-1248 DRP0 --ts 20220729T142653Z
  # This is assigned DRP-488
  # and update stats once the workflow has finished running (1 hour, typically)
  prodstat update-stat PREOPS-1248 DRP-488

  # One may now create two levels of issues above the lowest level workflow issues.
  # there is a step-level issue for each step of the campaign, step1 and step2 in
  # this case, and there is also an over-arching 'campaign' issue created which
  # points to the two step level issues.  

  # To initialize the campaign and step issues, generate a campaign desription yaml 
  # step12test.yaml with this format:

  :: 

     issue: null
     name: step12test
     steps:
     - campaign_issue: null
       issue_name: ''
       name: step1
       split_bands: false
       workflow_base: '/home/username/work/step1'
     - campaign_issue: null
       issue_name: ''
       name: step2
       split_bands: false
       workflow_base: '/home/username/work/step2'

  # Then run the create-campaign command to create a template yaml:
  prodstat create-campaign-yaml step12test step12test.yaml
  # edit that template to include only the steps in your
  # campaign and include the full paths to the workflow base dirs
  # for each step.  Then run:
  prodstat update-campaign step12test.yaml 
  # This will search the step1, step2 subdirs, looking
  # for yamls of the form stepX...yaml and generate 
  # one new issue for each step (DRP-489, DRP-490) as well as one 
  # overarching generate a new issue DRP-491.
  
  # Now to connect the individual workflows to the step level yamls
  # and to connect the step level issues to the campaign overarching
  # issue with processing information (currently inserted by hand),
  # one runs the map-drp-to-steps command once
  # for each step and once for the campaign.
  
  # Generate (by hand currently) a yaml file which connects a specific
  # workflow with a specific DRP issue. For step 1, stepmap1.yaml looks like:

  ::

    {
    step1_1248 : DRP-486,
    step1_1248_b : DRP-487
    } 

  prodstat map-drp-steps step1map.yaml DRP-489 0

  # The zero at the end indicates that this is a step not
  # a campaign map.

  # and for step2, step2map.yaml:

  :: 
    {
    step2_1248 : DRP-488
    }

  prodstat map-drp-steps step2map.yaml DRP-490 0

  # Then, optionally, one may summarize start-date, end-date, and core-hours used
  # for a campaign in campmap.yaml:

  :: 
    {
    step1 : [DRP-489,'2022-07-27','2022-07-28',100,Complete],
    step2 : [DRP-490,'2022-07-28','2022-07-28',100,Complete]
    }

   prodstat map-drp-steps campmap.yaml DRP-491 1

   # The 1 flag here indicates that this is a campaign level map to steps

   # Now one may, beginning at the DRP-491 issue, click through to the 
   # step level and under there, the workflow level issues. 


