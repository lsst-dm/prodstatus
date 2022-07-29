.. py:currentmodule:: lsst.prodstatus

.. _lsst.prodstatus:

###############
lsst.prodstatus
###############

.. Paragraph that describes what this Python module does and links to related modules and frameworks.

prodstatus is a package for tracking statistics on data processing of Rubin Obseratory data in production using Jira tickets.
   
.. _lsst.prodstatus-using:

Using lsst.prodstatus
=====================

.. toctree linking to topics related to using the module's APIs.

.. toctree::
   :maxdepth: 1

   install
   quickstart    
   
.. _lsst.prodstatus-contributing:

Contributing
============

``lsst.prodstatus`` is developed at https://github.com/lsst-dm/prodstatus.
You can find Jira issues for this module under the `prodstatus <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20prodstatus>`_ component.

.. If there are topics related to developing this module (rather than using it), link to this from a toctree placed here.

.. .. toctree::
..    :maxdepth: 1

.. _lsst.prodstatus-command-line-taskref:

Python API reference
====================

.. automodapi:: lsst.prodstatus.DRPUtils
   :no-main-docstr:
   :no-inheritance-diagram:

.. automodapi:: lsst.prodstatus.GetButlerStat
   :no-main-docstr:
   :no-inheritance-diagram:

.. automodapi:: lsst.prodstatus.GetPanDaStat
   :no-main-docstr:
   :no-inheritance-diagram:

.. automodapi:: lsst.prodstatus.JiraUtils
   :no-main-docstr:
   :no-inheritance-diagram:

.. automodapi:: lsst.prodstatus.MakePandaPlots
   :no-main-docstr:
   :no-inheritance-diagram:

.. automodapi:: lsst.prodstatus.ReportToJira
   :no-main-docstr:
   :no-inheritance-diagram:

.. automodapi:: lsst.prodstatus.cli.prodstat
   :no-main-docstr:
   :no-inheritance-diagram:
