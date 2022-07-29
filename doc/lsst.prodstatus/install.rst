
.. _prodstatus-install:
=============================
prodstatus installation guide
=============================

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

If you wont to avoid `cd prodstatus`,
you can also do `setup prodstatus -r <my_path_to_prodstatus>`

where it will find the `ups/prodstatus.table` file to complete the EUPS setup of the product.

Jira dependency
---------------

The package is using jira module to report results to jira tickets.
At present time jira is not part of the lsst stack.
To use the package one need to install it locally.
To do this after `setup lsst_distrib` one can run

::

  `pip install jira`

For using jira one need to provide user authentication.
This is done by creating .netrc file in one's home directory.
The structure of the file should look like following:

::

  machine lsstjira
    account https://jira.lsstcorp.org
    login <username>
    password <user password>

Be sure to set 600 permission on the file.

