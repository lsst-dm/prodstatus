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
# coding: utf-8
"""Test Workflow."""

import unittest
from os import environ
from pathlib import Path
from tempfile import TemporaryDirectory

from lsst.ctrl.bps import BpsConfig
from lsst.prodstatus.Workflow import Workflow

BPS_CONFIG_PATH = Path(
    environ["PRODSTATUS_DIR"], "tests", "data", "bps_config_base.yaml"
)
TEST_WORKFLOW_NAME = 'test'


class TestWorkflow(unittest.TestCase):
    def test_create(self):
        bps_config = BpsConfig(BPS_CONFIG_PATH)
        workflow = Workflow(bps_config, TEST_WORKFLOW_NAME)
        self.assertIsInstance(workflow.bps_config["campaign"], str)

    def test_file_save_load(self):
        bps_config = BpsConfig(BPS_CONFIG_PATH)
        workflow = Workflow(bps_config, TEST_WORKFLOW_NAME)

        with TemporaryDirectory() as temp_dir:
            workflow.to_files(Path(temp_dir))

            workflow_dir = Path(temp_dir).joinpath(TEST_WORKFLOW_NAME)
            read_workflow = Workflow.from_files(workflow_dir)
            self.assertEqual(
                workflow.bps_config["campaign"], read_workflow.bps_config["campaign"]
            )
