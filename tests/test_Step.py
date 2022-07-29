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

import numpy as np
import pandas as pd

from lsst.ctrl.bps import BpsConfig
from lsst.prodstatus.Step import Step

BPS_CONFIG_PATH = Path(
    environ["PRODSTATUS_DIR"], "tests", "data", "bps_config_base.yaml"
)
TEST_STEP_NAME = "teststep"
TEST_WORKFLOW_BASE_NAME = "testwf"


class TestStep(unittest.TestCase):
    def test_generate_new(self):
        bps_config = BpsConfig(BPS_CONFIG_PATH)

        exposures = pd.DataFrame(
            {
                "band": ["g", "g", "r", "g", "i", "i", "r"],
                "exp_id": [1, 3, 4, 5, 10, 11, 12],
            }
        )

        # Test without splitting by bands or exposures
        split_bands = False
        exposure_groups = {}
        step = Step.generate_new(
            TEST_STEP_NAME,
            bps_config,
            split_bands,
            exposure_groups,
            exposures,
            workflow_base_name=TEST_WORKFLOW_BASE_NAME,
        )
        self.assertEqual(len(step.workflows), 1)

        # Test splitting by bands, but not exposures
        split_bands = True
        exposure_groups = {}
        step = Step.generate_new(
            TEST_STEP_NAME,
            bps_config,
            split_bands,
            exposure_groups,
            exposures,
            workflow_base_name=TEST_WORKFLOW_BASE_NAME,
        )
        self.assertEqual(len(step.workflows), 3)

        # Test splitting by exposures, but not bands
        split_bands = False
        group_size = 3
        exposure_groups = {"group_size": group_size}
        step = Step.generate_new(
            TEST_STEP_NAME,
            bps_config,
            split_bands,
            exposure_groups,
            exposures,
            workflow_base_name=TEST_WORKFLOW_BASE_NAME,
        )
        self.assertEqual(len(step.workflows), int(np.ceil(len(exposures) / group_size)))

        # Test splitting by bands and exposures
        split_bands = True
        group_size = 2
        exposure_groups = {"group_size": group_size}
        step = Step.generate_new(
            TEST_STEP_NAME,
            bps_config,
            split_bands,
            exposure_groups,
            exposures,
            workflow_base_name=TEST_WORKFLOW_BASE_NAME,
        )

        # Count up how many workflows we expect in each band, and sum them
        # to get the total workflows we expect
        num_workflows = 0
        for band in "ugrizy":
            band_exposures = exposures.query(f"band == '{band}'")
            num_workflows_in_band = int(np.ceil(len(band_exposures) / group_size))
            num_workflows += num_workflows_in_band
        self.assertEqual(len(step.workflows), num_workflows)

    def test_file_save_load(self):
        bps_config = BpsConfig(BPS_CONFIG_PATH)
        exposures = pd.DataFrame(
            {
                "band": ["g", "g", "r", "g", "i", "i", "r"],
                "exp_id": [1, 3, 4, 5, 10, 11, 12],
            }
        )
        split_bands = True
        group_size = 2
        exposure_groups = {"group_size": group_size}
        step = Step.generate_new(
            TEST_STEP_NAME,
            bps_config,
            split_bands,
            exposure_groups,
            exposures,
            workflow_base_name=TEST_WORKFLOW_BASE_NAME,
        )

        with TemporaryDirectory() as temp_dir:
            step_dir = Path(temp_dir)
            step.to_files(step_dir)
            read_step = Step.from_files(step_dir, TEST_STEP_NAME)

        self.assertEqual(step.name, read_step.name)
        self.assertEqual(step.split_bands, read_step.split_bands)
        self.assertEqual(len(step.workflows), len(read_step.workflows))
        for workflow, read_workflow in zip(step.workflows, read_step.workflows):
            self.assertEqual(workflow.name, read_workflow.name)
            self.assertEqual(workflow.step, read_workflow.step)
            self.assertEqual(workflow.band, read_workflow.band)
            self.assertEqual(workflow.issue_name, read_workflow.issue_name)
