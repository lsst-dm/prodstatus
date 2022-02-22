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
from unittest import mock

import numpy as np
import pandas as pd
import jira

from lsst.ctrl.bps import BpsConfig
from lsst.prodstatus.Workflow import Workflow

BPS_CONFIG_PATH = Path(
    environ["PRODSTATUS_DIR"], "tests", "data", "bps_config_base.yaml"
)
TEST_WORKFLOW_NAME = "test"


class TestWorkflow(unittest.TestCase):
    def test_init(self):
        bps_config = BpsConfig(BPS_CONFIG_PATH)
        workflow = Workflow(bps_config, TEST_WORKFLOW_NAME)
        self.assertIsInstance(workflow.bps_config["campaign"], str)

    def test_file_save_load(self):
        bps_config = BpsConfig(BPS_CONFIG_PATH)
        workflow = Workflow(bps_config, TEST_WORKFLOW_NAME)

        with TemporaryDirectory() as temp_dir:
            workflow.to_files(Path(temp_dir))

            workflow_dir = Path(temp_dir)
            read_workflow = Workflow.from_files(workflow_dir, TEST_WORKFLOW_NAME)
            self.assertEqual(
                workflow.bps_config["campaign"], read_workflow.bps_config["campaign"]
            )

    def test_split_by_band(self):
        bps_config = BpsConfig(BPS_CONFIG_PATH)
        full_workflow = Workflow(bps_config, TEST_WORKFLOW_NAME)
        bands = "ugrizy"
        split_workflows = full_workflow.split_by_band(bands)
        for band, workflow in zip(bands, split_workflows):
            self.assertEqual(workflow.band, band)

    def test_split_by_exp(self):
        bps_config = BpsConfig(BPS_CONFIG_PATH)
        test_exps = pd.DataFrame(
            {
                "band": ["g", "g", "r", "g", "i", "i", "r"],
                "exp_id": [1, 3, 4, 5, 10, 11, 12],
            }
        )
        full_workflow = Workflow(bps_config, TEST_WORKFLOW_NAME, exposures=test_exps)

        group_size = 3
        split_workflows = full_workflow.split_by_exposure(group_size)
        for workflow in split_workflows:
            self.assertLessEqual(len(workflow.exposures), group_size)
        self.assertEqual(len(split_workflows), np.ceil(len(test_exps) / group_size))

        combined_exps = pd.concat(w.exposures for w in split_workflows)
        self.assertTrue(combined_exps.equals(test_exps))

    def test_create_many(self):
        bps_config = BpsConfig(BPS_CONFIG_PATH)
        test_exps = pd.DataFrame(
            {
                "band": ["g", "g", "r", "g", "i", "i", "r"],
                "exp_id": [1, 3, 4, 5, 10, 11, 12],
            }
        )
        step_configs = {
            "step1": {"split_bands": False, "exposure_groups": {"group_size": 3}},
            "step2": {"split_bands": True, "exposure_groups": {"group_size": 2}},
            "step3": {
                "split_bands": False,
                "exposure_groups": {"group_size": 2, "skip_groups": 1, "num_groups": 2},
            },
        }
        workflows = Workflow.create_many(bps_config, step_configs, test_exps)

        step_workflows = {}
        for w in workflows:
            if w.step not in step_workflows:
                step_workflows[w.step] = []
            step_workflows[w.step].append(w)

        # breakpoint()
        self.assertEqual(len(step_workflows["step1"]), 3)
        for w in step_workflows["step1"]:
            self.assertLessEqual(
                len(w.exposures), step_configs["step1"]["exposure_groups"]["group_size"]
            )

        for w in step_workflows["step2"]:
            self.assertLessEqual(len(w.exposures.band.unique()), 1)
            self.assertLessEqual(
                len(w.exposures), step_configs["step2"]["exposure_groups"]["group_size"]
            )

        self.assertEqual(
            len(step_workflows["step3"]),
            step_configs["step3"]["exposure_groups"]["num_groups"],
        )

    @mock.patch("jira.JIRA", autospec=True)
    def test_load_save_jira(self, MockJIRA):
        this_jira = jira.JIRA(options={"server": ''}, basic_auth=('',''))
        bps_config = BpsConfig(BPS_CONFIG_PATH)
        workflow = Workflow(bps_config, TEST_WORKFLOW_NAME)

        issue = workflow.to_jira(this_jira)
        
        #reread_workflow = Workflow.from_jira(issue)
        #self.assertIsInstance(reread_workflow, Workflow)
        
        