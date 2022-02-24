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
import yaml

from lsst.prodstatus.Campaign import Campaign


class TestCampaign(unittest.TestCase):
    def setUp(self):
        ref_test_data_dir = (
            Path(environ["PRODSTATUS_DIR"]).joinpath("tests").joinpath("data")
        )
        self.test_dir_itself = TemporaryDirectory()
        self.test_dir = Path(self.test_dir_itself.name)
        ref_campaign_spec_path = ref_test_data_dir.joinpath("campaign.yaml")

        with open(ref_campaign_spec_path, "rt") as campaign_spec_io:
            campaign_spec = yaml.safe_load(campaign_spec_io)

        for key in ("exposures", "bps_config_base"):
            campaign_spec[key] = str(ref_test_data_dir.joinpath(campaign_spec[key]))

        self.campaign_yaml_path = self.test_dir.joinpath("campaign.yaml")
        with self.campaign_yaml_path.open("wt") as campaign_yaml_io:
            yaml.dump(campaign_spec, campaign_yaml_io)

    def tearDown(self):
        self.test_dir_itself.cleanup()

    def test_create_from_yaml(self):
        campaign = Campaign.create_from_yaml(self.campaign_yaml_path)

        with self.campaign_yaml_path.open("rt") as campaign_yaml_io:
            campaign_spec = yaml.safe_load(campaign_yaml_io)

        self.assertGreaterEqual(len(campaign.steps), len(campaign_spec["steps"]))

        self.assertEqual(campaign.name, campaign_spec["name"])

    def test_file_save_load(self):
        campaign = Campaign.create_from_yaml(self.campaign_yaml_path)
        test_campaign_name = campaign.name

        with TemporaryDirectory() as temp_dir:
            campaign.to_files(Path(temp_dir))

            campaign_dir = Path(temp_dir)
            read_campaign = campaign.from_files(campaign_dir, test_campaign_name)
            self.assertEqual(read_campaign.name, campaign.name)
