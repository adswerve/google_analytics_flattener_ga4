from tests.test_base import BaseUnitTest
from cfconfigbuilder.main import FlattenerDatasetConfig
from cfconfigbuilder.main import FlattenerDatasetConfigStorage
from cfconfigbuilderps.main import FlattenerDatasetConfig as FlattenerDatasetConfigPS
from cfconfigbuilderps.main import FlattenerDatasetConfigStorage as FlattenerDatasetConfigStoragePS
import json
import logging
import sys
import re

# display logs in console while running unit tests
# https://stackoverflow.com/questions/14058453/making-python-loggers-output-all-messages-to-stdout-in-addition-to-log-file

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

# TODO: question: what's the diff between these two unit tests? test_build_flattener_ga_dataset_config vs test_build_flattener_ga_dataset_config_ps

# TODO: merge some of the repetitive unit tests

class TestCFBuildFlattenerGaDatasetConfig(BaseUnitTest):
    pattern = re.compile("^analytics\\_\\d+$")

    def test_build_flattener_ga_dataset_config_default(self):

        # generate config and upload it to GCS
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.reformat_config(json_config)
        json_config = config.add_output_format_params_into_config(json_config)
        json_config = config.add_intraday_params_into_config(json_config)
        store.upload_config(config=json_config)
        logging.info(f"build_ga_flattener_config: {json.dumps(json_config)}")
        # check
        self.assertIsInstance(json_config, dict)
        if json_config.keys():
            for key, value in json_config.items():
                self.assertIsInstance(key, str)
                assert self.pattern.match(key)

                self.assertIsInstance(value, dict)

                self.assertEqual({
                    "intraday_flat_tables_schedule": None,
                    "intraday_flat_views": True
                },
                    json_config[key]['intraday_flattening'])

                self.assertEqual({
                    "sharded": True,
                    "partitioned": False
                }, json_config[key]['output_format'])
                # assertEqual syntax is this: assertEqual(expected, actual)
                # https://stackoverflow.com/questions/17920625/what-is-actually-assertequals-in-python

        self.assertTrue(True)

    def test_build_flattener_ga_dataset_config_ps(self):
        # generate config and upload it to GCS
        config = FlattenerDatasetConfigPS()
        store = FlattenerDatasetConfigStoragePS()
        json_config = config.get_ga_datasets()
        json_config = config.reformat_config(json_config)
        json_config = config.add_output_format_params_into_config(json_config)
        json_config = config.add_intraday_params_into_config(json_config)
        store.upload_config(config=json_config)
        logging.info(f"build_ga_flattener_config: {json.dumps(json_config)}")
        # check
        self.assertIsInstance(json_config, dict)
        if json_config.keys():
            for key, value in json_config.items():
                self.assertIsInstance(key, str)
                assert self.pattern.match(key)

                self.assertIsInstance(value, dict)

                self.assertEqual({
                    "intraday_flat_tables_schedule": None,
                    "intraday_flat_views": True
                },
                    json_config[key]['intraday_flattening'])

                self.assertEqual({
                    "sharded": True,
                    "partitioned": False
                }, json_config[key]['output_format'])
                # assertEqual syntax is this: assertEqual(expected, actual)
                # https://stackoverflow.com/questions/17920625/what-is-actually-assertequals-in-python

        self.assertTrue(True)

    def test_build_flattener_ga_dataset_config_add_intraday_schedule_minutes(self):
        # generate config and upload it to GCS
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.reformat_config(json_config)
        json_config = config.add_output_format_params_into_config(json_config)
        json_config = config.add_intraday_params_into_config(json_config, intraday_flat_tables_schedule={
                                                      "frequency": 30,
                                                      "units": "minutes"
                                                    })
        store.upload_config(config=json_config)
        logging.info(f"build_ga_flattener_config: {json.dumps(json_config)}")

        # check
        self.assertIsInstance(json_config, dict)
        if json_config.keys():
            for key, value in json_config.items():
                self.assertIsInstance(key, str)
                assert self.pattern.match(key)
                self.assertIsInstance(value, dict)

                # intraday schedule
                self.assertEqual({
                    "intraday_flat_tables_schedule": {
                                                      "frequency": 30,
                                                      "units": "minutes"
                                                    },
                    "intraday_flat_views": True
                },
                    json_config[key]['intraday_flattening'])

        self.assertTrue(True)

    def test_build_flattener_ga_dataset_config_add_intraday_schedule_hours(self):

        # generate config and upload it to GCS
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.reformat_config(json_config)
        json_config = config.add_output_format_params_into_config(json_config)
        json_config = config.add_intraday_params_into_config(json_config, intraday_flat_tables_schedule={
            "frequency": 1,
            "units": "hours"
        })
        store.upload_config(config=json_config)
        logging.info(f"build_ga_flattener_config: {json.dumps(json_config)}")

        # check
        self.assertIsInstance(json_config, dict)
        if json_config.keys():
            for key, value in json_config.items():
                self.assertIsInstance(key, str)
                assert self.pattern.match(key)
                self.assertIsInstance(value, dict)

                # intraday schedule
                self.assertEqual({
                    "intraday_flat_tables_schedule": {
                        "frequency": 1,
                        "units": "hours"
                    },
                    "intraday_flat_views": True
                },
                    json_config[key]['intraday_flattening'])

        self.assertTrue(True)

    def test_build_flattener_ga_dataset_config_add_custom_output_params(self):
        # generate config and upload it to GCS
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.reformat_config(json_config)
        json_config = config.add_output_format_params_into_config(json_config, output_sharded=False,
                                                                  output_partitioned=True)
        json_config = config.add_intraday_params_into_config(json_config)
        store.upload_config(config=json_config)
        logging.info(f"build_ga_flattener_config: {json.dumps(json_config)}")
        # check
        self.assertIsInstance(json_config, dict)
        if json_config.keys():
            for key, value in json_config.items():
                self.assertIsInstance(key, str)
                assert self.pattern.match(key)
                self.assertIsInstance(value, dict)

                self.assertEqual({
                    "sharded": False,
                    "partitioned": True
                }, json_config[key]['output_format'])

        self.assertTrue(True)

    def tearDown(self):
        pass
        self.restore_default_config()
