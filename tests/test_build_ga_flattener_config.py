from tests.test_base import BaseUnitTest
from cfconfigbuilder.main import FlattenerDatasetConfig
from cfconfigbuilder.main import FlattenerDatasetConfigStorage
from cfconfigbuilderps.main import FlattenerDatasetConfig as FlattenerDatasetConfigPS
from cfconfigbuilderps.main import FlattenerDatasetConfigStorage as FlattenerDatasetConfigStoragePS


# TODO: question: what's the diff between these two unit tests? test_build_flattener_ga_dataset_config vs test_build_flattener_ga_dataset_config_ps

# TODO: merge some of the repetitive unit tests

class TestCFBuildFlattenerGaDatasetConfig(BaseUnitTest):

    def test_build_flattener_ga_dataset_config_default(self):

        # generate config and upload it to GCS
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.add_intraday_params_into_config(json_config)
        json_config = config.add_output_params_into_config(json_config)
        store.upload_config(config=json_config)
        # check
        self.assertIsInstance(json_config, dict)
        if json_config.keys():
            for key, value in json_config.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, dict)

                self.assertEqual({
                    "frequency": None,
                    "units": "hours"
                },
                    json_config[key]['intraday_schedule'])

                self.assertEqual({
                    "sharded": True,
                    "partitioned": False
                }, json_config[key]['output'])
                # TODO: in all other examples of assertEqual, swap the order
                # syntax is this: assertEqual(expected, actual)
                # we got them the other way round
            # https://stackoverflow.com/questions/17920625/what-is-actually-assertequals-in-python

        self.assertTrue(True)

    def test_build_flattener_ga_dataset_config_ps(self):
        # generate config and upload it to GCS
        config = FlattenerDatasetConfigPS()
        store = FlattenerDatasetConfigStoragePS()
        json_config = config.get_ga_datasets()
        json_config = config.add_intraday_params_into_config(json_config)
        json_config = config.add_output_params_into_config(json_config)
        store.upload_config(config=json_config)
        # check
        self.assertIsInstance(json_config, dict)
        if json_config.keys():
            for key, value in json_config.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, dict)
        self.assertTrue(True)

    def test_build_flattener_ga_dataset_config_add_intraday_schedule_minutes(self):
        # generate config and upload it to GCS
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.add_intraday_params_into_config(json_config, intraday_schedule_frequency=30,
                                                             intraday_schedule_units="minutes")
        json_config = config.add_output_params_into_config(json_config)
        store.upload_config(config=json_config)
        # check

        self.assertIsInstance(json_config, dict)
        if json_config.keys():
            for key, value in json_config.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, dict)

                # default intraday schedule
                self.assertEqual(json_config[key]['intraday_schedule'], {
                    "frequency": 30,
                    "units": "minutes"
                })

        self.assertTrue(True)

    def test_build_flattener_ga_dataset_config_add_intraday_schedule_hours(self):

        # generate config and upload it to GCS
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.add_intraday_params_into_config(json_config, intraday_schedule_frequency=1,
                                                             intraday_schedule_units="hours")
        json_config = config.add_output_params_into_config(json_config)
        store.upload_config(config=json_config)
        # check

        self.assertIsInstance(json_config, dict)
        if json_config.keys():
            for key, value in json_config.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, dict)

                self.assertEqual(json_config[key]['intraday_schedule'], {
                    "frequency": 1,
                    "units": "hours"
                })

        self.assertTrue(True)

    def test_build_flattener_ga_dataset_config_add_custom_output_params(self):
        # generate config and upload it to GCS
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.add_intraday_params_into_config(json_config)
        json_config = config.add_output_params_into_config(json_config, output_sharded=False,
                                                           output_partitioned=True)
        store.upload_config(config=json_config)
        # check
        self.assertIsInstance(json_config, dict)
        if json_config.keys():
            for key, value in json_config.items():
                self.assertIsInstance(key, str)
                self.assertIsInstance(value, dict)

                self.assertEqual({
                    "sharded": False,
                    "partitioned": True
                }, json_config[key]['output'])

        self.assertTrue(True)

    def tearDown(self):
        self.restore_default_config()