from tests.test_base import BaseUnitTest
from cfconfigbuilder.main import FlattenerDatasetConfig
from cfconfigbuilder.main import FlattenerDatasetConfigStorage
import json

class TestCFBuildFlattenerGaDatasetConfig(BaseUnitTest):
    
    def test_build_flattener_ga_dataset_config(self):
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        store.upload_config(config=json_config)
        self.assertTrue(("datasets" in json_config.keys()))
        self.assertIsInstance(json_config['datasets'],list)
        self.assertTrue(True)
