from tests.test_base import BaseUnitTest
from tests.test_base import Context
from dmt_cloud_function import generate_config


class ContextCloudFunction(Context):
    def __init__(self):
        super(ContextCloudFunction, self).__init__()
        self.properties = {"availableMemoryMb": 128
            , "codeBucket": "bucket_name_for_code"
            , "codeLocation": "cf/"
            , "entryPoint": "<entry_point_function_name_in_main>"
            , "triggerType": 'pubsub'  # also tried {}
            , "location": "us-east1"
            , "runtime": "python311"
            , "timeout": "60s"}

        self.imports = {"cf/requirements.txt": "file-contents"
            , "cf/main.py": "file-contents"
            , "dmt_cloud_function.py": "file-contents"}


class TestGenerateConfigCf(BaseUnitTest):
    def test_generate_config(self):
        c = ContextCloudFunction()
        config = generate_config(c)
        self.assertIsInstance(config, dict)

    def tearDown(self):
        pass