from tests.test_base import BaseUnitTest
from tests.test_base import Context
from dmt_cloud_function import GenerateConfig

class ContextCloudFunction(Context):
    def __init__(self):
        super(ContextCloudFunction, self).__init__()
        self.properties = {"availableMemoryMb": 128
            , "codeBucket": "bucket_name_for_code"
            , "codeLocation": "cf/"
            , "entryPoint": "<entry_point_function_name_in_main>"
            , "eventTrigger":
                  {"eventType": "providers/cloud.pubsub/eventTypes/topic.publish"
                , "resource": "projects/<project_name>/topics/<topic_name>"}
            , "location": "us-east1"
            , "runtime": "python37"
            , "timeout": "60s"}

        self.imports = {"cf/requirements.txt": "file-contents"
            , "cf/main.py": "file-contents"
            , "dmt_cloud_function.py":  "file-contents"}


class TestGenerateConfigCf(BaseUnitTest):
    def test_generate_config(self):
        c = ContextCloudFunction()
        config = GenerateConfig(c)
        self.assertIsInstance(config, dict)
