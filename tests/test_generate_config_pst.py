from tests.test_base import BaseUnitTest
from tests.test_base import Context
from dmt_pubsub_topic import generate_config


class TestGenerateConfigPst(BaseUnitTest):

    def test_generate_config(self):
        c = Context()
        config = generate_config(c)
        self.assertIsInstance(config, dict)
