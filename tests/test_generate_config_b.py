from tests.test_base import BaseUnitTest
from tests.test_base import Context
from dmt_bucket import generate_config


class TestGenerateConfigB(BaseUnitTest):

    def test_generate_config(self):
        c = Context()
        config = generate_config(c)
        self.assertIsInstance(config, dict)

    def tearDown(self):
        pass