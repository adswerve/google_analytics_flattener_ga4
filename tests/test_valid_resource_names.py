from tests.test_base import BaseUnitTest
from dm_helper import GaFlattenerDeploymentConfiguration
from tests.test_base import Context


class TestValidResourceNames(BaseUnitTest):
    c = Context()
    c.env["deployment"] = "deployment-name-deployment-name-deployment-name-deployment-name"
    configuration = GaFlattenerDeploymentConfiguration(c.env)

    def test_get_bucket_name(self):
        assert len(self.c.env["deployment"]) == 63

        bucket_name = self.configuration.get_bucket_name()

        assert len(bucket_name) == 63

    def test_get_cloud_function_name(self):
        assert len(self.c.env["deployment"]) == 63

        cf_name_1 = self.configuration.get_cf_name(code_location="cf/")

        cf_name_2 = self.configuration.get_cf_name(code_location="cfintraday/")

        assert len(cf_name_1) == len(cf_name_2) == 63

    def tearDown(self):
        pass