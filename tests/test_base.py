import unittest
import os
from dm_helper import GaFlattenerDeploymentConfiguration

class Context(object):
    def __init__(self):
        self.properties = {}

        self.env = {
            "deployment": "deploy-unit-test"
            , "project": "analyticspros.com:spotted-cinnamon-834"
            , "current_time": 1592017736
            , "project_number": "425632468050"
            , "username": "first.last@domain.com"
            , "name": "resource_name"
            , "type": "dmt_resource_type.py"}

        self.imports = {}

class BaseUnitTest(unittest.TestCase):

    DATASET = 'unit_test'       #specific to your project
    TABLE_TYPE = 'ga_sessions'  #or ga_sessions_intra or ga_sessions_realtime
    DATE = '20200608'           #any historical date will suffice if that date shard exists in GA_SESSIONS_YYYYMMDD

    def setUp(self):
        context = Context()
        configuration = GaFlattenerDeploymentConfiguration(context.env)
        #Set user environment variables
        for key, value in configuration.user_environment_variables.items():
            os.environ[key] = value
