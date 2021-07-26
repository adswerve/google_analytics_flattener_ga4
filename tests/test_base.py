import unittest
import os
from dm_helper import GaFlattenerDeploymentConfiguration


class Context(object):
    def __init__(self):
        self.properties = {}
        self.env = {
            "deployment": "ga-flattener-deployment"
            , "project": "as-dev-ga4-flattener-320623"
            , "current_time": 1626736965
            , "project_number": "464892960897"
            , "username": "ruslan.bergenov@adswerve.com"
            , "name": "resource_name"
            , "type": "dmt_resource_type.py"}

        self.imports = {}


class BaseUnitTest(unittest.TestCase):
    DATASET = 'analytics_222460912'       #specific to your project
    TABLE_TYPE = 'events'  #or events_intraday
    DATE = '20210720'           #any historical date will suffice if that date shard exists in GA_SESSIONS_YYYYMMDD

    def setUp(self):
        context = Context()
        configuration = GaFlattenerDeploymentConfiguration(context.env)
        #Set user environment variables
        for key, value in configuration.user_environment_variables.items():
            os.environ[key] = value
