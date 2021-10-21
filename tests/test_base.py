import unittest
import os
from dm_helper import GaFlattenerDeploymentConfiguration
import sys


class Context(object):
    def __init__(self):
        self.properties = {}
        if sys.platform.startswith('linux'):  # if we're on a GitHub CI/CD VM
            self.env = {
                "deployment": "ga-flattener-deployment"
                , "project": "as-dev-ga4-flattener-320623"
                , "current_time": 1626736965
                , "project_number": "464892960897"
                , "username": "ruslan.bergenov@adswerve.com"
                , "name": "resource_name"
                , "type": "dmt_resource_type.py"}
        else:  # if we are testing locally
            self.env = {
                "deployment": "ga-flattener-deployment"
                , "project": "adswerve-mobile-qa"
                , "current_time": 1626736965
                , "project_number": "86260628829"
                , "username": "ruslan.bergenov@adswerve.com"
                , "name": "resource_name"
                , "type": "dmt_resource_type.py"}
        self.imports = {}


class BaseUnitTest(unittest.TestCase):
    DATASET = 'analytics_222460912'  # specific to your project
    TABLE_TYPE = 'events'  # or events_intraday
    DATE = '20211018'  # any historical date will suffice if that date shard exists in GA_SESSIONS_YYYYMMDD

    def setUp(self):
        context = Context()
        configuration = GaFlattenerDeploymentConfiguration(context.env)
        # Set user environment variables
        for key, value in configuration.user_environment_variables.items():
            os.environ[key] = value
        # this is needed for GitHub CI/CD
        # you can comment this out for local unit testing and auth with gcloud auth application-default login
        if sys.platform.startswith('linux'):  # if we're on a GitHub CI/CD VM
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.normpath(
                os.path.join(os.path.dirname(__file__), "..", "sandbox", "sa.json"))
            # otherwise, it will use a local path to application_default_credentials.json

