import unittest
import os
from dm_helper import GaFlattenerDeploymentConfiguration
import sys


class Context(object):
    def __init__(self):
        self.properties = {}
        #TODO: I put dataset , table_type and date under context env var, but probably they don't really belong here(?)
        if sys.platform.startswith('linux'):  # if we're on a GitHub CI/CD VM
            self.env = {
                "deployment": "ga-flattener-deployment"
                , "project": "as-dev-ga4-flattener-320623"
                , "current_time": 1626736965
                , "project_number": "464892960897"
                , "username": "ruslan.bergenov@adswerve.com"
                , "name": "resource_name"
                , "type": "dmt_resource_type.py"
                , "dataset": 'analytics_222460912'
                , "table_type": 'events'
                , "date": '20211013'
                # We need to also set GOOGLE_APPLICATION_CREDENTIALS on a Linux VM,
                # but it needs to be done under user env vars
            }
        else:  # if we are testing locally
            self.env = {
                "deployment": "ga-flattener-deployment"
                , "project": "as-dev-ruslan"
                , "current_time": 1626736965
                , "project_number": "522429634784"
                , "username": "ruslan.bergenov@adswerve.com"
                , "name": "resource_name"
                , "type": "dmt_resource_type.py"
                , "dataset": 'analytics_222460912'  # specific to your project
                , "table_type": 'events'
                , "date": '20211018'  # any historical date will suffice if that date shard exists in GA_EVENTS_YYYYMMDD

            }
        self.imports = {}


class BaseUnitTest(unittest.TestCase):

    def setUp(self):
        context = Context()
        configuration = GaFlattenerDeploymentConfiguration(context.env)
        # Set user environment variables
        for key, value in configuration.user_environment_variables.items():
            os.environ[key] = value
        # local unit testing:
        # we are not explicitly setting GOOGLE_APPLICATION_CREDENTIALS env var
        # for local testing, it will use a local path to application_default_credentials.json
        # you'll get when you run gcloud auth application-default login
        # if we're on a GitHub CI/CD VM
        # we need to set GOOGLE_APPLICATION_CREDENTIALS
        # it needs to be done under user env vars
        if sys.platform.startswith('linux'):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.normpath(
                os.path.join(os.path.dirname(__file__), "..", "sandbox", "sa.json"))
