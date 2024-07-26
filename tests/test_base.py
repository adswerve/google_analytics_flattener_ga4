import unittest
import os
import sys
from google.cloud import bigquery
import logging

from dm_helper import GaFlattenerDeploymentConfiguration
from cfconfigbuilder.main import FlattenerDatasetConfig
from cfconfigbuilder.main import FlattenerDatasetConfigStorage


class Context(object):
    def __init__(self):
        self.properties = {}
        # TODO: I put dataset, table_type and date under context env var, but probably they don't really belong here(?)
        if sys.platform.startswith('linux'):  # if we're on a GitHub CI/CD VM
            self.env = {
                "deployment": "ga4-flattener-deployment"
                , "project": "as-dev-ga4-flattener-320623"
                , "current_time": 1626736965
                , "project_number": "464892960897"
                , "username": "ruslan.bergenov@adswerve.com"
                , "name": "resource_name"
                , "type": "dmt_resource_type.py"
                , "dataset": 'analytics_222460912'
                , "dataset_adswerve": "analytics_206551716"
                , "table_type": 'events'
                , "table_type_intraday": 'events_intraday'
                , "date": '20211013'
                , "date_collected_traffic_source_added": "20230503"
                , "date_is_active_user_added": "20230717"
                , "date_intraday": '20221114'
                # We need to also set GOOGLE_APPLICATION_CREDENTIALS on a Linux VM,
                # but it needs to be done under user env vars
            }
        else:  # if we are testing locally
            self.env = {
                "deployment": "ga4-flattener-deployment"
                , "project": "as-dev-ga4-flattener-320623"
                , "current_time": 1626736965
                , "project_number": "464892960897"
                , "username": "ruslan.bergenov@adswerve.com"
                , "name": "resource_name"
                , "type": "dmt_resource_type.py"
                , "dataset": 'analytics_222460912'  # specific to your project
                , "dataset_adswerve": "analytics_206551716"
                , "table_type": 'events'
                , "table_type_intraday": 'events_intraday'
                , "date": '20211013'
                , "date_collected_traffic_source_added": "20230503"
                , "date_is_active_user_added": "20230717"
                , "date_intraday": '20221114' # any historical date will suffice if that date shard exists in GA_EVENTS_YYYYMMDD

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

    def tearDown(self):
        self.delete_all_flat_tables_from_dataset()
        self.restore_default_config()

    def delete_all_flat_tables_from_dataset(self):

        logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
        context = Context()
        my_project_id = context.env['project']
        my_dataset_id = context.env['dataset']

        client = bigquery.Client(project=my_project_id)
        tables = list(
            client.list_tables(my_dataset_id))  # API request(s), now you have the list of tables in this dataset
        tables_to_delete = []
        logging.info("discovered flat tables:")
        for table in tables:
            if table.table_id.startswith("flat_"):  # will perform the action only if the table has the desired prefix
                tables_to_delete.append(table.table_id)
                logging.info(table.full_table_id)
        for table_id in tables_to_delete:
            table_path = f"{my_project_id}.{my_dataset_id}.{table_id}"
            client.delete_table(table_path)
            logging.info(f"deleted table {table_path}")


    def delete_all_flat_views_from_dataset(self):

        logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')
        context = Context()
        my_project_id = context.env['project']
        my_dataset_id = context.env['dataset']

        client = bigquery.Client(project=my_project_id)
        tables = list(
            client.list_tables(my_dataset_id))  # API request(s), now you have the list of tables in this dataset
        tables_to_delete = []
        logging.info("discovered flat views:")
        for table in tables:
            if table.table_id.startswith("view_flat_"):  # will perform the action only if the table has the desired prefix
                tables_to_delete.append(table.table_id)
                logging.info(table.full_table_id)
        for table_id in tables_to_delete:
            table_path = f"{my_project_id}.{my_dataset_id}.{table_id}"
            client.delete_table(table_path)
            logging.info(f"deleted view {table_path}")
    def restore_default_config(self):
        # generate config and upload it to GCS
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.reformat_config(json_config)
        json_config = config.add_output_format_params_into_config(json_config)
        json_config = config.add_intraday_params_into_config(json_config)
        store.upload_config(config=json_config)
