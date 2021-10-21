from tests.test_base import BaseUnitTest
from tests.test_base import Context
import json
import base64
from cf.main import InputValidator
from testfixtures import log_capture
from dm_helper import GaFlattenerDeploymentConfiguration
import os


class TestInputValidator(BaseUnitTest):
    def test_input_validator(self):
        # context and configuration
        c = Context()
        project_id = c.env["project"]
        dataset_id = c.env["dataset"]
        date_shard = c.env["date"]
        table_type = c.env["table_type"]

        # inputs
        SAMPLE_LOAD_DATA = {"protoPayload": {
            "serviceData": {"jobCompletedEvent": {"job": {"jobConfiguration": {"load": {"destinationTable": {
                "datasetId": dataset_id
                , "projectId": project_id
                , "tableId": "{table_type}_{date_shard}".format(table_type=table_type, date_shard=date_shard)
            }}}}}}}}
        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(SAMPLE_LOAD_DATA).encode('utf-8'))}

        # validate input
        iv = InputValidator(SAMPLE_PUBSUB_MESSAGE)

        # checks
        self.assertEqual(iv.table_date_shard, date_shard)
        self.assertEqual(iv.gcp_project, project_id)
        self.assertEqual(iv.dataset, dataset_id)
        self.assertEqual(iv.table_name, table_type)
        assert isinstance(iv.valid_dataset(), bool)
        self.assertTrue(True)

    @log_capture()
    def test_input_validator_attribute_error(self, logcapture):
        # context and configuration
        c = Context()
        project_id = c.env["project"]
        dataset_id = c.env["dataset"]
        date_shard = c.env["date"]
        table_type = c.env["table_type"]

        # input
        SAMPLE_LOAD_DATA_INVALID_MISSING_DATE_SHARD = {"protoPayload": {
            "serviceData": {"jobCompletedEvent": {"job": {"jobConfiguration": {"load": {"destinationTable": {
                "datasetId": dataset_id
                , "projectId": project_id
                , "tableId": "{table_type}".format(table_type=table_type)
            }}}}}}}}
        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(SAMPLE_LOAD_DATA_INVALID_MISSING_DATE_SHARD).encode('utf-8'))}

        # validate input. Error is expected
        iv = InputValidator(SAMPLE_PUBSUB_MESSAGE)

        message = "invalid message: {'protoPayload': {'serviceData': {'jobCompletedEvent': {'job': {'jobConfiguration': {'load': {'destinationTable': {'datasetId': '%s', 'projectId': '%s', 'tableId': 'events'}}}}}}}}" % (
        dataset_id, project_id)

        # check log
        # https://testfixtures.readthedocs.io/en/latest/logging.html
        expected_log = ('root', 'CRITICAL', message
                        )

        logcapture.check_present(expected_log, )  # check that our expected_log message is present in InputValidator log
        # check_present means "contains"
        # check means "equals"


class TestInputValidatorConfigurationError(BaseUnitTest):

    def setUp(self):
        context = Context()
        configuration = GaFlattenerDeploymentConfiguration(context.env)
        # Set user environment variables
        for key, value in configuration.user_environment_variables.items():
            if key == "CONFIG_BUCKET_NAME":
                os.environ[key] = "non-existing-bucket"  # set incorrect bucket name
            else:
                os.environ[key] = value

    @log_capture()
    def test_input_validator_Exception(self, logcapture):
        # context and configuration
        c = Context()
        project_id = c.env["project"]
        dataset_id = c.env["dataset"]
        date_shard = c.env["date"]
        table_type = c.env["table_type"]

        # input
        SAMPLE_LOAD_DATA = {"protoPayload": {
            "serviceData": {"jobCompletedEvent": {"job": {"jobConfiguration": {"load": {"destinationTable": {
                "datasetId": dataset_id
                , "projectId": project_id
                , "tableId": "{table_type}_{date_shard}".format(table_type=table_type, date_shard=date_shard)
            }}}}}}}}
        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(SAMPLE_LOAD_DATA).encode('utf-8'))}

        # validate input. Error is expected
        iv = InputValidator(SAMPLE_PUBSUB_MESSAGE)

        # check log
        expected_log = ('root', 'CRITICAL',
                        "flattener configuration error: 404 GET https://storage.googleapis.com/download/storage/v1/b/non-existing-bucket/o/config_datasets.json?alt=media: The specified bucket does not exist.: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>)")

        logcapture.check_present(expected_log, )  # check that our expected_log message is present in InputValidator log
