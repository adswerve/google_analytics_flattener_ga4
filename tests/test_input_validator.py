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
        SAMPLE_LOAD_DATA = {
            "protoPayload": {
                            "resourceName": f"projects/{project_id}/datasets/{dataset_id}/tables/{table_type}_{date_shard}"
        }}

        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(SAMPLE_LOAD_DATA).encode('utf-8'))}

        # validate input
        iv = InputValidator(SAMPLE_PUBSUB_MESSAGE)

        # checks
        self.assertEqual(date_shard, iv.table_date_shard)
        self.assertEqual(project_id, iv.gcp_project)
        self.assertEqual(dataset_id, iv.dataset)
        self.assertEqual(table_type, iv.table_type)
        assert isinstance(iv.valid_dataset(), bool)
        self.assertTrue(True)

    @log_capture()
    def test_input_validator_error(self, logcapture):
        # context and configuration
        c = Context()
        project_id = c.env["project"]
        dataset_id = c.env["dataset"]
        table_type = c.env["table_type"]

        # input
        SAMPLE_LOAD_DATA_INVALID_MISSING_DATE_SHARD = {
            "protoPayload": {
                            "resourceName": f"projects/{project_id}/datasets/{dataset_id}/tables/{table_type}"
        }}
        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(SAMPLE_LOAD_DATA_INVALID_MISSING_DATE_SHARD).encode('utf-8'))}

        # validate input. Error is expected
        iv = InputValidator(SAMPLE_PUBSUB_MESSAGE)

        message = "invalid message: {'protoPayload': {'resourceName': 'projects/%s/datasets/%s/tables/events'}}" % (project_id, dataset_id)
        # check log
        # https://testfixtures.readthedocs.io/en/latest/logging.html
        expected_log = ('root', 'CRITICAL', message
                        )

        logcapture.check_present(expected_log, )  # check that our expected_log message is present in InputValidator log
        # check_present means "contains"
        # check means "equals"

    def tearDown(self):
        pass


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
        SAMPLE_LOAD_DATA = {
            "protoPayload": {
                            "resourceName": f"projects/{project_id}/datasets/{dataset_id}/tables/{table_type}_{date_shard}"
        }}
        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(SAMPLE_LOAD_DATA).encode('utf-8'))}

        # validate input. Error is expected
        iv = InputValidator(SAMPLE_PUBSUB_MESSAGE)

        # check log
        expected_log = ('root', 'CRITICAL',
                        "flattener configuration error: 404 GET https://storage.googleapis.com/download/storage/v1/b/non-existing-bucket/o/config_datasets.json?alt=media: The specified bucket does not exist.: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>)")

        logcapture.check_present(expected_log, )  # check that our expected_log message is present in InputValidator log

    def tearDown(self):
        pass
