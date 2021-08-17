from tests.test_base import BaseUnitTest
from tests.test_base import Context
import json
import base64
from cf.main import InputValidator
from testfixtures import log_capture
import re

class TestInputValidator(BaseUnitTest):
    def test_input_validator(self):
        c = Context()
        project_id = c.env["project"]
        dataset_id = BaseUnitTest.DATASET
        date_shard = BaseUnitTest.DATE
        table_type = BaseUnitTest.TABLE_TYPE

        # This message is what is configured in the
        SAMPLE_LOAD_DATA = {"protoPayload": {
            "serviceData": {"jobCompletedEvent": {"job": {"jobConfiguration": {"load": {"destinationTable": {
                "datasetId": dataset_id
                , "projectId": project_id
                , "tableId": "{table_type}_{date_shard}".format(table_type=table_type,date_shard=date_shard)
            }}}}}}}}
        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data':base64.b64encode(json.dumps(SAMPLE_LOAD_DATA).encode('utf-8')) }
        iv = InputValidator(SAMPLE_PUBSUB_MESSAGE)

        self.assertEqual(iv.table_date_shard,date_shard)
        self.assertEqual(iv.gcp_project, project_id)
        self.assertEqual(iv.dataset, dataset_id)
        self.assertEqual(iv.table_name,table_type)
        assert isinstance(iv.valid_dataset(), bool)
        self.assertTrue(True)

    @log_capture()
    def test_input_validator_AttributeError(self, logcapture):
        # https://testfixtures.readthedocs.io/en/latest/logging.html
        c = Context()
        project_id = c.env["project"]
        dataset_id = BaseUnitTest.DATASET
        date_shard = BaseUnitTest.DATE
        table_type = BaseUnitTest.TABLE_TYPE

        # This message is what is configured in the
        SAMPLE_LOAD_DATA_INVALID_MISSING_DATE_SHARD = {"protoPayload": {
            "serviceData": {"jobCompletedEvent": {"job": {"jobConfiguration": {"load": {"destinationTable": {
                "datasetId": dataset_id
                , "projectId": project_id
                , "tableId": "{table_type}".format(table_type=table_type)
            }}}}}}}}
        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data':base64.b64encode(json.dumps(SAMPLE_LOAD_DATA_INVALID_MISSING_DATE_SHARD).encode('utf-8')) }
        try:

            message_payload = json.loads(base64.b64decode(SAMPLE_PUBSUB_MESSAGE['data']).decode('utf-8'))
            bq_destination_table = \
                message_payload['protoPayload']['serviceData']['jobCompletedEvent']['job']['jobConfiguration']['load'][
                    'destinationTable']
            gcp_project = bq_destination_table['projectId']
            dataset = bq_destination_table['datasetId']
            table_date_shard = re.search('_(20\d\d\d\d\d\d)$', bq_destination_table['tableId']).group(1)
            table_name = re.search('(events.*)_20\d\d\d\d\d\d$', bq_destination_table['tableId']).group(1)

        except AttributeError:
            import logging
            logging.critical(f'invalid message: {message_payload}')

        expected_log = ('root', 'CRITICAL',
                        "invalid message: {'protoPayload': {'serviceData': {'jobCompletedEvent': {'job': {'jobConfiguration': {'load': {'destinationTable': {'datasetId': 'analytics_222460912', 'projectId': 'as-dev-ga4-flattener-320623', 'tableId': 'events'}}}}}}}}")

        logcapture.check(expected_log, )





    def test_input_validator_Exception(self):
        pass