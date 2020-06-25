from tests.test_base import BaseUnitTest
from tests.test_base import Context
import json
import base64
from cf.main import InputValidator

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