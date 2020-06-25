from google.cloud import pubsub_v1
import os
import json

from  tests.test_base import BaseUnitTest
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./credentials/as-dev-ian-0ef537352615.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "c:\\auth_keys\\as-dev-gord-1522f36e41ad.json"
topic_name = "dev-gaflat-topic"
project_id = "as-dev-gord"
dataset_id = BaseUnitTest.DATASET
date_shard = BaseUnitTest.DATE

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

SAMPLE_LOAD_DATA = {"protoPayload": {
    "serviceData": {"jobCompletedEvent": {"job": {"jobConfiguration": {"load": {"destinationTable": {
        "datasetId": dataset_id
        , "projectId": project_id
        , "tableId": "ga_sessions_%s" % date_shard
    }}}}}}}}

print('Publishing backfill message to topic %s for %s.%s.ga_sessions_%s' % (topic_name,project_id, dataset_id, date_shard))
publisher.publish(topic_path, json.dumps(SAMPLE_LOAD_DATA).encode('utf-8'), origin='python-unit-test'
                              , username='gcp')

