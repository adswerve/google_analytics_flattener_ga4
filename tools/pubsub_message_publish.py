from google.cloud import pubsub_v1
import json
import datetime, time
from tests.test_base import Context
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
c = Context()

# To authenticate, run the following command.  The account you choose will execute this python script
# gcloud auth application-default login

'''*****************************'''
''' Configuration Section Start '''
'''*****************************'''
topic_id = "ga4-flattener-deployment-topic"  # pubsub topic your cloud function is subscribed to Example: [Deployment Name]-topic
project_id = "as-dev-ga4-flattener-320623" # GCP project ID, example:  [PROJECT_ID]
dry_run = False  # set to False to Backfill.  Setting to True will not pubish any messages to pubsub, but simply show what would have been published.
# Desired dates to backfill, both start and end are inclusive
backfill_range_start = datetime.datetime(2021, 7, 20)
backfill_range_end = datetime.datetime(2021, 7, 20)  # datetime.datetime.today()
datasets_to_backfill = ["analytics_222460912"]  # GA properties to backfill, "analytics_222460912"
table_type = "events"
'''*****************************'''
'''  Configuration Section End  '''
'''*****************************'''
# Seconds to sleep between each property date shard
SLEEP_TIME = 5  # throttling

# Unit Testing flag
IS_TEST = False  # set to False to backfill, True for unit testing

if IS_TEST:
    datasets_to_backfill = [c.env["dataset"]]
    y = int(c.env["date"][0:4])
    m = int(c.env["date"][4:6])
    d = int(c.env["date"][6:8])
    backfill_range_start = datetime.datetime(y, m, d)
    backfill_range_end = datetime.datetime(y, m, d)

num_days_in_backfill_range = int((backfill_range_end - backfill_range_start).days) + 1
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

for db in range(0, num_days_in_backfill_range):
    date_shard = (backfill_range_end - datetime.timedelta(days=db)).strftime('%Y%m%d')
    for dataset_id in datasets_to_backfill:
        SAMPLE_LOAD_DATA = {
            "protoPayload": {
                "resourceName": f"projects/{project_id}/datasets/{dataset_id}/tables/{table_type}_{date_shard}"
            }
        }
        logging.info(f"Publishing backfill message to topic {topic_id} for {project_id}.{dataset_id}.events_{date_shard}")
        if not dry_run:
            publisher.publish(topic_path, json.dumps(SAMPLE_LOAD_DATA).encode('utf-8'), origin='python-unit-test'
                              , username='gcp')
            time.sleep(SLEEP_TIME)
