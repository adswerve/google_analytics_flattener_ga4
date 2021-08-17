from google.cloud import pubsub_v1
import json
import datetime, time
from tests.test_base import BaseUnitTest
import logging
import sys

# configure logger to add log cal to stdout call (i.e., to print log message to console)
# create logger
root = logging.getLogger()
root.setLevel(logging.INFO) # what log severity are we going to capture?

# create console handler and set level
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO) # out of the logs we captured above, what log severity are we going to add to stdout (print to console)?

# create formatter
formatter = logging.Formatter('%(levelname)s - %(message)s')

# add formatter to ch
handler.setFormatter(formatter)

root.addHandler(handler)
# https://stackoverflow.com/questions/14058453/making-python-loggers-output-all-messages-to-stdout-in-addition-to-log-file
# https://docs.python.org/3/howto/logging.html

# To authenticate, run the following command.  The account you choose will execute this python script
# gcloud auth application-default login

'''*****************************'''
''' Configuration Section Start '''
'''*****************************'''
topic_id = "ga-flattener-deployment-topic"  # pubsub topic your cloud function is subscribed to Example: [Deployment Name]-topic
project_id = "as-dev-ga4-flattener-320623"  # GCP project ID, example:  [PROJECT_ID]
dry_run = False   # set to False to Backfill.  Setting to True will not pubish any messages to pubsub, but simply show what would have been published.
# Desired dates to backfill, both start and end are inclusive
backfill_range_start = datetime.datetime(2021, 7, 27)
backfill_range_end = datetime.datetime(2021, 7, 28)  # datetime.datetime.today()
datasets_to_backfill = ["analytics_222460912"]     #GA properties to backfill, "analytics_222460912"
'''*****************************'''
'''  Configuration Section End  '''
'''*****************************'''
#Seconds to sleep between each property date shard
SLEEP_TIME = 5  # throttling

#Unit Testing flag
IS_TEST = False  # set to False to backfill, True for unit testing


if IS_TEST:
    datasets_to_backfill = [BaseUnitTest.DATASET]
    y = int(BaseUnitTest.DATE[0:4])
    m = int(BaseUnitTest.DATE[4:6])
    d = int(BaseUnitTest.DATE[6:8])
    backfill_range_start = datetime.datetime(y, m, d)
    backfill_range_end = datetime.datetime(y, m, d)

num_days_in_backfill_range = int((backfill_range_end - backfill_range_start).days) + 1
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


for db in range(0, num_days_in_backfill_range):
    date_shard = (backfill_range_end - datetime.timedelta(days=db)).strftime('%Y%m%d')
    for dataset_id in datasets_to_backfill:
        SAMPLE_LOAD_DATA = {"protoPayload": {
            "serviceData": {"jobCompletedEvent": {"job": {"jobConfiguration": {"load": {"destinationTable": {
                "datasetId": dataset_id
                , "projectId": project_id
                , "tableId": "events_%s" % date_shard
            }}}}}}}}

        logging.info('Publishing backfill message to topic %s for %s.%s.events_%s' % (topic_id, project_id, dataset_id, date_shard))
        if not dry_run:
            publisher.publish(topic_path, json.dumps(SAMPLE_LOAD_DATA).encode('utf-8'), origin='python-unit-test'
                                          , username='gcp')
            time.sleep(SLEEP_TIME)

