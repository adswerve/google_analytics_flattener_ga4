from google.cloud import pubsub_v1
import os
import json
import datetime, time

from tests.test_base import BaseUnitTest
try:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
except:
    print("setting GOOGLE_APPLICATION_CREDENTIALS env var")
    # TODO: uncomment the line that corresponds to your system
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./credentials/as-dev-ian-0ef537352615.json"    # mac
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "c:\\auth_keys\\AP Bootcamp 2019 - Gord-gordsserviceaccount-9eb6a157db12.json"  # windows
topic_name = "gafltnr-topic"  # pubsub topic your CF is subscribed to
project_id = "analyticspros.com:spotted-cinnamon-834"
IS_TEST = False  # set to False to backfill, True for unit testing
dry_run = False   # set to False to Backfill
datasets_to_backfill = ["102887025"]     #GA Views to backfill, "24973611"

#Seconds to sleep between each property date shard
SLEEP_TIME = 5  # throttling

# ga_sessions_YYYYMMDD tables of desired dates must exist in order to backfill.
# both start and end are inclusive
backfill_range_start = datetime.datetime(2015, 5, 26)
backfill_range_end = datetime.datetime(2016, 12, 5)  # datetime.datetime.today()

if IS_TEST:
    datasets_to_backfill = [BaseUnitTest.DATASET]
    y = int(BaseUnitTest.DATE[0:4])
    m = int(BaseUnitTest.DATE[4:6])
    d = int(BaseUnitTest.DATE[6:8])
    backfill_range_start = datetime.datetime(y, m, d)
    backfill_range_end = datetime.datetime(y, m, d)

num_days_in_backfill_range = int((backfill_range_end - backfill_range_start).days) + 1
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)


for db in range(0, num_days_in_backfill_range):
    date_shard = (backfill_range_end - datetime.timedelta(days=db)).strftime('%Y%m%d')
    for dataset_id in datasets_to_backfill:
        SAMPLE_LOAD_DATA = {"protoPayload": {
            "serviceData": {"jobCompletedEvent": {"job": {"jobConfiguration": {"load": {"destinationTable": {
                "datasetId": dataset_id
                , "projectId": project_id
                , "tableId": "ga_sessions_%s" % date_shard
            }}}}}}}}

        print('Publishing backfill message to topic %s for %s.%s.ga_sessions_%s' % (topic_name, project_id, dataset_id, date_shard))
        if not dry_run:
            publisher.publish(topic_path, json.dumps(SAMPLE_LOAD_DATA).encode('utf-8'), origin='python-unit-test'
                                          , username='gcp')
            time.sleep(SLEEP_TIME)

