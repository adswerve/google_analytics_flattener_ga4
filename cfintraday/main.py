

# TODO: how do I create a cron job?

# TODO: how do I delete a cron job?

# TODO: to delete cron job, do I need to give it a name? how do I do it? it'd be nice to just call it by dataset and table type and date shard

# TODO: not configured to run

# 2021-10-14 21:25:33.006 MDT
# ga-flattener-deployment-cf
# jcru48cvegqg
# Function execution took 1338 ms, finished with status: 'ok'
# Info
# 2021-10-14 21:25:33.002 MDT
# ga-flattener-deployment-cf
# jcru48cvegqg
# events flattening query for analytics_222460912 not configured to run
# Info
# 2021-10-14 21:25:33.002 MDT
# ga-flattener-deployment-cf
# jcru48cvegqg
# items flattening query for analytics_222460912 not configured to run
# Info
# 2021-10-14 21:25:33.002 MDT
# ga-flattener-deployment-cf
# jcru48cvegqg
# user_properties flattening query for analytics_222460912 not configured to run
# Info
# 2021-10-14 21:25:33.002 MDT
# ga-flattener-deployment-cf
# jcru48cvegqg
# event_params flattening query for analytics_222460912 not configured to run

#TODO: create a log router

#TODO: create a pub/sub topic

# TODO: link this Cloud Function to new pub/sub topic

# TODO: this function works locally but doesn't run on GCP as a a Cloud Function, fails with permissions error

# TODO: nice-to-have: refactor f strings, so we use the same format (?)

import base64
import json
from google.cloud import storage
from google.cloud import scheduler
import re
import os
import tempfile


class InputValidatorScheduler(object):
    def __init__(self, event):
        try:
            message_payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
            self.method_name = message_payload['protoPayload']["methodName"]

            if self.method_name == "tableservice.insert":
                bq_destination_table = \
                    message_payload['protoPayload']['serviceData']["tableInsertResponse"]["resource"]["tableName"]
                self.gcp_project = bq_destination_table['projectId']
                self.dataset = bq_destination_table['datasetId']
                self.table_date_shard = re.search('_(20\d\d\d\d\d\d)$', bq_destination_table['tableId']).group(1)
                self.table_name = re.search('(events_intraday)_20\d\d\d\d\d\d$', bq_destination_table['tableId']).group(
                    1)
                print("test")

            elif self.method_name == "tableservice.delete":
                bq_destination_table = message_payload['protoPayload']["authorizationInfo"][0]["resource"]
                #  https://www.kite.com/python/answers/how-to-get-the-substring-between-two-markers-in-python
                self.gcp_project = re.search(
                    'projects/(.*?)/datasets/analytics_',
                    bq_destination_table)

                self.gcp_project = re.search(
                    'projects\/(.*?)\/datasets\/analytics_\d\d\d\d\d\d\d\d\d\/tables\/events_intraday_20\d\d\d\d\d\d$',
                    bq_destination_table).group(1)

                # self.dataset = re.search(
                #     '\/datasets\(analytics_\d\d\d\d\d\d\d\d\d)/\/tables\/events_intraday_20\d\d\d\d\d\d$',
                #     bq_destination_table).group(1)
                self.table_date_shard = re.search('_(20\d\d\d\d\d\d)$', bq_destination_table).group(1)
                self.table_name = re.search('(events_intraday)_20\d\d\d\d\d\d$', bq_destination_table).group(1)
                print("test")

        except AttributeError or KeyError:
            print(f'invalid message: {message_payload}')
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(os.environ["CONFIG_BUCKET_NAME"])
            blob = bucket.blob(os.environ["CONFIG_FILENAME"])
            downloaded_file = os.path.join(tempfile.gettempdir(), "tmp.json")
            blob.download_to_filename(downloaded_file)
            with open(downloaded_file, "r") as config_json:
                self.config = json.load(config_json)
        except Exception as e:
            print(f'flattener configuration error: {e}')

    def valid_dataset(self):
        valid = self.dataset in self.config.keys()
        return valid

    def schedule(self):
        schedule = self.config[self.dataset]["intraday_schedule"]
        return schedule


def manage_intraday_schedule(event, context="context"):
    """Create a job with an App Engine target via the Cloud Scheduler API"""
    # [START cloud_scheduler_create_job]

    input_event = InputValidatorScheduler(event)

    if input_event.method_name == "tableservice.insert" and input_event.valid_dataset() and input_event.schedule():
        # Create a client.
        client = scheduler.CloudSchedulerClient()

        # Construct the fully qualified location path.
        location_id = os.environ["LOCATION_ID"]
        parent = f"projects/{input_event.gcp_project}/locations/{location_id}"

        # Construct the request body.
        every_x_hour = input_event.schedule()

        MESSAGE_DATA = {"protoPayload": {
            "serviceData": {"jobCompletedEvent": {"job": {"jobConfiguration": {"load": {"destinationTable": {
                "datasetId": input_event.dataset
                , "projectId": input_event.gcp_project
                , "tableId": "events_intraday_%s" % input_event.table_date_shard
            }}}}}}}}

        topic_name = os.environ["TOPIC_NAME"]

        MESSAGE_DATA = json.dumps(MESSAGE_DATA)

        # https://stackoverflow.com/questions/18283725/how-to-create-a-python-dictionary-with-double-quotes-as-default-quote-format
        job = {
            'pubsub_target': {
                'data': str(MESSAGE_DATA).encode(),
                'topic_name': f"projects/{input_event.gcp_project}/topics/{topic_name}"
            }
            ,
            'schedule': f'0 */{every_x_hour} * * *',
            'time_zone': 'America/Los_Angeles'
        }

        # Use the client to send the job creation request.
        response = client.create_job(
            request={
                "parent": parent,
                "job": job
            }
        )

        print('Created job: {}'.format(response.name))
        # [END cloud_scheduler_create_job]
        return response
