import base64
import json
from google.cloud import storage
from google.cloud import scheduler
from google.api_core.exceptions import GoogleAPICallError
import re
import os
import tempfile
import logging
import googleapiclient.discovery


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


            elif self.method_name == "tableservice.delete":
                bq_destination_table = message_payload['protoPayload']["authorizationInfo"][0]["resource"]
                #  https://www.kite.com/python/answers/how-to-get-the-substring-between-two-markers-in-python

                self.gcp_project = re.search(
                    'projects\/(.*?)\/datasets\/analytics_\d\d\d\d\d\d\d\d\d\/tables\/events_intraday_20\d\d\d\d\d\d$',
                    bq_destination_table).group(1)

                self.dataset = re.search('(analytics_\d\d\d\d\d\d\d\d\d)', bq_destination_table).group(1)
                self.table_date_shard = re.search('_(20\d\d\d\d\d\d)$', bq_destination_table).group(1)
                self.table_name = re.search('(events_intraday)_20\d\d\d\d\d\d$', bq_destination_table).group(1)


        except AttributeError or KeyError as e:
            logging.critical(f'invalid message: {message_payload}')
            logging.critical(e)
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(os.environ["CONFIG_BUCKET_NAME"])
            blob = bucket.blob(os.environ["CONFIG_FILENAME"])
            downloaded_file = os.path.join(tempfile.gettempdir(), "tmp.json")
            blob.download_to_filename(downloaded_file)
            with open(downloaded_file, "r") as config_json:
                self.config = json.load(config_json)
        except Exception as e:
            logging.critical(f'flattener configuration error: {e}')

    def valid_dataset(self):
        valid = self.dataset in self.config.keys()
        return valid

    def schedule(self):
        schedule = self.config[self.dataset]["intraday_schedule"]
        return schedule

    def determine_location_of_app_engine(self, project):
        """
        Purpose: get app engine location
        https://stackoverflow.com/questions/63300303/how-to-get-locationid-in-google-app-engine-not-using-terminal

        We will need to build a Scheduler job.
        To do that, we need to know location of App Engine application, if it exists.

        project: GCP project
        Returns: app engine location

        """
        try:
            # start discovery service and use appengine admin API
            service = googleapiclient.discovery.build('appengine', 'v1', cache_discovery=False)

            # get App engine application details
            req = service.apps().get(appsId=project)

            response = req.execute()

            # this is the application location id
            location_id = (response["locationId"])

            return location_id

        except Exception as e:
            # if App Engine application doesn't exist, then we will just return the default CF location id
            # googleapiclient.errors.HttpError: <HttpError 404 when requesting
            # https://appengine.googleapis.com/v1/apps/{project-id}?alt=json returned "App does not exist.". Details: "App does not exist.">

            return os.environ["LOCATION_ID"]


def manage_intraday_schedule(event, context="context"):
    """Create a job with an App Engine target via the Cloud Scheduler API"""
    # [START cloud_scheduler_create_job]

    input_event = InputValidatorScheduler(event)

    if input_event.valid_dataset():

        # Create a client.
        client = scheduler.CloudSchedulerClient()

        # Construct the fully qualified location path.
        # if there is App Engine alread in the project,
        # location id should match App Engine region
        location_id = input_event.determine_location_of_app_engine(project=input_event.gcp_project)
        parent = f"projects/{input_event.gcp_project}/locations/{location_id}"

        # Construct the fully qualified job path.
        job_id_full_path = f"{parent}/jobs/flattening_msg_{input_event.dataset}_events_intraday_{input_event.table_date_shard}"

        if input_event.method_name == "tableservice.insert" and input_event.schedule():

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
                "name": job_id_full_path,
                'pubsub_target': {
                    'data': MESSAGE_DATA.encode('utf-8'),
                    'topic_name': f"projects/{input_event.gcp_project}/topics/{topic_name}"
                },
                'schedule': f'0 */{every_x_hour} * * *',
                'time_zone': 'America/Los_Angeles'
            }

            try:
                # Use the client to send the job creation request.
                response = client.create_job(
                    request={
                        "parent": parent,
                        "job": job
                    }
                )

                logging.info('Created Scheduler job: {}'.format(response.name))
                # [END cloud_scheduler_create_job]

                # verify that we have created a job
                response_get_job = client.get_job(
                    request={
                        "name": job_id_full_path
                    }
                )

                return job_id_full_path, response

            # if it already exists
            # 409 already exists
            except GoogleAPICallError as e:
                logging.critical("Error: %s" % e)



        elif input_event.method_name == "tableservice.delete":

            # Use the client to send the job deletion request.
            try:
                client.delete_job(name=job_id_full_path)
                logging.info(f"Deleted Scheduler job: {job_id_full_path}")

            # if it doesn't exist
            # 404 job not found
            except GoogleAPICallError as e:
                logging.critical("Error: %s" % e)


    else:
        logging.warning(f'Dataset {input_event.dataset} is not configured for intraday flattening')

# TODO: error handling while creating and deleting the schedule. use a warning and provide more info

# TODO: link the Cloud Function directly to logs

# TODO: nice-to-have: refactor f strings, so we use the same format (?)

# TODO: Install into a diff project and test with a live daily table

# TODO: warning
# """
# cfintraday\main.py:36
#   C:\Users\Ruslan Bergenov\dev\flattener\GA4F_development_ga4\cfintraday\main.py:36: DeprecationWarning: invalid escape sequence \d
#     self.table_date_shard = re.search('_(20\d\d\d\d\d\d)$', bq_destination_table['tableId']).group(1)
#
# cfintraday\main.py:37
#   C:\Users\Ruslan Bergenov\dev\flattener\GA4F_development_ga4\cfintraday\main.py:37: DeprecationWarning: invalid escape sequence \d
#     self.table_name = re.search('(events_intraday)_20\d\d\d\d\d\d$', bq_destination_table['tableId']).group(
#
# cfintraday\main.py:46
#   C:\Users\Ruslan Bergenov\dev\flattener\GA4F_development_ga4\cfintraday\main.py:46: DeprecationWarning: invalid escape sequence \/
#     'projects\/(.*?)\/datasets\/analytics_\d\d\d\d\d\d\d\d\d\/tables\/events_intraday_20\d\d\d\d\d\d$',
#
# cfintraday\main.py:49
#   C:\Users\Ruslan Bergenov\dev\flattener\GA4F_development_ga4\cfintraday\main.py:49: DeprecationWarning: invalid escape sequence \d
#     self.dataset = re.search('(analytics_\d\d\d\d\d\d\d\d\d)', bq_destination_table).group(1)
#
# cfintraday\main.py:50
#   C:\Users\Ruslan Bergenov\dev\flattener\GA4F_development_ga4\cfintraday\main.py:50: DeprecationWarning: invalid escape sequence \d
#     self.table_date_shard = re.search('_(20\d\d\d\d\d\d)$', bq_destination_table).group(1)
#
# cfintraday\main.py:51
#   C:\Users\Ruslan Bergenov\dev\flattener\GA4F_development_ga4\cfintraday\main.py:51: DeprecationWarning: invalid escape sequence \d
#     self.table_name = re.search('(events_intraday)_20\d\d\d\d\d\d$', bq_destination_table).group(1)
#
# cf\main.py:19
#   C:\Users\Ruslan Bergenov\dev\flattener\GA4F_development_ga4\cf\main.py:19: DeprecationWarning: invalid escape sequence \d
#     self.table_date_shard = re.search('_(20\d\d\d\d\d\d)$', bq_destination_table['tableId']).group(1)
#
# cf\main.py:20
#   C:\Users\Ruslan Bergenov\dev\flattener\GA4F_development_ga4\cf\main.py:20: DeprecationWarning: invalid escape sequence \d
#     self.table_name = re.search('(events.*)_20\d\d\d\d\d\d$', bq_destination_table['tableId']).group(1)
#
# -- Docs: https://docs.pytest.org/en/stable/warnings.html
# """
