import base64
import json
from google.cloud import storage
from google.cloud import scheduler
import re
import os
import tempfile
import logging
import googleapiclient.discovery
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

class InputValidatorIntraday(object):
    def __init__(self, event):
        try:
            message_payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
            self.method_name = message_payload['protoPayload']["methodName"]

            if self.method_name == "tableservice.insert":
                bq_destination_table = \
                    message_payload['protoPayload']['serviceData']["tableInsertResponse"]["resource"]["tableName"]
                self.gcp_project = bq_destination_table['projectId']
                self.dataset = bq_destination_table['datasetId']
                self.table_date_shard = re.search(r'_(20\d\d\d\d\d\d)$', bq_destination_table['tableId']).group(1)
                self.table_name = re.search(r'(events_intraday)_20\d\d\d\d\d\d$',
                                            bq_destination_table['tableId']).group(
                    1)

            elif self.method_name == "tableservice.delete":
                # we assume that there's only 1 element in the array. It is true, because for each deleted intraday table there will be a separate log
                # nevertheless, let's check the assumption on an off-chance that there are many elements in the array
                assert len(message_payload['protoPayload'][
                               "authorizationInfo"]) == 1, f"more than 1 element in the log array message_payload['protoPayload']['authorizationInfo']: {message_payload['protoPayload']['authorizationInfo']}"
                bq_destination_table = message_payload['protoPayload']["authorizationInfo"][0]["resource"]
                #  https://www.kite.com/python/answers/how-to-get-the-substring-between-two-markers-in-python

                self.gcp_project = re.search(
                    r'^projects\/(.*?)\/datasets\/analytics_\d\d\d\d\d\d\d\d\d\/tables\/events_intraday_20\d\d\d\d\d\d$',
                    bq_destination_table).group(1)

                self.dataset = re.search(r'(analytics_\d\d\d\d\d\d\d\d\d)', bq_destination_table).group(1)
                self.table_date_shard = re.search(r'_(20\d\d\d\d\d\d)$', bq_destination_table).group(1)
                self.table_name = re.search(r'(events_intraday)_20\d\d\d\d\d\d$', bq_destination_table).group(1)

        except AttributeError or KeyError as e:
            logging.critical(f"invalid message: {message_payload}")
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
            logging.critical(f"flattener configuration error: {e}")

    def valid_dataset(self):
        """is the BQ dataset (representing GA4 property) configured for flattening?"""
        valid = self.dataset in self.config.keys()
        return valid

    def intraday_schedule(self):
        """is the BQ dataset (representing GA4 property) configured for intraday flattening?
        If yes, how often do we want to update the flattened intraday tables?
        Every X hours OR every Y minutes
        """

        config_intraday_flattening = self.config[self.dataset].get("intraday_flattening", {
                                                                                "intraday_flat_tables_schedule": None,
                                                                                "intraday_flat_views": True
                                                                              })

        config_intraday_schedule = config_intraday_flattening['intraday_flat_tables_schedule']

        if config_intraday_flattening['intraday_flat_tables_schedule']:

            config_intraday_schedule_frequency = config_intraday_schedule.get("frequency", None)
            config_intraday_schedule_units = config_intraday_schedule.get("units", None)

            # TODO: include/handle a situation when it's "hour" or "minute"
            assert config_intraday_schedule_units in ["hours",
                                                      "minutes"], "Config file error: intraday schedule units should be minutes or hours"


            if config_intraday_schedule_units == "minutes":
                assert config_intraday_schedule_frequency >= 1 and config_intraday_schedule_frequency <= 59, "Config file error: if intraday schedule units are minutes, then the frequency should be between 1 and 59"
                cron_schedule = f"*/{config_intraday_schedule_frequency} * * * *"

            elif config_intraday_schedule_units == "hours":
                cron_schedule = f"0 */{config_intraday_schedule_frequency} * * *"

            return cron_schedule

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

    def contruct_scheduler_job_id_full_path(self):
        """
        # Construct the fully qualified location path for the Scheduler Job.
        # if there is App Engine already in the project, location id should match App Engine region
        """
        location_id = self.determine_location_of_app_engine(project=self.gcp_project)
        parent = f"projects/{self.gcp_project}/locations/{location_id}"

        # Construct the fully qualified job path.
        job_id_full_path = f"{parent}/jobs/flattening_msg_{self.dataset}_events_intraday_{self.table_date_shard}"

        return job_id_full_path, parent


def manage_intraday_schedule(event, context="context"):
    """
    creates or deletes a Scheduler Job. This scheduler job is used to flatten intraday table at a desired frequency
    if dataset is configured for flattening:
        if a new intraday tbl got created:
            if dataset is configured for intraday flattening:
                try:
                    create a schedule
                    # Created Scheduler job: projects/adswerve-mobile-qa/locations/us-west2/jobs/flattening_msg_analytics_222460912_events_intraday_20211021
                except:
                    critical: already exists
                    # Error creating a scheduler job projects/adswerve-mobile-qa/locations/us-west2/jobs/flattening_msg_analytics_222460912_events_intraday_20211021: 409 Job projects/adswerve-mobile-qa/locations/us-west2/jobs/flattening_msg_analytics_222460912_events_intraday_20211021 already exists.
            else:
                warning: not configured for intraday flattening

        elif if an intraday table got deleted:
            try:
                delete the schedule
            except:
                warning: doesn't exist
                # Error deleting a scheduler job projects/adswerve-mobile-qa/locations/us-west2/jobs/flattening_msg_analytics_222460912_events_intraday_20211021: 404 Job not found.

    else:
        warning: not configured for any flattening at all
    """

    # [START cloud_scheduler_create_job]

    input_event = InputValidatorIntraday(event)

    if input_event.valid_dataset():

        # Create a client.
        client = scheduler.CloudSchedulerClient()

        job_id_full_path, parent = input_event.contruct_scheduler_job_id_full_path()

        # did a new intraday table get created?
        if input_event.method_name == "tableservice.insert":

            # do we need to do intraday flattening?
            if input_event.intraday_schedule():

                # Construct the request body.
                cron_schedule = input_event.intraday_schedule()

                MESSAGE_DATA = {
                    "protoPayload": {
                        "resourceName": f"projects/{input_event.gcp_project}/datasets/{input_event.dataset}/tables/events_intraday_{input_event.table_date_shard}"
                    }
                }

                topic_name = os.environ["TOPIC_NAME"]

                MESSAGE_DATA = json.dumps(MESSAGE_DATA)

                # https://stackoverflow.com/questions/18283725/how-to-create-a-python-dictionary-with-double-quotes-as-default-quote-format
                job = {
                    "name": job_id_full_path,
                    'pubsub_target': {
                        'data': MESSAGE_DATA.encode('utf-8'),
                        'topic_name': f"projects/{input_event.gcp_project}/topics/{topic_name}"
                    },
                    'schedule': cron_schedule,
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

                    # [END cloud_scheduler_create_job]

                    # verify that we have created a job
                    response_get_job = client.get_job(
                        request={
                            "name": job_id_full_path
                        })

                    logging.info(f"Created Scheduler job: {response.name}")
                    return response

                # if it already exists, it doesn't need to be created
                # 409 already exists
                except Exception as e:
                    logging.error(
                        f"Error creating a Scheduler job {job_id_full_path} (the job probably already exists): {e}")

            else:
                logging.warning(f"Dataset {input_event.dataset} is not configured for intraday table flattening")

        # did an intraday table get deleted?
        elif input_event.method_name == "tableservice.delete":

            # Use the client to send the job deletion request.
            try:
                # TODO: design flaw(??): it'll keep running daily
                # if you have an intraday table, even if you don't have intraday flattening configured
                # however, it does need to listen to intraday table creations
                client.delete_job(name=job_id_full_path)
                logging.info(f"Deleted Scheduler job: {job_id_full_path}")

            # if it doesn't exist, so it doesn't need to be deleted
            # 404 job not found
            except Exception as e:
                logging.warning(
                    f"Error deleting a Scheduler job {job_id_full_path} (the job probably doesn't exist): {e}")

    else:
        logging.warning(f"Dataset {input_event.dataset} is not configured for flattening")

#TODO: when running on GCP, this error means that job already exists (can't create it) or doesn’t exist yet (can't delete it)
# "NoneType" object has no attribute "Call"
# this is a strange error
# when unit testing locally, I get good errors : 404 job not found (trying to delete) or 409 already exists(trying to create)


# TODO: nice-to-have: link the Cloud Function directly to logs

#TODO: Risk: there are two nested tables with yesterday's date shard: events_ and events_intraday_.
# We need to ensure that the flattened tbls are based on daily tbls, not intraday.
# Yesterday's intraday table flattening process might overwrite yesterday's daily flat table. How do we solve this?
# The main flattening CF: if it gets a message: flatten intraday table  with data shard 20211023.
# Make an API call to BQ. Does a DAILY nested table exist with this date shard?
# If yes, log a warning and exit the function. We shouldn't flatten the intraday table in this case.
# this it not likely: Daily table got created AFTER intraday table deleted, ~3 seconds later
# This is good, daily table has the last say and will determine the histrorical flat tables

#TODO: risk: we might have many intraday tables and therefore, many Cloud Scheduler jobs, which will waste resources
# we decided to leave it for now and if we observe situations like this, we'll fix the intraday flattener