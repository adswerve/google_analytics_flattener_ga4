from google.cloud import storage#, scheduler_v1
# #TODO: how do I install scheduler api? having issues with google-cloud-scheduler==2.3.2, scheduler_v1 is not recognized
import subprocess
import logging
import json
import os
import tempfile

logger = logging.getLogger(__name__)

class IntradayFlattenerDatasetConfigStorage(object):
    def __init__(self):
        self.bucket_name = os.environ["config_bucket_name"]
        self.config_filename = os.environ["config_filename"]
        # TODO: how do I get project id, if i need it?
        # self.project = GaFlattenerDeploymentConfiguration().get_project(DeploymentConfiguration)
        # self.project = GaFlattenerDeploymentConfiguration().get_project(DeploymentConfiguration)
        # print("test")

def create_intraday_schedule(event, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed.

    Args:
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    # print('Bucket: {}'.format(event['bucket']))
    # print('File: {}'.format(event['name']))
    #
    # print('event: {}'.format(event))
    # print("event:")
    # print(event)
    #
    # print("context:")
    # print('context: {}'.format(context))
    # print(context)
    # project_id = event['resource']['labels']['project_id'] # failing, as i can only parse payload, not log metadata
    # print('project_id: {}'.format(project_id)

    storage_client = storage.Client()

    if event['name'] == IntradayFlattenerDatasetConfigStorage().config_filename:

        bucket = storage_client.bucket(IntradayFlattenerDatasetConfigStorage().bucket_name)
        blob = bucket.blob(IntradayFlattenerDatasetConfigStorage().config_filename)

        downloaded_file = os.path.join(tempfile.gettempdir(), "tmp.json")
        blob.download_to_filename(downloaded_file)
        with open(downloaded_file, "r") as config_json_intraday:
            config_intraday = json.load(config_json_intraday)

        # print('config_intraday: {}'.format(str(config_intraday)))

        if "intraday" in config_intraday.keys():

            intraday_flatten_schedules = config_intraday["intraday"] # GA properties

            for dataset_intraday_flattening_config in intraday_flatten_schedules:
                for dataset, schedule in dataset_intraday_flattening_config.items():

                    if schedule is not None:
                        pass
        # https://cloud.google.com/blog/products/application-development/how-to-schedule-a-recurring-python-script-on-gcp
        # https://stackoverflow.com/questions/57527375/python-shell-commands-not-executed-from-google-cloud-functions
        # https://docs.python.org/3/library/subprocess.html

        data = subprocess.run(
            'gcloud scheduler jobs create pubsub daily_job --schedule "0 */12 * * *" --topic my-pubsub-topic --message-body "This is a job that I run twice per day!"',
            shell=True, capture_output=True, check=True)

        data = subprocess.run(
            'gcloud scheduler jobs describe daily_job',
            shell=True, capture_output=True, check=True)

        print(data)

        print("finished running")
            #
            # for dataset in datasets_to_flatten_intraday_data:
            #     if config_intraday:
            #         pass

            # https://stackoverflow.com/questions/60681672/how-to-create-a-job-with-google-cloud-scheduler-python-api

                    # project_id = XXXX
                    # client = scheduler_v1.CloudSchedulerClient.from_service_account_json(
                    #     r"./xxxx.json")
                    #
                    # parent= client.location_path(project_id,'us-central1')
                    #
                    # job={"name":"projects/your-project/locations/app-engine-location/jobs/traing_for_model",
                    #      "description":"this is for testing training model",
                    #      "http_target": {"uri":"https://us-central1-gerald-automl-test.cloudfunctions.net/automl-trainmodel-1-test-for-cron-job"},
                    #      "schedule":"0 10 * * *",
                    #      "time_zone":"Australia/Perth",
                    #      }
                    #
                    # training_job= client.create_job(parent,job)





#TODO: how do I create a cron job?

#TODO: how do I delete a cron job?

# TODO: how do I update a cron job?

# TODO: this function works locally but doesn't run on GCP as a a Cloud Function, fails with error code 127 "command not found"
# How do I solve this?