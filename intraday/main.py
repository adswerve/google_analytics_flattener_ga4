# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

# class IntradayFlattenerDatasetConfigStorage(object):
#     def __init__(self):
#         self.bucket_name = os.environ["config_bucket_name"]
#         self.config_filename = os.environ["config_filename"]
#         # TODO: how do I get project id, if i need it?
#         self.project = GaFlattenerDeploymentConfiguration().get_project(DeploymentConfiguration)
#         self.project = GaFlattenerDeploymentConfiguration().get_project(DeploymentConfiguration)
#         # print("test")

# # TODO(developer): Uncomment and set the following variables
project_id = "as-dev-ga4-flattener-320623"
location_id = "us-central1"
service_id = 'my-service'

def create_intraday_schedule(project_id=project_id, location_id=location_id, service_id=service_id):
    """Create a job with an App Engine target via the Cloud Scheduler API"""
    # [START cloud_scheduler_create_job]
    from google.cloud import scheduler

    # Create a client.
    client = scheduler.CloudSchedulerClient()

    # Construct the fully qualified location path.
    parent = f"projects/{project_id}/locations/{location_id}"

    # Construct the request body.
    job = {
        'app_engine_http_target': {
            'app_engine_routing': {
                'service': service_id
            },
            'relative_uri': '/log_payload',
            'http_method': 1,
            'body': 'Hello World'.encode()
        },
        'schedule': '* 1 * * *',
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


# def delete_scheduler_job(project_id, location_id, job_id):
#     """Delete a job via the Cloud Scheduler API"""
#     # [START cloud_scheduler_delete_job]
#     from google.cloud import scheduler
#     from google.api_core.exceptions import GoogleAPICallError
#
#     # Create a client.
#     client = scheduler.CloudSchedulerClient()
#
#     # TODO(developer): Uncomment and set the following variables
#     # project_id = 'PROJECT_ID'
#     # location_id = 'LOCATION_ID'
#     # job_id = 'JOB_ID'
#
#     # Construct the fully qualified job path.
#     job = f"projects/{project_id}/locations/{location_id}/jobs/{job_id}"
#
#     # Use the client to send the job deletion request.
#     try:
#         client.delete_job(name=job)
#         print("Job deleted.")
#     except GoogleAPICallError as e:
#         print("Error: %s" % e)
#     # [END cloud_scheduler_delete_job]

#
# from google.cloud import storage, scheduler
#
#
#
# # #TODO: how do I install scheduler api? having issues with google-cloud-scheduler==2.3.2, scheduler_v1 is not recognized
# import subprocess
# import logging
# import json

# import tempfile
#
# logger = logging.getLogger(__name__)
#

#
# def create_intraday_schedule(event, context):
#     """Background Cloud Function to be triggered by Cloud Storage.
#        This generic function logs relevant data when a file is changed.
#
#     Args:
#         event (dict):  The dictionary with data specific to this type of event.
#                        The `data` field contains a description of the event in
#                        the Cloud Storage `object` format described here:
#                        https://cloud.google.com/storage/docs/json_api/v1/objects#resource
#         context (google.cloud.functions.Context): Metadata of triggering event.
#     Returns:
#         None; the output is written to Stackdriver Logging
#     """
#
#     # print('Bucket: {}'.format(event['bucket']))
#     # print('File: {}'.format(event['name']))
#     #
#     # print('event: {}'.format(event))
#     # print("event:")
#     # print(event)
#     #
#     # print("context:")
#     # print('context: {}'.format(context))
#     # print(context)
#     # project_id = event['resource']['labels']['project_id'] # failing, as i can only parse payload, not log metadata
#     # print('project_id: {}'.format(project_id)
#
#     storage_client = storage.Client()
#
#     if event['name'] == IntradayFlattenerDatasetConfigStorage().config_filename:
#
#         bucket = storage_client.bucket(IntradayFlattenerDatasetConfigStorage().bucket_name)
#         blob = bucket.blob(IntradayFlattenerDatasetConfigStorage().config_filename)
#
#         downloaded_file = os.path.join(tempfile.gettempdir(), "tmp.json")
#         blob.download_to_filename(downloaded_file)
#         with open(downloaded_file, "r") as config_json_intraday:
#             config_intraday = json.load(config_json_intraday)
#
#         # print('config_intraday: {}'.format(str(config_intraday)))
#
#         if "intraday" in config_intraday.keys():
#
#             intraday_flatten_schedules = config_intraday["intraday"] # GA properties
#
#             for dataset_intraday_flattening_config in intraday_flatten_schedules:
#                 for dataset, schedule in dataset_intraday_flattening_config.items():
#
#                     if schedule is not None:
#                         pass
#         # https://cloud.google.com/blog/products/application-development/how-to-schedule-a-recurring-python-script-on-gcp
#         # https://stackoverflow.com/questions/57527375/python-shell-commands-not-executed-from-google-cloud-functions
#         # https://docs.python.org/3/library/subprocess.html
#
#         # data = subprocess.run(
#         #     'gcloud scheduler jobs create pubsub daily_job --schedule "0 */12 * * *" --topic my-pubsub-topic --message-body "This is a job that I run twice per day!"',
#         #     shell=True, capture_output=True, check=True)
#         #
#         # data = subprocess.run(
#         #     'gcloud scheduler jobs describe daily_job',
#         #     shell=True, capture_output=True, check=True)
#         #
#         # print(data)
#
#         print("finished running")
#             #
#             # for dataset in datasets_to_flatten_intraday_data:
#             #     if config_intraday:
#             #         pass
#
#             # https://stackoverflow.com/questions/60681672/how-to-create-a-job-with-google-cloud-scheduler-python-api
#             # https://github.com/googleapis/python-scheduler/blob/master/samples/snippets/create_job.py
#
#
#
#
#


#TODO: how do I create a cron job?

#TODO: how do I delete a cron job?

# TODO: how do I update a cron job?

# TODO: this function works locally but doesn't run on GCP as a a Cloud Function, fails with error code 127 "command not found"
# How do I solve this?