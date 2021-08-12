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

# # TODO(developer): Uncomment and set the following variables
project_id = "as-dev-ga4-flattener-320623"
location_id = "us-central1"
service_id = 'my-service'

#TODO: how do I create a cron job?

#TODO: how do I delete a cron job?

# TODO: how do I update a cron job?

# TODO: this function works locally but doesn't run on GCP as a a Cloud Function, fails with permissions error

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
        'pubsub_target': {
            'data': 'Hello World'.encode(),
            'topic_name': "projects/as-dev-ga4-flattener-320623/topics/ga-flattener-deployment-topic"
            }
        ,
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
