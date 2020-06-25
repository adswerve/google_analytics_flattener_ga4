# README #
Google Analytics 360 Flattener.

## Dependencies ##
* Python 3.7 as base interpreter
* Create a virtual environment
* Install packages using cf/requirements.txt
* pip install google-cloud-pubsub==1.6.0 [for
  tools/pubsub_message_publish.py only]

## Directories ##
* cf : pub/sub triggered cloud function that executes a destination
  query to unnest(flatten) the <GA View ID>.ga_sessions_yyyymmdd table
  immediately upon arrival in BigQuery into 5 tables:
  * ga_flat_sessions_yyyymmdd
  * ga_flat_hits_yyyymmdd
  * ga_flat_products_yyyymmdd
  * ga_flat_experiments_yyyymmdd
  * ga_flat_promotions_yyyymmdd
* tests : units test for both cloud functions and deployment manager
  templates
* cfconfigbuilder : http triggered cloud function that finds all
  BigQuery datasets that have a ga_sessions table and adds them to the
  default configuration on Google's Cloud Storage in the following
  location:
  [DEPLOY NAME]-[PROJECT_NAME]-adswerve-ga-flat-config\config_datasets.json

## Files ##
* dm_helper.py: provides consistent names for GCP resources accross
  solution. Configuration and constants also found in the class in this
  file
* dmt-*: any files prefixed with *dmt_* are python based Deployment
  Manager templates
* ga_flattener.yaml: Deployment Manager configuration file. The entire
  solution packages in this file. Used in the deployment manager *create* command
* tools/pubsub_message_publish.py : python based utility to publish a
  message to simulate an event that's being monitored in GCP logging.
  Useful for smoke testing and back-filling data historically.
* LICENSE: BSD 3-Clause open source license

## Prerequisites ##
1. Create Google GCP project or use an existing project that has Google
   Analytics data flowing to it. Referred to as [PROJECT_NAME]
2. Enable the Cloud Build API
3. Enable the Cloud Functions API
4. Add "Logs Configuration Writer", "Cloud Functions Developer" pre
   defined IAM roles to
   [PROJECT_NUMBER]@cloudservices.gserviceaccount.com (built in service
   account) otherwise deployment will fail with permission errors. See
   <https://cloud.google.com/deployment-manager/docs/access-control> for
   detailed explanation.
5. Install gCloud locally or use cloud shell.
6. Clone this github repo
7. Create bucket for staging code during deployment, for example:
   [PROJECT_NAME]-function-code-staging.
8. Edit the ga_flattener.yaml file, specifically the
   properties-->codeLocation value of the function and httpfunction
   resource. Make the value for both to name of the bucket created in #7
   (above step)

## Installation steps ##
1. Execute command: gcloud config set project [PROJECT_NAME]
2. Execute command: gcloud config set account username@domain.com
3. Navigate (locally) to root directory of this repository
4. Execute command: gcloud deployment-manager deployments create
   [Deploy Name] --config ga_flattener.yaml

## Testing / Simulating Event ##
1. After installation, set values in lines 6-11 of
   tools/pubsub_message_publish.py
2. Run tools/pubsub_message_publish.py, which will publish a
   simulated logging event of GA data being ingested into BigQuery

## Un-install steps ##
1. Optional command to remove solution: gcloud deployment-manager
   deployments delete [Deploy Name] -q

