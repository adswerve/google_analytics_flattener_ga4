# README #
Google Analytics 360 Flattener.  A Google Cloud Platform (GCP) solution that unnests (flattens) Google Analytics Data stored in Bigquery.  The GCP resources for the solutions are installed via Deployment Manager.

## Dependencies ##
* Python 3.7 or higher as base interpreter
* Create a virtual environment
* Install packages using cf/requirements.txt

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
* cfconfigbuilder(ps) : cloud function that finds all
  BigQuery datasets that have a ga_sessions table and adds them to the
  default configuration on Google's Cloud Storage in the following
  location:
  [DEPLOYMENT NAME]-[PROJECT_NUMBER]-adswerve-ga-flat-config\config_datasets.json

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
   Analytics data flowing to it. Referred to as [PROJECT_ID]
2. Enable the Cloud Build API
3. Enable the Cloud Functions API
4. Enable the Identity and Access Management (IAM) API
5. Add "Logs Configuration Writer", "Cloud Functions Developer", "pub/sub Admin" pre
   defined IAM roles to
   [PROJECT_NUMBER]@cloudservices.gserviceaccount.com (built in service
   account) otherwise deployment will fail with permission errors. See
   <https://cloud.google.com/deployment-manager/docs/access-control> for
   detailed explanation.
6. Install gCloud locally or use cloud shell.
7. Clone this github repo
8. Create bucket for staging code during deployment, for example:
   [PROJECT_NUMBER]-function-code-staging.  Referred to as [BUCKET_NAME].
9. Edit the ga_flattener.yaml file, specifically the
   _properties-->codeBucket_ value of the function and httpfunction
   resources. Set the value for both to [BUCKET_NAME] (see previous step)

## Installation steps ##
1. Execute command: gcloud config set project [PROJECT_ID]
2. Execute command: gcloud config set account <username@domain.com>
3. Navigate (locally) to root directory of this repository
4. If [PROJECT_ID] does **NOT** contain a colon (:) execute command: 
   * gcloud deployment-manager deployments create [Deployment Name] --config ga_flattener.yaml
   otherwise follow these steps:
     1. execute command: 
      * gcloud deployment-manager deployments create [Deployment Name] --config ga_flattener_colon.yaml
     2. Trigger function (with a blank message) named [Deployment Name]-cfconfigbuilderps.  It will create the necessary configuration file in the applications Google Coud Storage bucket.

## Testing / Simulating Event ##
1. After installation, modify values in lines 7-17 of
   tools/pubsub_message_publish.py accordingly.
2. Run tools/pubsub_message_publish.py, which will publish a
   simulated logging event of GA data being ingested into BigQuery.  Check dataset for date sharded tables named:
    * ga_flat_experiments_(x)
    * ga_flat_hits_(x)
    * ga_flat_products_(x)
    * ga_flat_promotions_(x)
    * ga_flat_sessions_(x)
   
## Un-install steps ##
1. Optional command to remove solution: 
   * gcloud deployment-manager deployments delete [Deployment Name] -q

## Common install errors ##
1. * **Message:** Step #2: AccessDeniedException: 403 [PROJECT_NUMBER]@cloudbuild.gserviceaccount.com does not have storage.objects.list access to the Google Cloud Storage bucket.
   * **Resolution:** Ensure the bucket configured in "codeBucket" of ga_flattener*.yaml is correct. [PROJECT_NUMBER]@cloudbuild.gserviceaccount.com only required GCP predfined role of _Cloud Build Service Account_