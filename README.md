# README #
Google Analytics 360 Flattener.  A Google Cloud Platform (GCP) solution that unnests (flattens) Google Analytics Data stored in Bigquery.  
The GCP resources for the solutions are installed via Deployment Manager.

## Local dependencies ##
* Google Cloud Platform SDK.  Download and install from these instructions: https://cloud.google.com/sdk/docs/install
* Python >= 3.7.  Download and install from https://python.org.
* Web browser
* git (**optional** for cloning this code repository)

## Prerequisites ##
1. Browse to https://cloud.console.google.com to create Google GCP project or use 
   an existing project that has Google Analytics data flowing to it. 
   Referred to as **[PROJECT_ID]**.
2. Grant the installing user (you most likely) the pre-defined IAM role of "Owner".
3. As the installing user for **[PROJECT_ID]**, enable the following APIs
     * Cloud Build API
     * Cloud Functions API.
     * Identity and Access Management (IAM) API
4. As the installing user for **[PROJECT_ID]**, grant the following pre-defined IAM roles to 
   **[PROJECT_NUMBER]**@cloudservices.gserviceaccount.com (built in service
   account) otherwise deployment will fail with permission errors. See
   <https://cloud.google.com/deployment-manager/docs/access-control> for
   detailed explanation.
     * Logs Configuration Writer
     * Cloud Functions Developer
     * pub/sub Admin
5. As the installing user for **[PROJECT_ID]**, create bucket for staging code during deployment, for example:
   **[PROJECT_NUMBER]**-function-code-staging.  Referred to as **[BUCKET_NAME]**.
6. Clone this github repo or download the source code from the releases section to your local machine or 
   cloud shell.   
7. Edit the _ga_flattener.yaml_ and _ga_flattener_colon.yaml_ files, specifically all occurrences of _properties-->codeBucket_ value . Set the value to **[BUCKET_NAME]** (see step above)

_**The following steps are only required if you plan to backfill historical tables._**   
8. Install python 3.7 or higher
9. From a command prompt, upgrade pip (Command:  py -m pip install --upgrade pip)
10. Navigate to the root directory of the source code that was downloaded or cloned in step 6 above.   
10. From a command prompt, install python virtual environments (Command: py -m pip install --user virtualenv)
11. Create a virtual environment for the source code in step 6  (Command: py -m venv venv)
12. Active the virtual environment in the step above.
13. Install the python dependent packages into the virtual environment.  (Command: pip install -r cf\requirements.txt)

## Installation steps ##
1. Execute command in Google Cloud SDK Shell: gcloud config set project **[PROJECT_ID]**
2. Execute command: gcloud config set account <username@domain.com>. **Note** - This must be the installing user from above prerequisites.
3. Navigate (locally) to root directory of this repository
4. If **[PROJECT_ID]** does **NOT** contain a colon (:) execute command: 
    * gcloud deployment-manager deployments create **[Deployment Name]** --config ga_flattener.yaml
  
   otherwise follow these steps:
     1. execute command: 
      * gcloud deployment-manager deployments create **[Deployment Name]** --config ga_flattener_colon.yaml
     2. Trigger function (with a blank message) named **[Deployment Name]**-cfconfigbuilderps.  It will create the necessary configuration file in the applications Google Coud Storage bucket.  An easy method to do this is to browse to https://console.cloud.google.com/functions and click the cloud function named **[Deployment Name]**-cfconfigbuilderps and go to the testing section and click "TEST THIS FUNCTION".
     
    ### **[Deployment Name]** naming convention 
    * Note that **[Deployment Name]** cannot have underscores in its name, but can have hyphens. 
    * Example of a valid name: gcloud deployment-manager deployments create ga-flattener-deployment --config ga_flattener.yaml
    * Please refer to the [documentation](https://cloud.google.com/deployment-manager/docs/deployments) for more examples of valid values of **[Deployment Name]** 

## Verification steps ##
1. After installation, a configuration file named config_datasets.json exists in **gs://[Deployment Name]-[PROJECT_NUMBER]-adswerve-ga-flat-config/** (Cloud Storage Bucket within **[PROJECT_ID]**).  This file contains all the datasets that have "ga_sessions_yyyymmdd" tables and which tables to unnest.  This configuration is required for this GA flattener solution to run daily or to backfill historical data.  Edit this file accordingly to include or exclude certain datasets or tables to unnest.  For example:
 * { "123456789": ["sessions","hits","products"] }   will only flatten those 3 nested tables for GA view 123456789
 * { "123456789": ["sessions","hits","products", "promotions", "experiments"], "987654321": ["sessions","hits"] } will flatten all possible nested tables for GA view 123456789 but only _sessions_ and _hits_ for GA View 987654321.

_**The following steps are only required if you plan to backfill historical tables._**   
2. Modify values in the configuration section of tools/pubsub_message_publish.py accordingly.  **Suggestion:** Use a small date range to start, like yesterday only.
3. From a gcloud command prompt, authenticate the installing user using command:
   _gcloud auth application-default login_
4. Run tools/pubsub_message_publish.py locally, which will publish a
   simulated logging event of GA data being ingested into BigQuery.  Check dataset(s) that are configured for new date sharded tables such as (depending on what is configured):
    * ga_flat_experiments_(x)
    * ga_flat_hits_(x)
    * ga_flat_products_(x)
    * ga_flat_promotions_(x)
    * ga_flat_sessions_(x)
   
## Un-install steps ##
1. Delete the config_datasets.json file from gs://[Deployment Name]-[PROJECT_NUMBER]-adswerve-ga-flat-config/ (Cloud Storage Bucket within [PROJECT_ID])
2. Optional command to remove solution: 
   * gcloud deployment-manager deployments delete **[Deployment Name]** -q

## Common errors ##
### Install ###
* * **Message:** AccessDeniedException: 403 **[PROJECT_NUMBER]**@cloudbuild.gserviceaccount.com does not have storage.objects.list access to the Google Cloud Storage bucket.
  * **Resolution:** Ensure the value (Cloud Storage bucket name) configured in "codeBucket" setting of ga_flattener*.yaml is correct. **[PROJECT_NUMBER]**@cloudbuild.gserviceaccount.com only requires GCP predefined role of _Cloud Build Service Account_
### Verification ###
* * **Message:**   google.auth.exceptions.DefaultCredentialsError: Could not automatically determine credentials. Please set GOOGLE_APPLICATION_CREDENTIALS or explicitly create credentials and re-run the application. For more information, please see https://cloud.google.com/docs/authentication/getting-started
   * **Resolution:** Ensure you run the gcloud command _gcloud auth application-default login_ as this sets up the required authentication and it'll just work.   
    
## Repository directories ##
* cf : pub/sub triggered cloud function that executes a destination
  query to unnest(flatten) the <GA View ID>.ga_sessions_yyyymmdd table
  immediately upon arrival in BigQuery into these tables, depending on the configuration:
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

## Repository files ##
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