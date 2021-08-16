# README #
Google Analytics 4 Flattener. A Google Cloud Platform (GCP) solution that unnests (flattens) [Google Analytics 4 (GA4) Data stored in Bigquery](https://support.google.com/analytics/answer/7029846?hl=en).  
The GCP resources for the solutions are installed via Deployment Manager.

[![Python package](https://github.com/adswerve/google_analytics_flattener_ga4/actions/workflows/python-package.yml/badge.svg)](https://github.com/adswerve/google_analytics_flattener_ga4/actions/workflows/python-package.yml)

## Contents
  * [Background and problem](#background-and-problem)
  * [Purpose](#purpose)
  * [Local dependencies](#local-dependencies)
  * [Prerequisites](#prerequisites)
  * [Installation steps](#installation-steps)
  * [Verification steps](#verification-steps)
  * [Un-install steps](#un-install-steps)
  * [Common errors](#common-errors)
    + [Install](#install)
    + [Verification](#verification)
  * [Repository directories](#repository-directories)
  * [Repository files](#repository-files)
  * [Running unit tests](#running-unit-tests)

## Background and problem
  * [Google Analytics 4 (GA4)](https://support.google.com/analytics/answer/10089681?hl=en) is the newest generation of Google Analytics.
  * When you export data from GA4 into BigQuery, the schema will have [nested and repeated fields](https://cloud.google.com/bigquery/docs/nested-repeated). 
    * Nested data represents a struct. In BigQuery, it can be described as a column inside of a column.
    * Repeated data represents an array. In BigQuery, it can be described as a row inside of a row.
  * Schema with nested and repeated data:
    * Can make it harder to query the data (it takes a learning curve to get used to).
    * May be incompatible with other data systems. It maybe be impossible to import data into another system, such as a spreadsheet software or database system. 


## Purpose
  * The purpose of GA4 Flattener is to solve the above problems by flattening/unnesting GA4 data. 
  * Flattening/unnesting means converting all nested and repeated fields to regular fields, so our data looks like a regular table, compatible with other systems (imagine a Google Spreadsheet / Microsoft Excel table).
  * When GA4 Flattener runs, it keeps your original GA4 data with nested and repeated data, but it also writes 4 flat tables into the same BigQuery dataset. These 4 flat tables can be queried and imported into other systems more easily.

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
    
If your GCP project is brand new, you might not have **[PROJECT_NUMBER]**@cloudservices.gserviceaccount.com yet. To fix this, enable Compute Engine API and then disable it. The service account **[PROJECT_NUMBER]**@cloudservices.gserviceaccount.com will appear in your GCP project under IAM.

5. As the installing user for **[PROJECT_ID]**, create bucket for staging code during deployment, for example:
   **[PROJECT_NUMBER]**-function-code-staging.  Referred to as **[BUCKET_NAME]**.
   
6. Clone this github repo or download the source code from the releases section to your local machine or 
   cloud shell.   
   
7. Edit the _ga_flattener.yaml_ and _ga_flattener_colon.yaml_ files, specifically all occurrences of _properties-->codeBucket_ value . Set the value to **[BUCKET_NAME]** (see step above)

_**The following steps are only required if you plan to backfill historical tables._**   

8. Install python 3.7 or higher

9. From a command prompt, upgrade pip (Command:  ```py -m pip install --upgrade pip```)

10. Navigate to the root directory of the source code that was downloaded or cloned in step 6 above.   

11. From a command prompt, install python virtual environments (Command: ```py -m pip install --user virtualenv```)
    
12. Create a virtual environment for the source code in step 6  (Command: ```py -m venv venv```)
    
12. Activate the virtual environment in the step above.
    
13. Install the python dependent packages into the virtual environment.  (Command: ```pip install -r cf\requirements.txt```)

## Installation steps ##
1. Execute command in Google Cloud SDK Shell: gcloud config set project **[PROJECT_ID]**
2. Execute command: gcloud config set account <username@domain.com>. **Note** - This must be the installing user from above prerequisites.
3. Navigate (locally) to root directory of this repository
4. If **[PROJECT_ID]** does **NOT** contain a colon (:) execute command: 
    * gcloud deployment-manager deployments create **[Deployment Name]** --config ga_flattener.yaml
  
   otherwise follow these steps:
     1. execute command: 
      * ```gcloud deployment-manager deployments create **[Deployment Name]** --config ga_flattener_colon.yaml```
     2. Trigger function (with a blank message) named **[Deployment Name]**-cfconfigbuilderps.  It will create the necessary configuration file in the applications Google Coud Storage bucket.  An easy method to do this is to browse to https://console.cloud.google.com/functions and click the cloud function named **[Deployment Name]**-cfconfigbuilderps and go to the testing section and click "TEST THIS FUNCTION".
     
    ### **[Deployment Name]** naming convention 
    * Note that **[Deployment Name]** cannot have underscores in its name, but can have hyphens. 
    * Example of a valid name: ```gcloud deployment-manager deployments create ga-flattener-deployment --config ga_flattener.yaml```
    * Please refer to the [documentation](https://cloud.google.com/deployment-manager/docs/deployments) for more examples of valid values of **[Deployment Name]** 

## Verification steps ##
1. After installation, a configuration file named config_datasets.json exists in **gs://[Deployment Name]-[PROJECT_NUMBER]-adswerve-ga-flat-config/** (Cloud Storage Bucket within **[PROJECT_ID]**).  This file contains all the datasets that have ```events_yyyymmdd``` tables and which tables to unnest.  This configuration is required for this GA4 flattener solution to run daily or to backfill historical data.  Edit this file accordingly to include or exclude certain datasets or tables to unnest.  For example:
 * ```{"analytics_123456789": ["events", "event_params"]}```   will only flatten those 2 nested tables for GA4 property 123456789
 * ```{"analytics_123456789": ["events", "event_params", "user_properties", "items"], "987654321": ["events", "event_params"]}``` will flatten all possible nested tables for GA4 property 123456789 but only events_ and event_params_ for GA4 property 987654321.

_**The following steps are only required if you plan to backfill historical tables._**   
2. Modify values in the configuration section of tools/pubsub_message_publish.py accordingly.  **Suggestion:** Use a small date range to start, like yesterday only.
3. From a gcloud command prompt, authenticate the installing user using command:
   ```gcloud auth application-default login```
4. Run tools/pubsub_message_publish.py locally, which will publish a
   simulated logging event of GA4 data being ingested into BigQuery.  Check dataset(s) that are configured for new date sharded tables such as (depending on what is configured):
    * flat_event_params_(x)
    * flat_events_(x)
    * flat_items_(x)
    * flat_user_properties_(x)
   
## Un-install steps ##
1. Delete the config_datasets.json file from gs://[Deployment Name]-[PROJECT_NUMBER]-adswerve-ga-flat-config/ (Cloud Storage Bucket within [PROJECT_ID])
2. Optional command to remove solution: 
   * ```gcloud deployment-manager deployments delete **[Deployment Name]** -q```

## Common errors ##
### Install ###
* * **Message:** AccessDeniedException: 403 **[PROJECT_NUMBER]**@cloudbuild.gserviceaccount.com does not have storage.objects.list access to the Google Cloud Storage bucket.
  * **Resolution:** Ensure the value (Cloud Storage bucket name) configured in "codeBucket" setting of ga_flattener*.yaml is correct. **[PROJECT_NUMBER]**@cloudbuild.gserviceaccount.com only requires GCP predefined role of _Cloud Build Service Account_
### Verification ###
* * **Message:**   google.auth.exceptions.DefaultCredentialsError: Could not automatically determine credentials. Please set GOOGLE_APPLICATION_CREDENTIALS or explicitly create credentials and re-run the application. For more information, please see https://cloud.google.com/docs/authentication/getting-started
   * **Resolution:** Ensure you run the gcloud command ```gcloud auth application-default login``` as this sets up the required authentication and it'll just work.   
    
## Repository directories ##
* cf : pub/sub triggered cloud function that executes a destination
  query to unnest(flatten) the ```analytics_{ga4_property_id}.ga_sessions_yyyymmdd``` table
  immediately upon arrival in BigQuery into these tables, depending on the configuration:
  * flat_event_params_yyyymmdd
  * flat_events_yyyymmdd
  * flat_items_yyyymmdd
  * flat_user_properties_yyyymmdd
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

## Running unit tests ##
* To run unit tests on your local machine, save an **sa.json** with GCP account credentials in the **sandbox** directory.
* Unit tests run locally or via GitHub CI/CD workflow assume that GA4 flattener is installed into your GCP project.

