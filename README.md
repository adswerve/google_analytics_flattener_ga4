# README #

Google Analytics 4 Flattener. A Google Cloud Platform (GCP) solution that unnests (
flattens) [Google Analytics Data 4 stored in Bigquery](https://support.google.com/analytics/answer/7029846?hl=en).  
The GCP resources for the solutions are installed via Deployment Manager.

[![Python package](https://github.com/adswerve/google_analytics_flattener_ga4/actions/workflows/python-package.yml/badge.svg)](https://github.com/adswerve/google_analytics_flattener_ga4/actions/workflows/python-package.yml)

## Contents

* [Background](#background)
* [Problem](#problem)
* [Purpose](#purpose)
* [Local dependencies](#local-dependencies)
* [Prerequisites](#prerequisites)
    + [Backfilling prerequisites](#backfilling-prerequisites)
* [Installation steps](#installation-steps)
* [Verification steps](#verification-steps)
    + [Config file](#config-file)
      + [Enabling intraday flattening via the config file](#enabling-intraday-flattening-via-the-config-file)
      + [Examples of a config file](#examples-of-a-config-file)
    + [Backfilling steps](#backfilling-steps)
* [Un-install steps](#un-install-steps)
* [Common errors](#common-errors)
    + [Install](#install)
    + [Verification](#verification)
* [Repository directories](#repository-directories)
* [Repository files](#repository-files)
* [Running unit tests](#running-unit-tests)

## Background

* [Google Analytics 4 (GA4)](https://support.google.com/analytics/answer/10089681?hl=en) is the newest generation of
  Google Analytics.
* When you export data from GA4 into BigQuery, the schema will
  have [nested and repeated fields](https://cloud.google.com/bigquery/docs/nested-repeated).
    * Nested data represents a struct. In BigQuery, it can be described as a column inside of a column.
    * Repeated data represents an array. In BigQuery, it can be described as a row inside of a row.

## Problem

* Schema with nested and repeated data:
    * Can make it harder to query the data (it takes a learning curve to get used to it).
    * May be incompatible with other data systems. It maybe be impossible to import data into another database system or
      into a spreadsheet.

## Purpose

* The purpose of GA4 Flattener is to solve the above problems by flattening/unnesting GA4 data.
    * Flattening/unnesting means converting all nested and repeated fields to regular fields (i.e., not a struct or an
      array).
    * As a result, our data becomes simpler and more easily compatible with other systems.
    * To imagine flat data, picture a Google Spreadsheet / Microsoft Excel table.
* GA4 Flattener keeps your original GA4 data with nested and repeated fiels, but it also writes 4 flat tables into the
  same BigQuery dataset.

## Local dependencies ##

* Google Cloud Platform SDK. Download and install from these instructions: https://cloud.google.com/sdk/docs/install
* Python >= 3.7. Download and install from https://python.org.
* Web browser
* git (**optional** for cloning this code repository)

## Prerequisites ##

1. Browse to https://cloud.console.google.com to create Google GCP project or use an existing project that has Google
   Analytics data flowing to it. Referred to as **[PROJECT_ID]**.

2. Grant the installing user (you most likely) the pre-defined IAM role of "Owner".

3. As the installing user for **[PROJECT_ID]**, enable the following APIs
    * Cloud Build API
    * Cloud Deployment Manager V2 API
    * Cloud Functions API
    * Identity and Access Management (IAM) API
    * Cloud Scheduler API (if you need to flatten intraday tables)

4. As the installing user for **[PROJECT_ID]**, grant the following pre-defined IAM roles to
   **[PROJECT_NUMBER]**@cloudservices.gserviceaccount.com (built in service account) otherwise deployment will fail with
   permission errors. See
   <https://cloud.google.com/deployment-manager/docs/access-control> for detailed explanation.
    * Logs Configuration Writer
    * Cloud Functions Developer
    * Pub/Sub Admin
    * Cloud Scheduler Admin (if you need to flatten intraday tables)

5. As the installing user for **[PROJECT_ID]**, create a bucket or use an existing bucket for staging code, for example:
   **[PROJECT_NUMBER]**-function-code-staging. Referred to as **[BUCKET_NAME]**.

If your GCP project is brand new, you might not have **[PROJECT_NUMBER]**@cloudservices.gserviceaccount.com yet. To fix
this, enable Compute Engine API and then disable it. The service account **[PROJECT_NUMBER]**
@cloudservices.gserviceaccount.com will appear in your GCP project under IAM.

6. Clone this github repo or download the source code from the releases section to your local machine or cloud shell.

7. Edit the _ga_flattener.yaml_ and _ga_flattener_colon.yaml_ files, specifically all occurrences of _properties-->
   codeBucket_ value. Set the value to **[BUCKET_NAME]** (see step above)

### Backfilling prerequisites ###

**The following steps are only required if you plan to backfill historical tables.**

8. Install python 3.7 or higher

9. From Mac Terminal or Windows Command Prompt, upgrade pip:

   Mac:

   ```python3 -m pip install --upgrade pip```

   Windows:

   ```py -m pip install --upgrade pip```

10. Navigate to the root directory of the source code that was downloaded or cloned in step 6 above.

11. From a command prompt, install python virtual environments:

    Mac:

    ```python3 -m pip install --user virtualenv```

    Windows:

    ```py -m pip install --user virtualenv```

12. Create a virtual environment for the source code in step 6:

    Mac:

    ```python3 -m venv venv_ga_flattener```

    Windows:

    ```py -m venv venv_ga_flattener```

13. Activate the virtual environment in the step above:

    Mac:

    ```source venv_ga_flattener/bin/activate```

    Windows:

    ```.\venv_ga_flattener\Scripts\activate```

14. Install the python dependent packages into the virtual environment:

    Mac:

    ```pip install -r cf/requirements.txt```

    Windows:

    ```pip install -r cf/requirements.txt```

## Installation steps ##

1. Execute command in Google Cloud SDK Shell: gcloud config set project **[PROJECT_ID]**
2. Execute command: gcloud config set account <username@domain.com>. **Note** - This must be the installing user from
   above prerequisites.
3. Navigate (locally) to root directory of this repository
4. If **[PROJECT_ID]** does **NOT** contain a colon (:) execute command:

   ```gcloud deployment-manager deployments create **[deployment_name]** --config ga_flattener.yaml```

   otherwise follow these steps:
    1. execute command:

       ```gcloud deployment-manager deployments create **[deployment_name]** --config ga_flattener_colon.yaml```

    2. Trigger function (with a blank message) named **[deployment_name]**-cfconfigbuilderps. It will create the
       necessary configuration file in the applications Google Coud Storage bucket. An easy method to do this is to
       browse to https://console.cloud.google.com/functions and click the cloud function named **[deployment_name]**
       -cfconfigbuilderps and go to the testing section and click "TEST THIS FUNCTION".

   ### **[deployment_name]** naming convention
    * Note that **[deployment_name]** cannot have underscores in its name, but can have hyphens.
    * Example of a valid
      name: ```gcloud deployment-manager deployments create ga-flattener-deployment --config ga_flattener.yaml```
    * Please refer to the [documentation](https://cloud.google.com/deployment-manager/docs/deployments) for more
      examples of valid values of **[deployment_name]**

## Verification steps ##

### Config file

1. After installation, a configuration file named ```config_datasets.json``` exists in **gs://[deployment_name]
   -[PROJECT_NUMBER]-adswerve-ga-flat-config/** (Cloud Storage Bucket within **[PROJECT_ID]**).

- This file contains all the datasets that have ```events_yyyymmdd``` tables and which tables to unnest.

- This configuration is required for this GA4 flattener solution to run daily or to backfill historical data.

- Edit this file accordingly to include or exclude certain datasets or tables to unnest.

- You can also enable intraday flattening via this config file and specify its frequency in hours or minutes.

#### Enabling intraday flattening via the config file

- In addition to daily tables ```events_yyyymmdd```, you may also have the table ```events_intraday_yyyymmdd```, which refreshes every few minutes.

- By default, the flattener does not flatten the intraday table.

- You can enable intraday flattening by editing the config file and supplying ```"intraday_schedule": {"frequency": "your_frequency", "units": "your_units"}```. 

  - ```"your_units"``` can be ```"hours"``` or ```"minutes"```
  - ```"your_frequency"``` is an integer.
  - if your units are minutes, then the frequency should be between 1 and 59.
  

#### Examples of a config file

- ```{"analytics_123456789": {"tables_to_flatten": ["events", "event_params", "user_properties", "items"], "intraday_schedule": {"frequency": null, "units": "hours"}}}``` 
  - this is the default config file. 
  - 4 flat tables will be created. 
  - There will be no flattening of the intraday table. 
  - You may notice that "intraday_schedule" is not necessary, but it provides a template in case you do want intraday flattening.

- ```{"analytics_123456789": {"tables_to_flatten": ["events", "event_params"], "intraday_schedule": {"frequency": null, "units": "hours"}}}``` 
  - this config file will only create 2 flat tables for one GA4 property. 
  - There will be no intraday flattening

- ```{"analytics_123456789": {"tables_to_flatten": ["events", "event_params", "user_properties", "items"], "intraday_schedule": {"frequency": 15, "units": "minutes"}},"analytics_987654321": {"tables_to_flatten": ["events", "event_params", "user_properties", "items"], "intraday_schedule": {"frequency": 1, "units": "hours"}}}```
  - this config file will create all 4 flat tables for each of the 2 GA4 properties. 
  - In both properties, we will also do
  intraday flattening. 
  - The flattened intraday tables will refresh every 15 minutes for the 1st dataset and every hour
  for the 2nd dataset.

- See another example in ```./sample_config/config_datasets_sample.json``` in this repo.

### Backfilling steps

**The following steps are only required if you plan to backfill historical tables.**

2. Modify values in the configuration section of tools/pubsub_message_publish.py accordingly.  **Suggestion:** Use a
   small date range to start, like yesterday only.
3. From a gcloud command prompt, authenticate the installing user using command:
   ```gcloud auth application-default login```
4. Run tools/pubsub_message_publish.py locally, which will publish a simulated logging event of GA4 data being ingested
   into BigQuery. Check dataset(s) that are configured for new date sharded tables such as (depending on what is
   configured):
    * flat_event_params_yyyymmdd
    * flat_events_yyyymmdd
    * flat_items_yyyymmdd
    * flat_user_properties_yyyymmdd

## Un-install steps ##

1. Delete the config_datasets.json file from gs://[deployment_name]-[PROJECT_NUMBER]-adswerve-ga-flat-config/ (Cloud
   Storage Bucket within [PROJECT_ID])
    * You can do this in GSC (Google Cloud Storage) UI or via command line:
      ```gsutil rm gs://[deployment_name]-[PROJECT_NUMBER]-adswerve-ga-flat-config/config_datasets.json```
2. Remove solution:
    * ```gcloud deployment-manager deployments delete **[deployment_name]** -q```

3. If you were doing intraday flattening, manually delete a Cloud Scheduler job(s) in the UI

## Common errors ##

### Install ###

- **Message**: AccessDeniedException: 403 **[PROJECT_NUMBER]**@cloudbuild.gserviceaccount.com does not have
  storage.objects.list access to the Google Cloud Storage bucket.

- **Resolution**: Ensure the value (Cloud Storage bucket name) configured in "codeBucket" setting of ga_flattener*
  .yaml is correct. **[PROJECT_NUMBER]**@cloudbuild.gserviceaccount.com only requires GCP predefined role of _Cloud
  Build Service Account_

### Verification ###

- **Message**: google.auth.exceptions.DefaultCredentialsError: Could not automatically determine credentials. Please set
  GOOGLE_APPLICATION_CREDENTIALS or explicitly create credentials and re-run the application. For more information,
  please see https://cloud.google.com/docs/authentication/getting-started

- **Resolution**: Ensure you run the gcloud command ```gcloud auth application-default login``` as this sets up the
  required authentication and it'll just work.

## Repository directories ##

* cf : Pub/Sub triggered cloud function that executes a destination query to unnest(flatten)
  the ```analytics_{ga4_property_id}.events_yyyymmdd``` table immediately upon arrival in BigQuery into these tables,
  depending on the configuration:
    * flat_event_params_yyyymmdd
    * flat_events_yyyymmdd
    * flat_items_yyyymmdd
    * flat_user_properties_yyyymmdd
* tests : units test for both cloud functions and deployment manager templates
* cfconfigbuilder(ps) : cloud function that finds all BigQuery datasets that have an events table and adds them to the
  default configuration on Google's Cloud Storage in the following location:
  [deployment_name]-[PROJECT_NUMBER]-adswerve-ga-flat-config\config_datasets.json

## Repository files ##

* dm_helper.py: provides consistent names for GCP resources accross solution. Configuration and constants also found in
  the class in this file
* dmt-*: any files prefixed with *dmt_* are python based Deployment Manager templates
* ga_flattener.yaml: Deployment Manager configuration file. The entire solution packages in this file. Used in the
  deployment manager *create* command
* tools/pubsub_message_publish.py : python based utility to publish a message to simulate an event that's being
  monitored in GCP logging. Useful for smoke testing and back-filling data historically.
* LICENSE: BSD 3-Clause open source license

## Running unit tests ##

* To run unit tests on your local machine, save an **sa.json** with GCP account credentials in the **sandbox**
  directory.
* However, it's not necessary, you can also auth locally with ```gcloud auth application-default login```.
* GitHub CI/CD pipeline uses automatically saves **sa.json** into the **sandbox** directory and
  sets ```GOOGLE_APPLICATION_CREDENTIALS``` to the filepath.
* Set the environment variables in `test_base.py`: `"deployment"`, `"project"` and `"username"`.
* Unit tests run locally or via GitHub CI/CD workflow assume that GA4 flattener is installed into your GCP project.

