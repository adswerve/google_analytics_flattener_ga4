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
* [Installation steps](#installation-steps)
    + [Installation commands recap](#installation-commands-recap)
    + [Deployment naming conventions](#deployment-naming-conventions)
* [Verification steps](#verification-steps)
+ [About the config file](#about-the-config-file)
  + [How do edit the config file](#how-do-edit-the-config-file)
+ [Intraday flattening](#intraday-flattening)
  + [Background on intraday flattening](#background-on-intraday-flattening )
  + [How to enable intraday flattening in the config file](#how-to-enable-intraday-flattening-in-the-config-file)
  + [Disclaimer regarding GA4 intraday data in BigQuery](#disclaimer-regarding-ga4-intraday-data-in-bigquery)
+ [Controlling the type of output](#controlling-the-type-of-output)
  + [Background on sharded and partitioned output](#background-on-sharded-and-partitioned-output)
  + [How to enable partitioned output in the config file](#how-to-enable-partitioned-output-in-the-config-file)
+ [Config file examples](#config-file-examples)
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

1. Browse to https://console.cloud.google.com/ to create Google GCP project or use an existing project that has Google
   Analytics data flowing to it. Referred to as **[PROJECT_ID]**.

2. Grant the installing user (you most likely) the basic IAM role of `Editor`.

3. As the installing user for **[PROJECT_ID]**, enable the following APIs
    * Cloud Build API
    * Cloud Deployment Manager V2 API
    * Cloud Functions API
    * Artifact Registry API 
    * Identity and Access Management (IAM) API
    * Cloud Scheduler API (if you need to flatten intraday tables)

4. As the installing user for **[PROJECT_ID]**, create a bucket or use an existing bucket for staging code, for example:
   **`[PROJECT_NUMBER]**-function-code-staging`. Referred to as **[BUCKET_NAME]**.

    We recommend adding a label to this GCS bucket, for example:

    `key: bucket`

    `value: ga4-flattener-function-code-staging`

    This label will help you roughly estimate GCS cost related to flattener. 


5. Clone this github repo or download the source code from the releases section to your local machine or cloud shell.

6. Edit the _ga_flattener.yaml_ and _ga_flattener_colon.yaml_ files, specifically all occurrences of _properties-->
   codeBucket_ value. Set the value to **[BUCKET_NAME]** (see step above)

## Installation steps ##

1. Execute command in Google Cloud SDK Shell: `gcloud config set project **[PROJECT_ID]**`

    - alternatively, you can use this commands:


    ```
    # instantiate a configuration (connect to a GCP project)
    gcloud init 

    # activate the configuration
    gcloud config configurations activate {configuration_name} 

    # make sure you're in the right GCP project)
    gcloud config configurations list
    ```

2. Execute command: `gcloud config set account <username@domain.com>`. **Note** - This must be the installing user from above prerequisites. Feel free to skip this part unless it's required - I usually skip it as GCP already detects the user.
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
  
   ### Installation commands recap

   The steps above can be summarized in the template below. When I install the flattener, I like referring to the commands below. I adjust the commands to a specific project run one command at a time. 

    ```
    # INSTALL

    # initiate the configuration
    gcloud init

    # view configurations
    gcloud config configurations list

    # activate the right configuration
    gcloud config configurations activate {configuration}

    # dir

    cd {code_directory}

    # install
    gcloud deployment-manager deployments create ga4-flattener-deployment --config ga_flattener.yaml

    # DESCRIBE

    # verify installation
    gcloud deployment-manager deployments describe ga4-flattener-deployment

    # UNINSTALL

    # delete the flattener config file
    gsutil rm gs://ga4-flattener-deployment-{project_number}-adswerve-ga-flat-config/config_datasets.json
    
    # uninstall
    gcloud deployment-manager deployments delete ga4-flattener-deployment -q
    ```

   ### Deployment naming conventions
   * Deployment name must be a match of regex `'[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?'`
    * Deployment naming conventions are as follows:
      * Starts with a lowercase letter
      * Can contain lowercase letters, numbers and hyphens
      * Ends with a lowercase letter or number
      * Up to 63 characters long (a beginning lowercase letter + 61 chars (hyphens, lowercase letters and number) + the ending lowercase letter or a number)
    * Note that deployment name cannot have underscores in its name, but can have hyphens.
    * An example of a valid
      name is in this command: 
      
      ```gcloud deployment-manager deployments create ga4-flattener-deployment --config ga_flattener.yaml```

    * Please refer to the [documentation](https://cloud.google.com/deployment-manager/docs/deployments) for more examples of valid deployment names.
    

## Verification steps ##

Below are some ideas, you don't have to do all of them.

- Important checks: 

  - Check to make sure the deployement command gave no errors

  - Check to make sure the config file is correct. See below more information about the config file.

  - Run backfill (at least for a couple of days) to make sure there are no errors in the Cloud Functions and flat data is being written. See the section about backfill.

- Optional checks: 

  - Check GCP Cloud Functions to make sure they deployed successfully. 

  - Check GCP logs to make sure there are no errors in the flattening Cloud Function.

## About the config file

1. After installation, a configuration file named ```config_datasets.json``` exists in **gs://[deployment_name]
   -[PROJECT_NUMBER]-adswerve-ga-flat-config/** (Cloud Storage Bucket within **[PROJECT_ID]**).

- This file contains all the datasets that have ```events_yyyymmdd``` tables and which tables to unnest.

- This configuration is required for this GA4 flattener solution to run daily or to backfill historical data.

- Edit this file accordingly to include or exclude certain datasets or tables to unnest.

- You can enable intraday flattening via this config file and specify its frequency in hours or minutes.

- You can also enable sharded and/or partitioned output in the config file.

### How do edit the config file

If you want non-default config file options, you need to do the following:

- Manually download the `config_datasets.json` file from GCS (Google Cloud Storage)

- Edit it locally in a text/code upload

- Upload the config file back to GCS and overwrite the old version.


## Intraday flattening 

### Background on intraday flattening 

- In addition to daily tables ```events_yyyymmdd```, you may also have the table ```events_intraday_yyyymmdd```, which has data from today and refreshes in real-time.

- To get the intraday BigQuery table, you need to check `Frequency`: `Streaming` in your GA4 BigQuery linking. 

- By default, the flattener does not flatten the intraday table.

- You can enable intraday flattening by editing the config file (see instructions below).

- There are two kinds of intraday table, you can have either of them, neither of them or both. The flattener can write flat intraday to a BigQuery table and/or to a BigQuery SQL view.

- About intraday **table** flattening:

  - If you don't have have a daily table yet for a specific date (usually, today or yesterday), but you have an intraday table for that date, then the intraday table will be flattened. You will have flat tables, such as ```flat_events_yyyymmdd```, which are based on a specific date's intraday table ```events_intraday_yyyymmdd``.

  - At night, Google deletes the intraday table for a specific date and writes a daily table for that date.

  - The flattener Cloud Function runs and ovewrites the flat table `flat_events_yyyymmdd`. Now you have flat tables based on **daily** table `events_yyyymmdd`.

- About intraday **view** flattening:
  - If it is enabled, you will have SQL views, for example `view_flat_events_{todays_date}`. Such views will be automatically created and deleted based on the source intraday table `events_intraday_yyyymmdd`.

### How to enable intraday flattening in the config file


- You can enable intraday flattening by editing the config file and supplying the following part: 
    ```
    "intraday_flattening": {
        "intraday_flat_tables_schedule": {
          "frequency": 1,
          "units": "hours"
        },
        "intraday_flat_views": true
      }
    ```

  - ```"units"``` can be ```"hours"``` or ```"minutes"```
  - ```"frequency"``` is an integer.
  - if your units are minutes, then the frequency should be between 1 and 59.

- The flattened intraday tables will be overwritten at the specified frequency. 


### Disclaimer regarding GA4 intraday data in BigQuery

- In the intraday table, user properties are not accurate, until the daily table arrives

- The flat intraday table doesn't say it's based on intraday data in its name. The flat intraday table is called `flat_events_{date}`.

- therefore, please interpret flattened intraday data with caution 

## Controlling the type of output

### Background on sharded and partitioned output

Enabling partitioned output in the config file

- By default, the flattener produces flat sharded tables, because the original GA tables are also sharded.
- You can enable the following in the config file:

    A) sharded output

    B) paritioned output

    C) both sharded and partitioned output. It means you have two sets of flat tables: sharded flat tables and partitioned flat tables. 


### How to enable partitioned output in the config file
- You configure output type by changing the following part of the config file: ```"output": {"sharded": true, "partitioned": false}```. If you only want partitioned output, This default option should be changed to this: ```"output": {"sharded": false, "partitioned": true}```

## Config file examples

Example 1 - default config 

```
{
  "analytics_222460912": {
    "tables_to_flatten": [
      "events",
      "event_params",
      "user_properties",
      "items"
    ],
    "output_format": {
      "sharded": true,
      "partitioned": false
    },
    "intraday_flattening": {
      "intraday_flat_tables_schedule": null,
      "intraday_flat_views": true
    }
  }
}
``` 

  - This is the default config file. 
  - 4 flat tables will be created. 
  - There will be no flattening of the intraday table written to a BigQuery table, however, there will be an intraday SQL view. 
  - Only sharded output is produced.

Example 2 - excluding tables from flattening.

 ```
{
  "analytics_222460912": {
    "tables_to_flatten": [
      "events",
      "event_params"
    ],
    "output_format": {
      "sharded": true,
      "partitioned": false
    },
    "intraday_flattening": {
      "intraday_flat_tables_schedule": null,
      "intraday_flat_views": true
    }
  }
}
 ``` 

  - This config file will only create 2 flat tables for one GA4 property. 


Example 3 - adding more datasets, intraday flattening and partitioned output.

 ```
{
  "analytics_222460912": {
    "tables_to_flatten": [
      "events",
      "event_params",
      "user_properties",
      "items"
    ],
    "output_format": {
      "sharded": true,
      "partitioned": true
    },
    "intraday_flattening": {
      "intraday_flat_tables_schedule": {
        "frequency": 1,
        "units": "hours"
      },
      "intraday_flat_views": true
    }
  },

  "analytics_123456789": {
    "tables_to_flatten": [
      "events",
      "event_params",
      "user_properties",
      "items"
    ],
    "output_format": {
      "sharded": true,
      "partitioned": true
    },
    "intraday_flattening": {
      "intraday_flat_tables_schedule": {
        "frequency": 30,
        "units": "minutes"
      },
      "intraday_flat_views": true
    }
  }

}
  ```

  - This config file will create all 4 flat tables for each of the 2 GA4 properties. 
  - In both properties, we will also do intraday flattening. 
  - In both properties, we create both shaded and partitioned output.


- See the examples in the ```./sample_config/*``` in this repo.

## Backfilling steps

**The following steps are only required if you plan to backfill historical tables.**
1. Make sure that you performed the steps from [backfilling prerequisites](#backfilling-prerequisites).


2. Install Python >= 3.7

3. From Mac Terminal or Windows Command Prompt, upgrade pip:

   Mac:

   ```python3 -m pip install --upgrade pip```

   Windows:

   ```py -m pip install --upgrade pip```

4. Navigate to the root directory of the source code that was downloaded or cloned in step 6 above.

5. From a command prompt, install python virtual environments:

    Mac:

    ```python3 -m pip install --user virtualenv```

    Windows:

    ```py -m pip install --user virtualenv```

6. Create a virtual environment for the source code in step 6:

    Mac:

    ```python3 -m venv venv_ga_flattener```

    Windows:

    ```py -m venv venv_ga_flattener```

7. Activate the virtual environment in the step above:

    Mac:

    ```source venv_ga_flattener/bin/activate```

    Windows:

    ```.\venv_ga_flattener\Scripts\activate```

8. Install the python dependent packages into the virtual environment:

    Mac:

    ```pip install -r cf/requirements.txt```

    Windows:

    ```pip install -r cf/requirements.txt```


9. Modify values in the configuration section of `tools/pubsub_message_publish.py` accordingly. Use a
   small date range to start, like yesterday only.

10. From gcloud CLI, authenticate the installing user using command:
   ```gcloud auth application-default login```

11. Run tools/pubsub_message_publish.py locally, for example from the package root:
    
    ```python -m tools.pubsub_message_publish```

   This will publish a simulated logging event of GA4 data being ingested
   into BigQuery. Check dataset(s) that are configured for new date sharded tables such as (depending on what is
   configured): `flat_event_params_yyyymmdd`, `flat_events_yyyymmdd`, `flat_items_yyyymmdd`, `flat_user_properties_yyyymmdd`.

Tip: if you are having issues running the backfill locally (on your machine) due to some local environment peculiarities, try running the backfill on GCP using Cloud Shell.

Activate GCP Cloud Shell and run the following commands:
```

git clone https://github.com/adswerve/google_analytics_flattener_ga4

# go into root dir of the project
cd google_analytics_flattener_ga4

# make sure you're in the right directory:
ls -a

# Make the required changes to backfill Python script. Modify values in the configuration section of `tools/pubsub_message_publish.py` accordingly

python3 -m pip install --upgrade pip

python3 -m pip install --user virtualenv

python3 -m venv venv_ga_flattener

source venv_ga_flattener/bin/activate

pip install -r cf/requirements.txt

# you will only need this command on your machine. While using Cloud Shell, you can skip this command
# gcloud auth application-default login

# make sure we're inside the venv and inside the root flattener directory

python -m tools.pubsub_message_publish

# authorize the request in GCP UI if required

# the backfill script will print something like this: 
# INFO: Publishing backfill message to topic ga4-flattener-deployment-topic for {project_id}.analytics_{ga4_property_id}.events_{date_shard}

```


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
### local testing:
* To make sure BigQuery analysis cost is billed to the right project and the SQL queries run in the correct project, activate the correct gcloud configuration: `gcloud config configurations activate {configuration_name}`
* If necessary, create a new gcloud configuration: `gcloud init`
* To run unit tests on your local machine, authenticate locally with ```gcloud auth application-default login```. You could also save a file **sa.json** with GCP account credentials in the **sandbox**
  directory and specify path to it in `test_base.py`. 
* Set the environment variables in `test_base.py`: `"deployment"`, `"project"` and `"username"`.
* Unit tests run locally assume that GA4 flattener is installed into your GCP project.

### GitHub CI/CD:
* GitHub CI/CD pipeline uses automatically saves **sa.json** into the **sandbox** directory and
  sets ```GOOGLE_APPLICATION_CREDENTIALS``` to the filepath.