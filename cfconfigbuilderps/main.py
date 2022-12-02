from google.cloud import bigquery
from google.cloud import storage
import tempfile
import json
import os
import logging


class FlattenerDatasetConfigStorage(object):
    def __init__(self):
        self.bucket_name = os.environ["CONFIG_BUCKET_NAME"]

    def upload_config(self, config):
        storage_client = storage.Client()  # initialize the GCS client
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(os.environ["CONFIG_FILENAME"])

        filepath = os.path.join(tempfile.gettempdir(), "tmp.json")
        with open(filepath, "w") as f:
            f.write(json.dumps(config))
        blob.upload_from_filename(filepath)


class FlattenerDatasetConfig(object):
    def __init__(self):
        """
        find ga4 datasets in project
        """
        self.query = """
        EXECUTE IMMEDIATE (
 WITH schemas AS (
  SELECT
    schema_name,
    LAST_VALUE(schema_name) OVER (PARTITION BY catalog_name ORDER BY schema_name ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_schema
  FROM
    INFORMATION_SCHEMA.SCHEMATA 
  where regexp_contains(schema_name,r'^analytics\\_\\d+$')
), static AS (
  SELECT
    "SELECT dataset_id FROM `%s.__TABLES__` where regexp_contains(table_id,r'^events.*\\\\d{8}$') and ((UNIX_MILLIS(current_timestamp()) - creation_time)/1000)/86400 < 30 group by 1" AS sql,
    " union all " AS cmd_u,
    " order by 1 " AS cmd_f 
)
SELECT
  ARRAY_TO_STRING(ARRAY_AGG(sql_command_rows),"") AS generated_sql_statement
FROM (
  SELECT
    CASE WHEN schemas.schema_name != schemas.last_schema THEN CONCAT(FORMAT(static.sql,schema_name),static.cmd_u)
         ELSE CONCAT(FORMAT(static.sql,schema_name),static.cmd_f)
     END AS sql_command_rows
  FROM
    static
  CROSS JOIN
    schemas
  ORDER BY
    schema_name ASC
      ) -- end of sub select
);  --end of dynamic SQL statement
"""

    def get_ga_datasets(self):
        """
        Obtain a list of GA4 datasets available in the GCP project.
        Build a configurations dict which lists GA4 datasets to flatten.
        It'll be later uploaded to GCS as a JSON file.
        """
        ret_val = {}
        client = bigquery.Client()
        query_job = client.query(self.query)
        query_results = query_job.result()  # Waits for job to complete.
        # the dictionary will list GA4 datasets
        # add tables information into dictionary
        # by default, all these 4 tables flat tables will be written by the flattener
        for row in query_results:  # each row is a ga4 dataset
            ret_val[(row.dataset_id)] = [os.environ["EVENTS"]
                , os.environ["EVENT_PARAMS"]
                , os.environ["USER_PROPERTIES"]
                , os.environ["ITEMS"]
                                         ]
        return ret_val

    def add_intraday_params_into_config(self, json_config, intraday_schedule_frequency=None,
                                      intraday_schedule_units="hours"):
        """
        Adds cfintraday config params to config file.

        Args:
            json_config:
                {
                  "analytics_222460912": [
                    "events",
                    "event_params",
                    "user_properties",
                    "items"
                  ],
                  "analytics_251817041": [
                    "events",
                    "event_params",
                    "user_properties",
                    "items"
                  ]
                }

        Returns:
            json_config:
                {
                  "analytics_222460912": {
                    "tables_to_flatten": [
                      "events",
                      "event_params",
                      "user_properties",
                      "items"
                    ],
                        "frequency": null,
                        "units": "hours"
                        }
                  },
                  "analytics_251817041": {
                    "tables_to_flatten": [
                      "events",
                      "event_params",
                      "user_properties",
                      "items"
                    ],
                    "frequency": null,
                    "units": "hours"
                        }
                  }
                }

        Config file, after being transformed by this function, answers the following questions:
            In what datasets do we want to flatten cfintraday data?

            How often do we update flat cfintraday data (e.g., every X minutes/hours).
                Default frequency is null (meaning we won't be flattening cfintraday data)
                Default schedule unit is hours, but can be changed to minutes.

        Example:
            Config file contains this:
                "analytics_222460912": {
                    "tables_to_flatten": [
                      ...
                    ],
                    "frequency": 15,
                    "units": "minutes"
                  }

                It means we will be flattening cfintraday data for "analytics_222460912" every 15 minutes.

                If the intraday schedule units is minutes, then intraday schedule frequency can only be a number between 1 and 59. It can't be 60+ (or else GCP with throw an invalid schedule error

        """
        json_config_updated = {}

        for dataset, list_of_tables in json_config.items():
            json_config_updated.update(
                {dataset: {"tables_to_flatten": list_of_tables, "intraday_schedule": {
                    "frequency": intraday_schedule_frequency,
                    "units": intraday_schedule_units
                }}})
        return json_config_updated

    def add_output_params_into_config(self, json_config, output_sharded=True,
                                      output_partitioned=False):
        """
        Adds cfintraday config params to config file.

        Args:
            json_config:
                {
                  "analytics_222460912": [
                    "events",
                    "event_params",
                    "user_properties",
                    "items"
                  ]
                }

        Returns:
            json_config:
                {
                  "analytics_222460912": [
                    "events",
                    "event_params",
                    "user_properties",
                    "items"
                  ],
                  "output": {
                    "sharded": true,
                    "partitioned": false
                  }
                }

        Config file, after being transformed by this function, answers the following questions:
            Do we want sharded, partitioned output, or both?
        """
        for dataset, config in json_config.items():
            config.update(
                            {"output": {
                  "sharded": output_sharded,
                  "partitioned": output_partitioned
                }})
        return json_config


def build_ga_flattener_config(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    config = FlattenerDatasetConfig()  # object with the SQL query which finds GA4 datasets
    store = FlattenerDatasetConfigStorage()  # object with the bucket_name as its property
    json_config = config.get_ga_datasets()  # build a configurations dict which lists GA4 datasets to flatten
    json_config = config.add_intraday_params_into_config(json_config)
    json_config = config.add_output_params_into_config(json_config)
    store.upload_config(config=json_config)  # upload config file to GCS bucket
    logging.info(f"build_ga_flattener_config: {json.dumps(json_config)}")
