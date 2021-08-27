from google.cloud import bigquery
from google.cloud import storage
import tempfile
import json
import os


class FlattenerDatasetConfigStorage(object):
    def __init__(self):
        self.bucket_name = os.environ["config_bucket_name"]
    def upload_config(self,config):
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(os.environ["config_filename"])

        filepath = os.path.join(tempfile.gettempdir(), "tmp.json")
        with open(filepath, "w") as f:
            f.write(json.dumps(config))
        blob.upload_from_filename(filepath)


class FlattenerDatasetConfig(object):
    def __init__(self):
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
        ret_val = {}
        client = bigquery.Client()
        query_job = client.query(self.query)
        query_results = query_job.result()  # Waits for job to complete.
        for row in query_results:
            ret_val[(row.dataset_id)]=[os.environ["EVENTS"]
                ,os.environ["EVENT_PARAMS"]
                ,os.environ["USER_PROPERTIES"]
                ,os.environ["ITEMS"]
                ]
        return ret_val

    def add_intraday_info_into_config(self, json_config):
        """
        Adds intraday config params to config files.

        Args:
            json_config:
                {"analytics_222460912": ["events", "event_params", "user_properties", "items"],
                "analytics_251817041": ["events", "event_params", "user_properties", "items"]}

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
                    "intraday_schedule": null
                  },
                  "analytics_251817041": {
                    "tables_to_flatten": [
                      "events",
                      "event_params",
                      "user_properties",
                      "items"
                    ],
                    "intraday_schedule": null
                  }
                }
        Config file, after being transformed by this function, answers the following questions:
            In what datasets do we want to flatten intraday data?

            How often do we update flat intraday data (e.g., every X hours).
                Default frequency is null (meaning we won't be flattening intraday data)

        Example:
            Config file contains this:
                "analytics_222460912": {
                    "tables_to_flatten": [
                      ...
                    ],
                    "intraday_schedule": 3
                  }

                It means we will be flattening intraday data for "analytics_222460912" every 3 hours.

            Config file contains this:
                "analytics_222460912": {
                    "tables_to_flatten": [
                      ...
                    ],
                    "intraday_schedule": null
                  }

                We won't be flattening intraday data in "analytics_222460912"

        """
        intraday_configuration = {"intraday":[]}
        for key, value in json_config.items():
            intraday_configuration["intraday"].append({key:None})
        json_config.update(intraday_configuration)
        return json_config


def build_ga_flattener_config(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Request>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>.
    """
    config = FlattenerDatasetConfig()
    store = FlattenerDatasetConfigStorage()
    json_config = config.get_ga_datasets()
    json_config = config.add_intraday_info_into_config(json_config)
    store.upload_config(config=json_config)
    print("build_ga_flattener_config: {}".format(json.dumps(json_config)))
