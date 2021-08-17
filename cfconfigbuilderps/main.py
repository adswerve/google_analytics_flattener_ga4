from google.cloud import bigquery
from google.cloud import storage
import tempfile
import json
import os
import logging
import sys

# configure logger to add log cal to stdout call (i.e., to print log message to console)
# create logger
root = logging.getLogger()
root.setLevel(logging.INFO) # what log severity are we going to capture?

# create console handler and set level
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO) # out of the logs we captured above, what log severity are we going to add to stdout (print to console)?

# create formatter
formatter = logging.Formatter('%(levelname)s - %(message)s')

# add formatter to ch
handler.setFormatter(formatter)

root.addHandler(handler)
# https://stackoverflow.com/questions/14058453/making-python-loggers-output-all-messages-to-stdout-in-addition-to-log-file
# https://docs.python.org/3/howto/logging.html

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


def build_ga_flattener_config(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    config = FlattenerDatasetConfig()
    store = FlattenerDatasetConfigStorage()
    json_config = config.get_ga_datasets()
    store.upload_config(config=json_config)
    logging.info("build_ga_flattener_config: {}".format(json.dumps(json_config)))
