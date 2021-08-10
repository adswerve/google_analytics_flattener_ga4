from google.cloud import pubsub_v1, storage
import json
import time
from datetime import date
import os
import tempfile

class IntradayFlattenerDatasetConfigStorage(object):
    def __init__(self):
        self.bucket_name = os.environ["config_bucket_name"]
        self.config_filename = os.environ["config_filename"]

def create_intraday_schedule(event, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed.

    Args:
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))

    print('event: {}'.format(event))
    print("event:")
    print(event)

    print("context:")
    print('context: {}'.format(context))
    print(context)

    # project_id = event['resource']['labels']['project_id'] # failing, as i can only parse payload, not log metadata


    # print('project_id: {}'.format(project_id)

    storage_client = storage.Client()

    if event['name'] == IntradayFlattenerDatasetConfigStorage().config_filename:

        bucket = storage_client.bucket(IntradayFlattenerDatasetConfigStorage().bucket_name)
        blob = bucket.blob(IntradayFlattenerDatasetConfigStorage().config_filename)

        downloaded_file = os.path.join(tempfile.gettempdir(), "tmp.json")
        blob.download_to_filename(downloaded_file)
        with open(downloaded_file, "r") as config_json_intraday:
            config_intraday = json.load(config_json_intraday)

        print('config_intraday: {}'.format(str(config_intraday)))
        # datasets_to_flatten_intraday_data = [key for key, value in config_intraday.items()]  # GA properties
        #
        # for dataset in datasets_to_flatten_intraday_data:
        #     if config_intraday