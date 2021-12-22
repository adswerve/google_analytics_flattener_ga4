import base64
import json
from google.cloud import bigquery
from google.cloud import storage
import re
import os
import tempfile
import logging
from datetime import datetime

class InputValidator(object):
    def __init__(self, event):
        try:
            # validate input message
            # extract information from message payload
            message_payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
            bq_destination_table = \
                message_payload['protoPayload']['serviceData']['jobCompletedEvent']['job']['jobConfiguration']['load'][
                    'destinationTable']
            self.gcp_project = bq_destination_table['projectId']
            self.dataset = bq_destination_table['datasetId']
            self.table_date_shard = re.search(r'_(20\d\d\d\d\d\d)$', bq_destination_table['tableId']).group(1)
            self.table_name = re.search(r'(events.*)_20\d\d\d\d\d\d$', bq_destination_table['tableId']).group(1)
        except AttributeError:
            logging.critical(f'invalid message: {message_payload}')
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(os.environ["CONFIG_BUCKET_NAME"])
            blob = bucket.blob(os.environ["CONFIG_FILENAME"])
            downloaded_file = os.path.join(tempfile.gettempdir(), "tmp.json")
            blob.download_to_filename(downloaded_file)
            with open(downloaded_file, "r") as config_json:
                self.config = json.load(config_json)
        except Exception as e:
            logging.critical(f'flattener configuration error: {e}')

    def valid_dataset(self):
        return self.dataset in self.config.keys()

    def flatten_nested_table(self, nested_table):
        return nested_table in self.config[self.dataset]["tables_to_flatten"]


class GaExportedNestedDataStorage(object):
    def __init__(self, gcp_project, dataset, table_name, date_shard, type='DAILY'):

        # main configurations
        self.gcp_project = gcp_project
        self.dataset = dataset
        self.date_shard = date_shard
        self.table_name = table_name
        self.type = type

        # The next several properties will correspond to GA4 fields

        # These fields will be used to build a compound id of a unique event
        # stream_id is added to make sure that there is definitely no id collisions, if you have multiple data streams
        self.unique_event_id_fields = [
            "stream_id",
            "user_pseudo_id",
            "event_name",
            "event_timestamp"
        ]

        # event parameters
        self.event_params_fields = [
            "event_params.key",

            "event_params.value.string_value",
            "event_params.value.int_value",
            "event_params.value.float_value",
            "event_params.value.double_value"
        ]

        # user properties
        self.user_properties_fields = [
            "user_properties.key",

            "user_properties.value.string_value",
            "user_properties.value.int_value",
            "user_properties.value.float_value",
            "user_properties.value.double_value",

            "user_properties.value.set_timestamp_micros"
        ]

        # items
        self.items_fields = [
            "items.item_id",
            "items.item_name",
            "items.item_brand",
            "items.item_variant",
            "items.item_category",
            "items.item_category2",
            "items.item_category3",
            "items.item_category4",
            "items.item_category5",
            "items.price_in_usd",
            "items.price",
            "items.quantity",
            "items.item_revenue_in_usd",
            "items.item_revenue",
            "items.item_refund_in_usd",
            "items.item_refund",
            "items.coupon",
            "items.affiliation",
            "items.location_id",
            "items.item_list_id",
            "items.item_list_name",
            "items.item_list_index",
            "items.promotion_id",
            "items.promotion_name",
            "items.creative_name",
            "items.creative_slot"
        ]

        # events
        self.events_fields = [
            "event_date",
            "event_timestamp",
            "event_name",
            "event_previous_timestamp",
            "event_value_in_usd",
            "event_bundle_sequence_id",
            "event_server_timestamp_offset",
            "user_id",
            "user_pseudo_id",

            "privacy_info.analytics_storage",
            "privacy_info.ads_storage",
            "privacy_info.uses_transient_token",
            "user_first_touch_timestamp",

            "user_ltv.revenue",
            "user_ltv.currency",

            "device.category",
            "device.mobile_brand_name",
            "device.mobile_model_name",
            "device.mobile_marketing_name",
            "device.mobile_os_hardware_model",
            "device.operating_system",
            "device.operating_system_version",
            "device.vendor_id",
            "device.advertising_id",
            "device.language",
            "device.is_limited_ad_tracking",
            "device.time_zone_offset_seconds",
            "device.browser",
            "device.browser_version",

            "device.web_info.browser",
            "device.web_info.browser_version",
            "device.web_info.hostname",

            "geo.continent",
            "geo.country",
            "geo.region",
            "geo.city",
            "geo.sub_continent",
            "geo.metro",

            "app_info.id",
            "app_info.version",
            "app_info.install_store",
            "app_info.firebase_app_id",
            "app_info.install_source",

            "traffic_source.name",
            "traffic_source.medium",
            "traffic_source.source",
            "stream_id",
            "platform",

            "event_dimensions.hostname",

            "ecommerce.total_item_quantity",
            "ecommerce.purchase_revenue_in_usd",
            "ecommerce.purchase_revenue",
            "ecommerce.refund_value_in_usd",
            "ecommerce.refund_value",
            "ecommerce.shipping_value_in_usd",
            "ecommerce.shipping_value",
            "ecommerce.tax_value_in_usd",
            "ecommerce.tax_value",
            "ecommerce.unique_items",
            "ecommerce.transaction_id"
        ]

    def get_unique_event_id(self, unique_event_id_fields):
        """
        build unique event id
        """
        return 'CONCAT(%s, "_", %s, "_", %s, "_", %s) as event_id' % (unique_event_id_fields[0],
                                                                      unique_event_id_fields[1],
                                                                      unique_event_id_fields[2],
                                                                      unique_event_id_fields[3])

    def get_event_params_query(self):
        qry = "SELECT "

        # get unique event id
        qry += self.get_unique_event_id(self.unique_event_id_fields)

        qry += ",%s as %s" % (self.event_params_fields[0], self.event_params_fields[0].replace(".", "_"))

        qry += ",CONCAT(IFNULL(%s, ''), IFNULL(CAST(%s AS STRING), ''), IFNULL(CAST(%s AS STRING), ''), IFNULL(CAST(%s AS STRING), '')) AS event_params_value" \
               % (self.event_params_fields[1], self.event_params_fields[2], self.event_params_fields[3],
                  self.event_params_fields[4])

        qry += " FROM `{p}.{ds}.{t}_{d}`".format(p=self.gcp_project, ds=self.dataset, t=self.table_name,
                                                 d=self.date_shard)

        qry += ",UNNEST (event_params) AS event_params"

        return qry

    def get_user_properties_query(self):
        qry = "SELECT "

        # get unique event id
        qry += self.get_unique_event_id(self.unique_event_id_fields)

        qry += ",%s as %s" % (self.user_properties_fields[0], self.user_properties_fields[0].replace(".", "_"))

        qry += ",CONCAT(IFNULL(%s, ''), IFNULL(CAST(%s AS STRING), ''), IFNULL(CAST(%s AS STRING), ''), IFNULL(CAST(%s AS STRING), '')) AS user_properties_value" \
               % (self.user_properties_fields[1], self.user_properties_fields[2], self.user_properties_fields[3],
                  self.user_properties_fields[4])

        qry += ",%s as %s" % (self.user_properties_fields[5], self.user_properties_fields[5].replace(".", "_"))

        qry += " FROM `{p}.{ds}.{t}_{d}`".format(p=self.gcp_project, ds=self.dataset, t=self.table_name,
                                                 d=self.date_shard)

        qry += ",UNNEST (user_properties) AS user_properties"

        return qry

    def get_items_query(self):
        qry = "SELECT "

        # get unique event id
        qry += self.get_unique_event_id(self.unique_event_id_fields)

        for f in self.items_fields:
            qry += ",%s as %s" % (f, f.replace(".", "_"))

        qry += " FROM `{p}.{ds}.{t}_{d}`".format(p=self.gcp_project, ds=self.dataset, t=self.table_name,
                                                 d=self.date_shard)

        qry += ",UNNEST (items) AS items"

        return qry

    def get_events_query(self):
        qry = "SELECT "

        # get unique event id
        qry += self.get_unique_event_id(self.unique_event_id_fields)

        for f in self.events_fields:
            qry += ",%s as %s" % (f, f.replace(".", "_"))

        qry += " FROM `{p}.{ds}.{t}_{d}`".format(p=self.gcp_project, ds=self.dataset, t=self.table_name,
                                                 d=self.date_shard)
        return qry

    def _create_valid_bigquery_field_name(self, p_field):
        '''
        Creates a valid BigQuery field name
        BQ Fields must contain only letters, numbers, and underscores, start with a letter or underscore,
        and be at most 300 characters long.
        :param p_field: starting point of the field
        :return: cleaned big query field name
        '''
        r = ""  # initialize emptry string
        for char in p_field.lower():
            if char.isalnum():
                # if char is alphanumeric (either letters or numbers), append char to our string
                r += char
            else:
                # otherwise, replace it with underscore
                r += "_"
        # if field starts with digit, prepend it with underscore
        if r[0].isdigit():
            r = "_%s" % r
        return r[:300]  # trim the string to the first x chars

    def run_query_job(self, query, table_type='flat'):
        client = bigquery.Client()  # initialize BigQuery client
        # get table name
        table_name = "{p}.{ds}.{t}_{d}" \
            .format(p=self.gcp_project, ds=self.dataset, t=table_type, d=self.date_shard)
        table_id = bigquery.Table(table_name)
        # configure query job
        query_job_config = bigquery.QueryJobConfig(
            destination=table_id
            , dry_run=False
            , use_query_cache=False
            , labels={"queryfunction": "flatteningquery"}  # todo: apply proper labels
            , write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        # run the job
        query_job = client.query(query,
                                 job_config=query_job_config)
        query_job_result = query_job.result()  # Waits for job to complete.

        # BQ -> pandas df
        # # https://cloud.google.com/bigquery/docs/bigquery-storage-python-pandas#download_query_results_using_the_client_library

        dataframe = query_job_result.to_dataframe()

        # add date field to the dataframe
        date = datetime.strptime(self.date_shard, '%Y%m%d')

        partitioning_column = "event_date"

        dataframe[partitioning_column] = date

        # https://stackoverflow.com/questions/25122099/move-column-by-name-to-front-of-table-in-pandas
        col = dataframe[partitioning_column]
        dataframe.drop(labels=[partitioning_column], axis=1, inplace=True)
        dataframe.insert(0, partitioning_column, col)

        # pandas df -> BQ
        # https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe


        job_config_partitioned = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema=[
                # Specify the type of columns whose type cannot be auto-detected. For
                # example the "title" column uses pandas dtype "object", so its
                # data type is ambiguous.
                bigquery.SchemaField("event_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("event_id", bigquery.enums.SqlTypeNames.STRING),
                # Indexes are written if included in the schema by name.
                bigquery.SchemaField("event_params_key", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("event_params_value", bigquery.enums.SqlTypeNames.STRING),
            ],
            # https://stackoverflow.com/questions/59430708/how-to-load-dataframe-into-bigquery-partitioned-table-from-cloud-function-with-p
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partitioning_column # field to use for partitioning
            ),
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            write_disposition="WRITE_APPEND"
        )

        table_name_partitioned = "{p}.{ds}.{t}" \
            .format(p=self.gcp_project, ds=self.dataset, t=table_type)
        table_id_partitioned = bigquery.Table(table_name_partitioned)


        job = client.load_table_from_dataframe(
            dataframe=dataframe, destination=table_id_partitioned, job_config=job_config_partitioned
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id_partitioned)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id_partitioned
            )
        )

        return


def flatten_ga_data(event, context):
    """
    Flatten GA4 data
    Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    input_event = InputValidator(event)

    if input_event.valid_dataset():
        ga_source = GaExportedNestedDataStorage(gcp_project=input_event.gcp_project,
                                                dataset=input_event.dataset,
                                                table_name=input_event.table_name,
                                                date_shard=input_event.table_date_shard)

        # EVENT_PARAMS
        if input_event.flatten_nested_table(nested_table=os.environ["EVENT_PARAMS"]):
            ga_source.run_query_job(query=ga_source.get_event_params_query(), table_type="flat_event_params")
            logging.info(f'Ran {os.environ["EVENT_PARAMS"]} flattening query for {input_event.dataset}')
        else:
            logging.info(
                f'{os.environ["EVENT_PARAMS"]} flattening query for {input_event.dataset} not configured to run')

        # USER_PROPERTIES
        if input_event.flatten_nested_table(nested_table=os.environ["USER_PROPERTIES"]):
            ga_source.run_query_job(query=ga_source.get_user_properties_query(), table_type="flat_user_properties")
            logging.info(f'Ran {os.environ["USER_PROPERTIES"]} flattening query for {input_event.dataset}')
        else:
            logging.info(
                f'{os.environ["USER_PROPERTIES"]} flattening query for {input_event.dataset} not configured to run')

        # ITEMS
        if input_event.flatten_nested_table(nested_table=os.environ["ITEMS"]):
            ga_source.run_query_job(query=ga_source.get_items_query(), table_type="flat_items")
            logging.info(f'Ran {os.environ["ITEMS"]} flattening query for {input_event.dataset}')
        else:
            logging.info(f'{os.environ["ITEMS"]} flattening query for {input_event.dataset} not configured to run')

        # EVENTS
        if input_event.flatten_nested_table(nested_table=os.environ["EVENTS"]):
            ga_source.run_query_job(query=ga_source.get_events_query(), table_type="flat_events")
            logging.info(f'Ran {os.environ["EVENTS"]} flattening query for {input_event.dataset}')
        else:
            logging.info(f'{os.environ["EVENTS"]} flattening query for {input_event.dataset} not configured to run')

    else:
        logging.warning(f'Dataset {input_event.dataset} not configured for flattening')
