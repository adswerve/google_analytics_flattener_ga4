import base64
import json
from google.cloud import bigquery
from google.cloud import storage
import re
import os
import tempfile
import logging
from datetime import datetime
from http import HTTPStatus
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

class InputValidator(object):
    def __init__(self, event):
        try:
            # validate input message
            # extract information from message payload
            message_payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
            bq_destination_table_path = \
                message_payload['protoPayload']['resourceName']
            self.gcp_project, self.dataset,  self.table_type, self.table_date_shard = self.extract_values(bq_destination_table_path)
        except ValueError as e:
            logging.critical(f"invalid message: {message_payload}")
        try:
            storage_client = storage.Client(project=self.gcp_project)
            bucket = storage_client.bucket(os.environ["CONFIG_BUCKET_NAME"])
            blob = bucket.blob(os.environ["CONFIG_FILENAME"])
            downloaded_file = os.path.join(tempfile.gettempdir(), "tmp.json")
            blob.download_to_filename(downloaded_file)
            with open(downloaded_file, "r") as config_json:
                self.config = json.load(config_json)
        except Exception as e:
            logging.critical(f"flattener configuration error: {e}")

    def extract_values(self, string):
        pattern = re.compile(r'projects\/(.*?)\/datasets\/(.*?)\/tables\/(events.*?)_(20\d\d\d\d\d\d)$')
        match = pattern.search(string)
        if match:
            project, dataset, table_type, shard = match.groups()
            return project, dataset, table_type, shard
        else:
            raise ValueError("String format is incorrect")

    def valid_dataset(self):
        return self.dataset in self.config.keys()

    def flatten_nested_table(self, nested_table):
        return nested_table in self.config[self.dataset]["tables_to_flatten"]

    def get_output_configuration(self):
        """
        Extract info from the config file on whether we want sharded output, partitioned output or both
        :return:
        """
        config_output = self.config[self.dataset].get("output_format", {
            "sharded": True,
            "partitioned": False})

        return config_output

class GaExportedNestedDataStorage(object):
    def __init__(self, gcp_project, dataset, table_type, date_shard):

        # main configurations
        self.gcp_project = gcp_project
        self.dataset = dataset
        self.date_shard = date_shard
        self.date = datetime.strptime(self.date_shard, '%Y%m%d')
        self.table_type = table_type
        self.source_table_type = "'intraday'" if self.source_table_is_intraday() else "'daily'"

        # The next several properties will correspond to GA4 fields

        # These fields will be used to build a compound id of a unique event
        # stream_id is added to make sure that there is definitely no id collisions, if you have multiple data streams
        self.unique_event_id_fields = [
            "stream_id",
            "user_pseudo_id",
            "event_name",
            "event_timestamp"
        ]

        self.date_field_name = "event_date"

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

        self.collected_traffic_source_fields = [
        "collected_traffic_source.manual_campaign_id",
        "collected_traffic_source.manual_campaign_name",
        "collected_traffic_source.manual_source",
        "collected_traffic_source.manual_medium",
        "collected_traffic_source.manual_term",
        "collected_traffic_source.manual_content",
        "collected_traffic_source.gclid",
        "collected_traffic_source.dclid",
        "collected_traffic_source.srsltid"
        ]

        if self.date_shard >= '20230503':
            self.events_fields.extend(self.collected_traffic_source_fields)

        if self.date_shard >= "20230717":
            self.events_fields.append("is_active_user")

        self.partitioning_column = "event_date"

    def source_table_is_intraday(self):
        return "intraday" in self.table_type

    def get_unique_event_id(self, unique_event_id_fields):
        """
        build unique event id
        """
        return f"CONCAT({unique_event_id_fields[0]}, '_', {unique_event_id_fields[1]}, '_', {unique_event_id_fields[2]}, '_', {unique_event_id_fields[3]}) as event_id"

    def get_event_params_query(self):

        qry = f"""
              SELECT 
                  PARSE_DATE('%Y%m%d', {self.date_field_name}) AS {self.date_field_name}, 
                  {self.get_unique_event_id(self.unique_event_id_fields)},
                  {self.event_params_fields[0]} as {self.event_params_fields[0].replace(".", "_")},
                  COALESCE({self.event_params_fields[1]}, 
                      CAST({self.event_params_fields[2]} AS STRING), 
                      CAST({self.event_params_fields[3]} AS STRING), 
                      CAST({self.event_params_fields[4]} AS STRING)
                  ) AS event_params_value,
                  {self.source_table_type} AS source_table_type
              FROM 
                `{self.gcp_project}.{self.dataset}.{self.table_type}_{self.date_shard}`
              ,UNNEST (event_params) AS event_params"""

        return qry

    def get_user_properties_query(self):

        qry = f"""
            SELECT
                PARSE_DATE('%Y%m%d', {self.date_field_name}) AS {self.date_field_name},
                {self.get_unique_event_id(self.unique_event_id_fields)},
                {self.user_properties_fields[0]} as {self.user_properties_fields[0].replace(".", "_")},
                COALESCE({self.user_properties_fields[1]}, 
                    CAST({self.user_properties_fields[2]} AS STRING), 
                    CAST({self.user_properties_fields[3]} AS STRING), 
                    CAST({self.user_properties_fields[4]} AS STRING)
                ) AS user_properties_value,
                {self.user_properties_fields[5]} as {self.user_properties_fields[5].replace(".", "_")},
                {self.source_table_type} AS source_table_type
            FROM 
                `{self.gcp_project}.{self.dataset}.{self.table_type}_{self.date_shard}`
            ,UNNEST (user_properties) AS user_properties"""

        return qry

    def get_items_query(self):
        qry = f"""SELECT 
        PARSE_DATE('%Y%m%d', {self.date_field_name}) AS {self.date_field_name},
        {self.get_unique_event_id(self.unique_event_id_fields)}"""
        # should we use list comprehension in 2 dynamic queries? It might be less readable and it's also harder to implement
        for field in self.items_fields:
            qry += f",{field} as {field.replace('.', '_')}"

        qry += f""" 
        ,{self.source_table_type} AS source_table_type
        FROM `{self.gcp_project}.{self.dataset}.{self.table_type}_{self.date_shard}`
        ,UNNEST (items) AS items"""

        return qry

    def get_events_query(self):
        qry = f"""SELECT
                PARSE_DATE('%Y%m%d', {self.date_field_name}) AS {self.date_field_name},
                {self.get_unique_event_id(self.unique_event_id_fields)}"""
        for field in self.events_fields:
            qry += f",{field} as {field.replace('.', '_')}"

        qry += f""" ,{self.source_table_type} AS source_table_type 
        FROM `{self.gcp_project}.{self.dataset}.{self.table_type}_{self.date_shard}`"""

        return qry

    def run_query_job(self, query, table_type, sharded_output_required=True, partitioned_output_required=False, wait_for_the_query_job_to_complete=False):

        """
        Depending on the configuration, we will write data to sharded table, partitioned table, or both.
        :param query:
        :param table_type:
        :return:
        """

        # TODO: this function is huge, split it into multiple functions???

        # 1
        # QUERY AND FLATTEN DATA. WRITE SHARDED OUTPUT, if flattener is configured to do so

        client = bigquery.Client(project=self.gcp_project)  # initialize BigQuery client

        # we will write a sharded or a partitioned table, depending on the config

        if sharded_output_required:
            # get table name
            table_name = f"{self.gcp_project}.{self.dataset}.{table_type}_{self.date_shard}"

            table_id = bigquery.Table(table_name)
            # WRITE SHARDED OUTPUT, if flattener is configured to do so
            # configure query job
            query_job_flatten_config_sharded = bigquery.QueryJobConfig(
                # we will query and flatten the data ,
                # but we may or may not write the result to a sharded table,
                # depending on the config
                destination=table_id
                , dry_run=False
                # when a destination table is specified in the job configuration, query results are not cached
                # https://cloud.google.com/bigquery/docs/cached-results
                , use_query_cache=True
                , labels={"queryfunction": "flattenerquerysharded"}
                , write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

            # run the job
            query_job_flatten_sharded = client.query(query,
                                                     job_config=query_job_flatten_config_sharded)

            # as a default option, we decided not to wait for hte job to complete, but exit the function after submitting the job, in orde to avoid a potential Cloud Function timeout
            # however, some unit tests (e.g., partitioning) are much easier to manage, if we wait for the job to complete, therefore, an option to wait is available
            if wait_for_the_query_job_to_complete:
                query_job_flatten_result_sharded = query_job_flatten_sharded.result()  # Waits for job to complete.

        if partitioned_output_required:
            # 2
            # WRITE PARTITIONED OUTPUT, if flattener is configured to do so
            try:
                # delete the partition, if it already exists, before we load it
                # this ensures that we don't have dupes
                datetime_string = str(self.date)
                date_string = re.search(r'20\d\d\-\d\d\-\d\d', datetime_string).group(0)

                query_delete = f"DELETE FROM `{self.gcp_project}.{self.dataset}.{table_type}` WHERE event_date = '{date_string}';"

                query_job_delete_config = bigquery.QueryJobConfig(
                    labels={"queryfunction": "flattenerpartitiondeletionquery"}  # todo: apply proper labels
                )
                query_job_delete = client.query(query_delete,
                                                job_config=query_job_delete_config)  # Make an API request.
                query_job_delete.result()  # Waits for job to complete.

            except Exception as e:
                if e.code == HTTPStatus.NOT_FOUND:  # 404 Not found
                    logging.warning(f"Cannot delete the partition because the table doesn't exist yet: {e}")
                else:
                    logging.critical(f"Cannot delete the partition: {e}")

            table_name_partitioned = f"{self.gcp_project}.{self.dataset}.{table_type}"

            table_id_partitioned = bigquery.Table(table_name_partitioned)

            query_job_config_partitioned = bigquery.QueryJobConfig(
                # Specify a (partial) schema. All columns are always written to the
                # table. The schema is used to assist in data type definitions.
                destination=table_id_partitioned,
                dry_run=False,
                use_query_cache=True,
                labels={"queryfunction": "flattenerpartitioncreationquery"},
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=self.partitioning_column  # field to use for partitioning
                )
            )

            # run the job
            query_job_flatten_partitioned = client.query(query,
                                                         job_config=query_job_config_partitioned)

            # as a default option, we decided not to wait for hte job to complete, but exit the function after submitting the job, in orde to avoid a potential Cloud Function timeout
            # however, some unit tests (e.g., partitioning) are much easier to manage, if we wait for the job to complete, therefore, an option to wait is available
            if wait_for_the_query_job_to_complete:
                query_job_flatten_result_partitioned = query_job_flatten_partitioned.result()  # Waits for job to complete.


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

        output_config = input_event.get_output_configuration()
        output_config_sharded = output_config["sharded"]
        output_config_partitioned = output_config["partitioned"]

        ga_source = GaExportedNestedDataStorage(gcp_project=input_event.gcp_project,
                                                dataset=input_event.dataset,
                                                table_type=input_event.table_type,
                                                date_shard=input_event.table_date_shard)

        # EVENT_PARAMS
        if input_event.flatten_nested_table(nested_table=os.environ["EVENT_PARAMS"]):
            ga_source.run_query_job(query=ga_source.get_event_params_query(), table_type="flat_event_params",
                                    sharded_output_required=output_config_sharded,
                                    partitioned_output_required=output_config_partitioned)
            logging.info(f"Ran {os.environ['EVENT_PARAMS']} flattening query for {input_event.dataset} for {input_event.table_date_shard}")
        else:
            logging.info(
                f"{os.environ['EVENT_PARAMS']} flattening query for {input_event.dataset} not configured to run")

        # USER_PROPERTIES
        if input_event.flatten_nested_table(nested_table=os.environ["USER_PROPERTIES"]):
            ga_source.run_query_job(query=ga_source.get_user_properties_query(), table_type="flat_user_properties",
                                    sharded_output_required=output_config_sharded,
                                    partitioned_output_required=output_config_partitioned)
            logging.info(f"Ran {os.environ['USER_PROPERTIES']} flattening query for {input_event.dataset} for {input_event.table_date_shard}")
        else:
            logging.info(
                f"{os.environ['USER_PROPERTIES']} flattening query for {input_event.dataset} not configured to run")

        # ITEMS
        if input_event.flatten_nested_table(nested_table=os.environ["ITEMS"]):
            ga_source.run_query_job(query=ga_source.get_items_query(), table_type="flat_items",
                                    sharded_output_required=output_config_sharded,
                                    partitioned_output_required=output_config_partitioned)
            logging.info(f"Ran {os.environ['ITEMS']} flattening query for {input_event.dataset} for {input_event.table_date_shard}")
        else:
            logging.info(f"{os.environ['ITEMS']} flattening query for {input_event.dataset} not configured to run")

        # EVENTS
        if input_event.flatten_nested_table(nested_table=os.environ["EVENTS"]):
            ga_source.run_query_job(query=ga_source.get_events_query(), table_type="flat_events",
                                    sharded_output_required=output_config_sharded,
                                    partitioned_output_required=output_config_partitioned)
            logging.info(f"Ran {os.environ['EVENTS']} flattening query for {input_event.dataset} for {input_event.table_date_shard}")
        else:
            logging.info(f"{os.environ['EVENTS']} flattening query for {input_event.dataset} not configured to run")

    else:
        logging.warning(f"Dataset {input_event.dataset} not configured for flattening")
