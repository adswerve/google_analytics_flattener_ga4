#TODO: flatten
#TODO: deploy
#TODO: (optional for now) - intraday
#TODO: (optional for now) - remaining unit tests other than the most important ones
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
import sys#TODO: flatten
#TODO: deploy
#TODO: (optional for now) - intraday
#TODO: (optional for now) - remaining unit tests other than the most important ones
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

        self.date_field_name = "event_date"

        self.partitioning_column = "event_date"

    def source_table_is_intraday(self):
        return "intraday" in self.table_type

    def get_temp_table_query(self):
        """
        build unique event id
        """
        qry = f"""
                  CREATE OR REPLACE TEMP TABLE temp_events AS (
                  SELECT
                    CONCAT(stream_id, ".", COALESCE(user_pseudo_id, "user_pseudo_id_null"),".",event_name, ".", event_timestamp, ".", ROW_NUMBER() OVER(PARTITION BY stream_id, COALESCE(user_pseudo_id, "user_pseudo_id_null"),
                        event_name,
                        event_timestamp)) AS event_id,
                    *
                  FROM
                    `{self.gcp_project}.{self.dataset}.{self.table_type}_{self.date_shard}`
                )
                ;
              """
        return qry

    def get_events_query_select_statement(self):
        qry = f"""SELECT
                    PARSE_DATE('%Y%m%d', event_date) AS event_date,
                    event_id,
                
                    event_timestamp AS event_timestamp,
                    event_name AS event_name,
                    event_previous_timestamp AS event_previous_timestamp,
                    event_value_in_usd AS event_value_in_usd,
                    event_bundle_sequence_id AS event_bundle_sequence_id,
                    event_server_timestamp_offset AS event_server_timestamp_offset,
                    user_id AS user_id,
                    user_pseudo_id AS user_pseudo_id,
                
                    privacy_info.analytics_storage AS privacy_info_analytics_storage,
                    privacy_info.ads_storage AS privacy_info_ads_storage,
                    privacy_info.uses_transient_token AS privacy_info_uses_transient_token,
                    user_first_touch_timestamp AS user_first_touch_timestamp,
                
                    user_ltv.revenue AS user_ltv_revenue,
                    user_ltv.currency AS user_ltv_currency,
                
                    device.category AS device_category,
                    device.mobile_brand_name AS device_mobile_brand_name,
                    device.mobile_model_name AS device_mobile_model_name,
                    device.mobile_marketing_name AS device_mobile_marketing_name,
                    device.mobile_os_hardware_model AS device_mobile_os_hardware_model,
                    device.operating_system AS device_operating_system,
                    device.operating_system_version AS device_operating_system_version,
                    device.vendor_id AS device_vendor_id,
                    device.advertising_id AS device_advertising_id,
                    device.language AS device_language,
                    device.is_limited_ad_tracking AS device_is_limited_ad_tracking,
                    device.time_zone_offset_seconds AS device_time_zone_offset_seconds,
                    device.browser AS device_browser,
                    device.browser_version AS device_browser_version,
                
                    device.web_info.browser AS device_web_info_browser,
                    device.web_info.browser_version AS device_web_info_browser_version,
                    device.web_info.hostname AS device_web_info_hostname,
                
                    geo.continent AS geo_continent,
                    geo.country AS geo_country,
                    geo.region AS geo_region,
                    geo.city AS geo_city,
                    geo.sub_continent AS geo_sub_continent,
                    geo.metro AS geo_metro,
                
                    app_info.id AS app_info_id,
                    app_info.version AS app_info_version,
                    app_info.install_store AS app_info_install_store,
                    app_info.firebase_app_id AS app_info_firebase_app_id,
                    app_info.install_source AS app_info_install_source,
                
                    traffic_source.name AS traffic_source_name,
                    traffic_source.medium AS traffic_source_medium,
                    traffic_source.source AS traffic_source_source,
                    stream_id AS stream_id,
                    platform AS platform,
                
                    event_dimensions.hostname AS event_dimensions_hostname,
                
                    ecommerce.total_item_quantity AS ecommerce_total_item_quantity,
                    ecommerce.purchase_revenue_in_usd AS ecommerce_purchase_revenue_in_usd,
                    ecommerce.purchase_revenue AS ecommerce_purchase_revenue,
                    ecommerce.refund_value_in_usd AS ecommerce_refund_value_in_usd,
                    ecommerce.refund_value AS ecommerce_refund_value,
                    ecommerce.shipping_value_in_usd AS ecommerce_shipping_value_in_usd,
                    ecommerce.shipping_value AS ecommerce_shipping_value,
                    ecommerce.tax_value_in_usd AS ecommerce_tax_value_in_usd,
                    ecommerce.tax_value AS ecommerce_tax_value,
                    ecommerce.unique_items AS ecommerce_unique_items,
                    ecommerce.transaction_id AS ecommerce_transaction_id
                """

        if self.date_shard >= '20230503':
            qry += f"""
                    ,collected_traffic_source.manual_campaign_id AS collected_traffic_source_manual_campaign_id
                    ,collected_traffic_source.manual_campaign_name AS collected_traffic_source_manual_campaign_name
                    ,collected_traffic_source.manual_source AS collected_traffic_source_manual_source
                    ,collected_traffic_source.manual_medium AS collected_traffic_source_manual_medium
                    ,collected_traffic_source.manual_term AS collected_traffic_source_manual_term
                    ,collected_traffic_source.manual_content AS collected_traffic_source_manual_content
                    ,collected_traffic_source.gclid AS collected_traffic_source_gclid
                    ,collected_traffic_source.dclid AS collected_traffic_source_dclid
                    ,collected_traffic_source.srsltid AS collected_traffic_source_srsltid
                    """

        if self.date_shard >= "20230717":
            qry += f",is_active_user AS is_active_user"

        qry += f""" ,{self.source_table_type} AS source_table_type
        FROM temp_events
        ;"""

        return qry

    def get_event_params_query_select_statement(self, sharded_output_required=True, partitioned_output_required=False):

        qry = f"""
              SELECT 
                    PARSE_DATE('%Y%m%d', {self.date_field_name}) AS {self.date_field_name}, 
                    event_id,
                    event_params.key as event_params_key,
                    COALESCE(event_params.value.string_value,
                        CAST(event_params.value.int_value AS STRING),
                        CAST(event_params.value.float_value AS STRING),
                        CAST(event_params.value.double_value AS STRING)
                        ) AS event_params_value,
                    {self.source_table_type} AS source_table_type
              FROM 
                temp_events
              ,UNNEST (event_params) AS event_params
              ;"""

        return qry

    def get_user_properties_query_select_statement(self, sharded_output_required=True, partitioned_output_required=False):

        qry = f"""
            SELECT
                PARSE_DATE('%Y%m%d', event_date) AS event_date,
                event_id,
                user_properties.key	AS user_properties_key,
                COALESCE(user_properties.value.string_value,
                    CAST(user_properties.value.int_value AS STRING),
                    CAST(user_properties.value.float_value AS STRING),
                    CAST(user_properties.value.double_value AS STRING)
                    ) AS user_properties_value,
                user_properties.value.set_timestamp_micros AS user_properties_value_set_timestamp_micros,
                {self.source_table_type} AS source_table_type
            FROM 
                temp_events
            ,UNNEST (user_properties) AS user_properties;"""

        return qry

    def get_items_query_select_statement(self, sharded_output_required=True, partitioned_output_required=False):

        qry = f""" SELECT

                        PARSE_DATE('%Y%m%d', event_date) AS event_date,
                        event_id,
                        
                        items.item_id AS items_item_id,
                        items.item_name AS items_item_name,
                        items.item_brand AS items_item_brand,
                        items.item_variant AS items_item_variant,
                        items.item_category AS items_item_category,
                        items.item_category2 AS items_item_category2,
                        items.item_category3 AS items_item_category3,
                        items.item_category4 AS items_item_category4,
                        items.item_category5 AS items_item_category5,
                        items.price_in_usd AS items_price_in_usd,
                        items.price AS items_price,
                        items.quantity AS items_quantity,
                        items.item_revenue_in_usd AS items_item_revenue_in_usd,
                        items.item_revenue AS items_item_revenue,
                        items.item_refund_in_usd AS items_item_refund_in_usd,
                        items.item_refund AS items_item_refund,
                        items.coupon AS items_coupon,
                        items.affiliation AS items_affiliation,
                        items.location_id AS items_location_id,
                        items.item_list_id AS items_item_list_id,
                        items.item_list_name AS items_item_list_name,
                        items.item_list_index AS items_item_list_index,
                        items.promotion_id AS items_promotion_id,
                        items.promotion_name AS items_promotion_name,
                        items.creative_name AS items_creative_name,
                        items.creative_slot AS items_creative_slot,
                        {self.source_table_type} AS source_table_type
                        
                FROM temp_events
                ,UNNEST (items) AS items;"""

        return qry

    def get_flat_table_update_query(self, select_statement, flat_table, sharded_output_required=True, partitioned_output_required=False):

        assert flat_table in ["flat_events", "flat_event_params", "flat_user_properties", "flat_items"]
        assert "flat" in flat_table

        query = ""

        if sharded_output_required:
            # get table name
            dest_table_name = f"{self.gcp_project}.{self.dataset}.{flat_table}_{self.date_shard}"

            query += f"""CREATE OR REPLACE TABLE `{dest_table_name}`
                    AS
                    """
            query += select_statement

        if partitioned_output_required:
            dest_table_name = f"{self.gcp_project}.{self.dataset}.{flat_table}"

            query += f"""DELETE FROM `{dest_table_name}` WHERE event_date = '{self.date_shard}';
                        INSERT INTO TABLE `{dest_table_name}`
                        AS """
            query += select_statement


        return query

    def get_select_statement(self, flat_table):

        assert flat_table in ["flat_events", "flat_event_params", "flat_user_properties", "flat_items"]

        query = ""

        if flat_table == "flat_events":
            query += self.get_events_query_select_statement()

        elif flat_table == "flat_event_params":
            query += self.get_event_params_query_select_statement()

        elif flat_table == "flat_user_properties":
            query += self.get_user_properties_query_select_statement()

        elif flat_table == "flat_items":
            query += self.get_items_query_select_statement()

        return query

    def build_full_query(self, sharded_output_required=True, partitioned_output_required=False,
                         list_of_flat_tables=["flat_events", "flat_event_params", "flat_user_properties",
                                              "flat_items"]):

        assert len(list_of_flat_tables) > 1, "At least 1 flat table needs to be included in the config file"
        query = ""
        query += self.get_temp_table_query()

        for flat_table in list_of_flat_tables:
            query_select = self.get_select_statement(flat_table)
            query_write_to_dest_table = self.get_flat_table_update_query(select_statement=query_select, flat_table=flat_table,
                                                                         sharded_output_required=sharded_output_required,
                                                                         partitioned_output_required=partitioned_output_required)

            query += query_write_to_dest_table
            query += "\n"

        return query


    def run_query_job(self, query, table_type, sharded_output_required=True, partitioned_output_required=False, wait_for_the_query_job_to_complete=True):

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

        self.date_field_name = "event_date"

        self.partitioning_column = "event_date"

    def source_table_is_intraday(self):
        return "intraday" in self.table_type

    def get_temp_table_query(self):
        """
        build unique event id
        """
        qry = f"""
                  CREATE OR REPLACE TEMP TABLE temp_events AS (
                  SELECT
                    CONCAT(stream_id, ".", COALESCE(user_pseudo_id, "user_pseudo_id_null"),".",event_name, ".", event_timestamp, ".", ROW_NUMBER() OVER(PARTITION BY stream_id, COALESCE(user_pseudo_id, "user_pseudo_id_null"),
                        event_name,
                        event_timestamp)) AS event_id,
                    *
                  FROM
                    `{self.gcp_project}.{self.dataset}.{self.table_type}_{self.date_shard}`
                )
                ;
              """
        return qry

    def get_events_query_select_statement(self):
        qry = f"""SELECT
                    PARSE_DATE('%Y%m%d', event_date) AS event_date,
                    event_id,
                
                    event_timestamp AS event_timestamp,
                    event_name AS event_name,
                    event_previous_timestamp AS event_previous_timestamp,
                    event_value_in_usd AS event_value_in_usd,
                    event_bundle_sequence_id AS event_bundle_sequence_id,
                    event_server_timestamp_offset AS event_server_timestamp_offset,
                    user_id AS user_id,
                    user_pseudo_id AS user_pseudo_id,
                
                    privacy_info.analytics_storage AS privacy_info_analytics_storage,
                    privacy_info.ads_storage AS privacy_info_ads_storage,
                    privacy_info.uses_transient_token AS privacy_info_uses_transient_token,
                    user_first_touch_timestamp AS user_first_touch_timestamp,
                
                    user_ltv.revenue AS user_ltv_revenue,
                    user_ltv.currency AS user_ltv_currency,
                
                    device.category AS device_category,
                    device.mobile_brand_name AS device_mobile_brand_name,
                    device.mobile_model_name AS device_mobile_model_name,
                    device.mobile_marketing_name AS device_mobile_marketing_name,
                    device.mobile_os_hardware_model AS device_mobile_os_hardware_model,
                    device.operating_system AS device_operating_system,
                    device.operating_system_version AS device_operating_system_version,
                    device.vendor_id AS device_vendor_id,
                    device.advertising_id AS device_advertising_id,
                    device.language AS device_language,
                    device.is_limited_ad_tracking AS device_is_limited_ad_tracking,
                    device.time_zone_offset_seconds AS device_time_zone_offset_seconds,
                    device.browser AS device_browser,
                    device.browser_version AS device_browser_version,
                
                    device.web_info.browser AS device_web_info_browser,
                    device.web_info.browser_version AS device_web_info_browser_version,
                    device.web_info.hostname AS device_web_info_hostname,
                
                    geo.continent AS geo_continent,
                    geo.country AS geo_country,
                    geo.region AS geo_region,
                    geo.city AS geo_city,
                    geo.sub_continent AS geo_sub_continent,
                    geo.metro AS geo_metro,
                
                    app_info.id AS app_info_id,
                    app_info.version AS app_info_version,
                    app_info.install_store AS app_info_install_store,
                    app_info.firebase_app_id AS app_info_firebase_app_id,
                    app_info.install_source AS app_info_install_source,
                
                    traffic_source.name AS traffic_source_name,
                    traffic_source.medium AS traffic_source_medium,
                    traffic_source.source AS traffic_source_source,
                    stream_id AS stream_id,
                    platform AS platform,
                
                    event_dimensions.hostname AS event_dimensions_hostname,
                
                    ecommerce.total_item_quantity AS ecommerce_total_item_quantity,
                    ecommerce.purchase_revenue_in_usd AS ecommerce_purchase_revenue_in_usd,
                    ecommerce.purchase_revenue AS ecommerce_purchase_revenue,
                    ecommerce.refund_value_in_usd AS ecommerce_refund_value_in_usd,
                    ecommerce.refund_value AS ecommerce_refund_value,
                    ecommerce.shipping_value_in_usd AS ecommerce_shipping_value_in_usd,
                    ecommerce.shipping_value AS ecommerce_shipping_value,
                    ecommerce.tax_value_in_usd AS ecommerce_tax_value_in_usd,
                    ecommerce.tax_value AS ecommerce_tax_value,
                    ecommerce.unique_items AS ecommerce_unique_items,
                    ecommerce.transaction_id AS ecommerce_transaction_id
                """

        if self.date_shard >= '20230503':
            qry += f"""
                    ,collected_traffic_source.manual_campaign_id AS collected_traffic_source_manual_campaign_id
                    ,collected_traffic_source.manual_campaign_name AS collected_traffic_source_manual_campaign_name
                    ,collected_traffic_source.manual_source AS collected_traffic_source_manual_source
                    ,collected_traffic_source.manual_medium AS collected_traffic_source_manual_medium
                    ,collected_traffic_source.manual_term AS collected_traffic_source_manual_term
                    ,collected_traffic_source.manual_content AS collected_traffic_source_manual_content
                    ,collected_traffic_source.gclid AS collected_traffic_source_gclid
                    ,collected_traffic_source.dclid AS collected_traffic_source_dclid
                    ,collected_traffic_source.srsltid AS collected_traffic_source_srsltid
                    """

        if self.date_shard >= "20230717":
            qry += f",is_active_user AS is_active_user"

        qry += f""" ,{self.source_table_type} AS source_table_type
        FROM temp_events
        ;"""

        return qry

    def get_event_params_query_select_statement(self, sharded_output_required=True, partitioned_output_required=False):

        qry = f"""
              SELECT 
                    PARSE_DATE('%Y%m%d', {self.date_field_name}) AS {self.date_field_name}, 
                    event_id,
                    event_params.key as event_params_key,
                    COALESCE(event_params.value.string_value,
                        CAST(event_params.value.int_value AS STRING),
                        CAST(event_params.value.float_value AS STRING),
                        CAST(event_params.value.double_value AS STRING)
                        ) AS event_params_value,
                    {self.source_table_type} AS source_table_type
              FROM 
                temp_events
              ,UNNEST (event_params) AS event_params
              ;"""

        return qry

    def get_user_properties_query_select_statement(self, sharded_output_required=True, partitioned_output_required=False):

        qry = f"""
            SELECT
                PARSE_DATE('%Y%m%d', event_date) AS event_date,
                event_id,
                user_properties.key	AS user_properties_key,
                COALESCE(user_properties.value.string_value,
                    CAST(user_properties.value.int_value AS STRING),
                    CAST(user_properties.value.float_value AS STRING),
                    CAST(user_properties.value.double_value AS STRING)
                    ) AS user_properties_value,
                user_properties.value.set_timestamp_micros AS user_properties_value_set_timestamp_micros,
                {self.source_table_type} AS source_table_type
            FROM 
                temp_events
            ,UNNEST (user_properties) AS user_properties;"""

        return qry

    def get_items_query_select_statement(self, sharded_output_required=True, partitioned_output_required=False):

        qry = f""" SELECT

                        PARSE_DATE('%Y%m%d', event_date) AS event_date,
                        event_id,
                        
                        items.item_id AS items_item_id,
                        items.item_name AS items_item_name,
                        items.item_brand AS items_item_brand,
                        items.item_variant AS items_item_variant,
                        items.item_category AS items_item_category,
                        items.item_category2 AS items_item_category2,
                        items.item_category3 AS items_item_category3,
                        items.item_category4 AS items_item_category4,
                        items.item_category5 AS items_item_category5,
                        items.price_in_usd AS items_price_in_usd,
                        items.price AS items_price,
                        items.quantity AS items_quantity,
                        items.item_revenue_in_usd AS items_item_revenue_in_usd,
                        items.item_revenue AS items_item_revenue,
                        items.item_refund_in_usd AS items_item_refund_in_usd,
                        items.item_refund AS items_item_refund,
                        items.coupon AS items_coupon,
                        items.affiliation AS items_affiliation,
                        items.location_id AS items_location_id,
                        items.item_list_id AS items_item_list_id,
                        items.item_list_name AS items_item_list_name,
                        items.item_list_index AS items_item_list_index,
                        items.promotion_id AS items_promotion_id,
                        items.promotion_name AS items_promotion_name,
                        items.creative_name AS items_creative_name,
                        items.creative_slot AS items_creative_slot,
                        {self.source_table_type} AS source_table_type
                        
                FROM temp_events
                ,UNNEST (items) AS items;"""

        return qry

    def get_flat_table_update_query(self, select_statement, flat_table, sharded_output_required=True, partitioned_output_required=False):

        assert flat_table in ["flat_events", "flat_event_params", "flat_user_properties", "flat_items"]
        assert "flat" in flat_table

        query = ""

        if sharded_output_required:
            # get table name
            dest_table_name = f"{self.gcp_project}.{self.dataset}.{flat_table}_{self.date_shard}"

            query += f"""CREATE OR REPLACE TABLE `{dest_table_name}`
                    AS
                    """
            query += select_statement

        if partitioned_output_required:
            dest_table_name = f"{self.gcp_project}.{self.dataset}.{flat_table}"

            query += f"""DELETE FROM `{dest_table_name}` WHERE event_date = '{self.date_shard}';
                        INSERT INTO TABLE `{dest_table_name}`
                        AS """
            query += select_statement


        return query

    def get_select_statement(self, flat_table):

        assert flat_table in ["flat_events", "flat_event_params", "flat_user_properties", "flat_items"]

        query = ""

        if flat_table == "flat_events":
            query += self.get_events_query_select_statement()

        elif flat_table == "flat_event_params":
            query += self.get_event_params_query_select_statement()

        elif flat_table == "flat_user_properties":
            query += self.get_user_properties_query_select_statement()

        elif flat_table == "flat_items":
            query += self.get_items_query_select_statement()

        return query

    def build_full_query(self, sharded_output_required=True, partitioned_output_required=False,
                         list_of_flat_tables=["flat_events", "flat_event_params", "flat_user_properties",
                                              "flat_items"]):

        assert len(list_of_flat_tables) > 1, "At least 1 flat table needs to be included in the config file"
        query = ""
        query += self.get_temp_table_query()

        for flat_table in list_of_flat_tables:
            query_select = self.get_select_statement(flat_table)
            query_write_to_dest_table = self.get_flat_table_update_query(select_statement=query_select, flat_table=flat_table,
                                                                         sharded_output_required=sharded_output_required,
                                                                         partitioned_output_required=partitioned_output_required)

            query += query_write_to_dest_table
            query += "\n"

        return query


    def run_query_job(self, query, wait_for_the_query_job_to_complete=True):
        """
        Submits a BQ SQL query and optionally waits for the query to complete.

        :param query: SQL query string to be executed.
        :param wait_for_the_query_job_to_complete: If True, wait for the query to complete before returning.
        :return: The query job result if wait_for_the_query_job_to_complete is True, else the query job itself.
        """

        client = bigquery.Client(project=self.gcp_project)  # initialize BigQuery client

        try:
            # Configure the query job
            job_config = bigquery.QueryJobConfig(
                use_query_cache=True,  # Use cached results if available
                labels={"queryfunction": "ga4flattener"}  # Add labels for the query job
            )

            # Run the query
            query_job = client.query(query, job_config=job_config)
            logging.info(f"Query submitted: {query}")

            if wait_for_the_query_job_to_complete:
                # Wait for the query to complete
                query_job.result()
                logging.info("Query completed.")
                return query_job
            else:
                logging.info("Query submitted, not waiting for completion.")
                return query_job

        except Exception as e:
            logging.error(f"An error occurred while submitting the query: {e}")
            raise


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
