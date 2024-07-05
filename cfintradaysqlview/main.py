import base64
import json
from google.cloud import storage
from google.cloud import bigquery
import re
import os
import tempfile
import logging
from datetime import datetime
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

class InputValidatorIntraday(object):
    def __init__(self, event):
        try:
            message_payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
            self.method_name = message_payload['protoPayload']["methodName"]

            if self.method_name == "tableservice.insert":
                bq_destination_table = \
                    message_payload['protoPayload']['serviceData']["tableInsertResponse"]["resource"]["tableName"]
                self.gcp_project = bq_destination_table['projectId']
                self.dataset = bq_destination_table['datasetId']
                self.table_date_shard = re.search(r'_(20\d\d\d\d\d\d)$', bq_destination_table['tableId']).group(1)
                self.table_name = re.search(r'(events_intraday)_20\d\d\d\d\d\d$',
                                            bq_destination_table['tableId']).group(
                    1)

            elif self.method_name == "tableservice.delete":
                # we assume that there's only 1 element in the array. It is true, because for each deleted intraday table there will be a separate log
                # nevertheless, let's check the assumption on an off-chance that there are many elements in the array
                assert len(message_payload['protoPayload']["authorizationInfo"])==1, f"more than 1 element in the log array message_payload['protoPayload']['authorizationInfo']: {message_payload['protoPayload']['authorizationInfo']}"
                bq_destination_table = message_payload['protoPayload']["authorizationInfo"][0]["resource"]
                #  https://www.kite.com/python/answers/how-to-get-the-substring-between-two-markers-in-python

                self.gcp_project = re.search(
                    r'^projects\/(.*?)\/datasets\/analytics_\d\d\d\d\d\d\d\d\d\/tables\/events_intraday_20\d\d\d\d\d\d$',
                    bq_destination_table).group(1)

                self.dataset = re.search(r'(analytics_\d\d\d\d\d\d\d\d\d)', bq_destination_table).group(1)
                self.table_date_shard = re.search(r'_(20\d\d\d\d\d\d)$', bq_destination_table).group(1)
                self.table_name = re.search(r'(events_intraday)_20\d\d\d\d\d\d$', bq_destination_table).group(1)

        except AttributeError or KeyError as e:
            logging.critical(f"invalid message: {message_payload}")
            logging.critical(e)
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(os.environ["CONFIG_BUCKET_NAME"])
            blob = bucket.blob(os.environ["CONFIG_FILENAME"])
            downloaded_file = os.path.join(tempfile.gettempdir(), "tmp.json")
            blob.download_to_filename(downloaded_file)
            with open(downloaded_file, "r") as config_json:
                self.config = json.load(config_json)
        except Exception as e:
            logging.critical(f"flattener configuration error: {e}")

    def valid_dataset(self):
        """is the BQ dataset (representing GA4 property) configured for flattening?"""
        valid = self.dataset in self.config.keys()
        return valid

    def flatten_nested_table(self, nested_table):
        return nested_table in self.config[self.dataset]["tables_to_flatten"]

    def intraday_sql_view_configured(self):
        return self.config[self.dataset]["intraday_flattening"]["intraday_flat_views"] == True

class IntradaySQLView(object):
    def __init__(self, gcp_project, dataset, table_name, date_shard):

        # main configurations
        self.gcp_project = gcp_project
        self.dataset = dataset
        self.date_shard = date_shard
        self.date = datetime.strptime(self.date_shard, '%Y%m%d')
        self.table_name = table_name
        self.source_table_type = "'intraday'" if self.source_table_is_intraday() else "'daily'"
        self.date_field_name = "event_date"


    def source_table_is_intraday(self):
        return "intraday" in self.table_name

    def get_unique_pk_query(self):
        """
        build unique event id
        """
        qry = f"""
                  SELECT
                    CONCAT(stream_id, ".", COALESCE(user_pseudo_id, "user_pseudo_id_null"),".",event_name, ".", event_timestamp, ".", ROW_NUMBER() OVER(PARTITION BY stream_id, COALESCE(user_pseudo_id, "user_pseudo_id_null"),
                        event_name,
                        event_timestamp)) AS event_id,
                    *
                  FROM
                    `{self.gcp_project}.{self.dataset}.{self.table_name}_{self.date_shard}`
                ;
              """
        return qry

    def create_intraday_sql_view_unique_pk(self, wait_for_the_query_job_to_complete=True):


        client = bigquery.Client(project=self.gcp_project)  # initialize BigQuery client

        query = self.get_unique_pk_query()

        query_create_view = f"""
                    CREATE OR REPLACE VIEW `{self.gcp_project}.{self.dataset}.view_events_{self.date_shard}_unique_pk`
                    AS 
                    {query}
                """

        # run the job
        query_job = client.query(query_create_view)

        if wait_for_the_query_job_to_complete:
            query_job_result = query_job.result()  # Waits for job to complete.

    def drop_intraday_sql_view_unique_pk(self, wait_for_the_query_job_to_complete=True):


        client = bigquery.Client(project=self.gcp_project)  # initialize BigQuery client


        query_create_view = f"""
                    DROP VIEW IF EXISTS `{self.gcp_project}.{self.dataset}.view_events_{self.date_shard}_unique_pk`
                    ;
                """

        # run the job
        query_job = client.query(query_create_view)

        if wait_for_the_query_job_to_complete:
            query_job_result = query_job.result()  # Waits for job to complete.

    def get_events_query_select_statement(self):
        qry = f"""SELECT
                    PARSE_DATE('%Y%m%d', event_date) AS event_date,
                    event_id,
                    CONCAT(user_pseudo_id, ".",(SELECT value.int_value from UNNEST(event_params) WHERE key = 'ga_session_id')) as session_id,
                    CONCAT(user_pseudo_id, ".",(SELECT value.int_value from UNNEST(event_params) WHERE key = 'ga_session_number')) as session_id_1,

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
        FROM `{self.gcp_project}.{self.dataset}.view_events_{self.date_shard}_unique_pk`
        ;"""

        return qry

    def get_event_params_query_select_statement(self):

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
                `{self.gcp_project}.{self.dataset}.view_events_{self.date_shard}_unique_pk`
              ,UNNEST (event_params) AS event_params
              ;"""

        return qry

    def get_user_properties_query_select_statement(self):

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
                `{self.gcp_project}.{self.dataset}.view_events_{self.date_shard}_unique_pk`
            ,UNNEST (user_properties) AS user_properties;"""

        return qry

    def get_items_query_select_statement(self):

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

                FROM `{self.gcp_project}.{self.dataset}.view_events_{self.date_shard}_unique_pk`
                ,UNNEST (items) AS items;"""

        return qry





    def create_intraday_sql_views(self, query, table_type, wait_for_the_query_job_to_complete=False):

        client = bigquery.Client(project=self.gcp_project)  # initialize BigQuery client

        self.create_intraday_sql_view_unique_pk()

        query_create_view = f"""
                    CREATE OR REPLACE VIEW `{self.gcp_project}.{self.dataset}.view_{table_type}_{self.date_shard}`
                    AS 
                    {query}
                """

        # run the job
        query_job = client.query(query_create_view)

        if wait_for_the_query_job_to_complete:
            query_job_result = query_job.result()  # Waits for job to complete.

    def delete_intraday_sql_views(self, table_type, wait_for_the_query_job_to_complete=False):

        client = bigquery.Client(project=self.gcp_project)  # initialize BigQuery client

        self.drop_intraday_sql_view_unique_pk()

        query_delete_view = f"""
                    DROP VIEW IF EXISTS `{self.gcp_project}.{self.dataset}.view_{table_type}_{self.date_shard}`
                    """
        # run the job
        query_job = client.query(query_delete_view)

        if wait_for_the_query_job_to_complete:
            query_job = query_job.result()  # Waits for job to complete.

def manage_intraday_sql_view(event, context="context"):
    input_event = InputValidatorIntraday(event)

    ga_source = IntradaySQLView(gcp_project=input_event.gcp_project,
                                            dataset=input_event.dataset,
                                            table_name=input_event.table_name,
                                            date_shard=input_event.table_date_shard)

    if input_event.valid_dataset():
        # did a new intraday table get created?
        if input_event.method_name == "tableservice.insert":

            if input_event.intraday_sql_view_configured():

                # EVENT_PARAMS
                if input_event.flatten_nested_table(nested_table=os.environ["EVENT_PARAMS"]):
                    ga_source.create_intraday_sql_views(query=ga_source.get_event_params_query_select_statement(), table_type="flat_event_params", wait_for_the_query_job_to_complete=True)

                    logging.info(
                        f"Created an {os.environ['EVENT_PARAMS']} intraday SQL view for {input_event.dataset} for {input_event.table_date_shard}")
                else:
                    logging.info(
                        f"{os.environ['EVENT_PARAMS']} flattening query for {input_event.dataset} not configured to run")

                # EVENTS
                if input_event.flatten_nested_table(nested_table=os.environ["EVENTS"]):
                    ga_source.create_intraday_sql_views(query=ga_source.get_events_query_select_statement(), table_type="flat_events",
                                                        wait_for_the_query_job_to_complete=True)

                    logging.info(
                        f"Created an {os.environ['EVENTS']} intraday SQL view for {input_event.dataset} for {input_event.table_date_shard}")
                else:
                    logging.info(
                        f"{os.environ['EVENTS']} flattening query for {input_event.dataset} not configured to run")

                # ITEMS
                if input_event.flatten_nested_table(nested_table=os.environ["ITEMS"]):
                    ga_source.create_intraday_sql_views(query=ga_source.get_items_query_select_statement(), table_type="flat_items",
                                                        wait_for_the_query_job_to_complete=True)

                    logging.info(
                        f"Created an {os.environ['ITEMS']} intraday SQL view for {input_event.dataset} for {input_event.table_date_shard}")
                else:
                    logging.info(
                        f"{os.environ['ITEMS']} flattening query for {input_event.dataset} not configured to run")

                # USER_PROPERTIES
                if input_event.flatten_nested_table(nested_table=os.environ["USER_PROPERTIES"]):
                    ga_source.create_intraday_sql_views(query=ga_source.get_user_properties_query_select_statement(),
                                                        table_type="flat_user_properties",
                                                        wait_for_the_query_job_to_complete=True)

                    logging.info(
                        f"Created an {os.environ['USER_PROPERTIES']} intraday SQL view for {input_event.dataset} for {input_event.table_date_shard}")
                else:
                    logging.info(
                        f"{os.environ['USER_PROPERTIES']} flattening query for {input_event.dataset} not configured to run")

            else:
                logging.info(f"Intraday SQL view for {input_event.dataset} is not configured")

        # did an intraday table get deleted?
        elif input_event.method_name == "tableservice.delete":
            ga_source.delete_intraday_sql_views(table_type="flat_event_params")
            logging.info(
                f"Deleted an {os.environ['EVENT_PARAMS']} intraday SQL view for {input_event.dataset} for {input_event.table_date_shard}")

            ga_source.delete_intraday_sql_views(table_type="flat_events")
            logging.info(
                f"Deleted an {os.environ['EVENTS']} intraday SQL view for {input_event.dataset} for {input_event.table_date_shard}")

            ga_source.delete_intraday_sql_views(table_type="flat_items")
            logging.info(
                f"Deleted an {os.environ['ITEMS']} intraday SQL view for {input_event.dataset} for {input_event.table_date_shard}")

            ga_source.delete_intraday_sql_views(table_type="flat_user_properties")
            logging.info(
                f"Deleted an {os.environ['USER_PROPERTIES']} intraday SQL view for {input_event.dataset} for {input_event.table_date_shard}")

    else:
        logging.warning(f"Dataset {input_event.dataset} is not configured for flattening")