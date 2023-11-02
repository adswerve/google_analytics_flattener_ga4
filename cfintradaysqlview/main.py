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
        return "intraday" in self.table_name

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
                `{self.gcp_project}.{self.dataset}.{self.table_name}_{self.date_shard}`
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
                `{self.gcp_project}.{self.dataset}.{self.table_name}_{self.date_shard}`
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
        FROM `{self.gcp_project}.{self.dataset}.{self.table_name}_{self.date_shard}`
        ,UNNEST (items) AS items"""

        return qry

    def get_events_query(self):
        qry = f"""SELECT
                PARSE_DATE('%Y%m%d', {self.date_field_name}) AS {self.date_field_name},
                {self.get_unique_event_id(self.unique_event_id_fields)}"""
        for field in self.events_fields:
            qry += f",{field} as {field.replace('.', '_')}"

        qry += f""" ,{self.source_table_type} AS source_table_type 
        FROM `{self.gcp_project}.{self.dataset}.{self.table_name}_{self.date_shard}`"""

        return qry

    def create_intraday_sql_views(self, query, table_type, wait_for_the_query_job_to_complete=False):

        client = bigquery.Client(project=self.gcp_project)  # initialize BigQuery client

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
                    ga_source.create_intraday_sql_views(query=ga_source.get_event_params_query(), table_type="flat_event_params", wait_for_the_query_job_to_complete=True)

                    logging.info(
                        f"Created an {os.environ['EVENT_PARAMS']} intraday SQL view for {input_event.dataset} for {input_event.table_date_shard}")
                else:
                    logging.info(
                        f"{os.environ['EVENT_PARAMS']} flattening query for {input_event.dataset} not configured to run")

                # EVENTS
                if input_event.flatten_nested_table(nested_table=os.environ["EVENTS"]):
                    ga_source.create_intraday_sql_views(query=ga_source.get_events_query(), table_type="flat_events",
                                                        wait_for_the_query_job_to_complete=True)

                    logging.info(
                        f"Created an {os.environ['EVENTS']} intraday SQL view for {input_event.dataset} for {input_event.table_date_shard}")
                else:
                    logging.info(
                        f"{os.environ['EVENTS']} flattening query for {input_event.dataset} not configured to run")

                # ITEMS
                if input_event.flatten_nested_table(nested_table=os.environ["ITEMS"]):
                    ga_source.create_intraday_sql_views(query=ga_source.get_items_query(), table_type="flat_items",
                                                        wait_for_the_query_job_to_complete=True)

                    logging.info(
                        f"Created an {os.environ['ITEMS']} intraday SQL view for {input_event.dataset} for {input_event.table_date_shard}")
                else:
                    logging.info(
                        f"{os.environ['ITEMS']} flattening query for {input_event.dataset} not configured to run")

                # USER_PROPERTIES
                if input_event.flatten_nested_table(nested_table=os.environ["USER_PROPERTIES"]):
                    ga_source.create_intraday_sql_views(query=ga_source.get_user_properties_query(),
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