#TODO: (optional for now) - intraday flattening
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
from google.cloud.exceptions import NotFound


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
        pattern = re.compile(r'projects\/(.*?)\/datasets\/(.*?)\/tables\/(events|pseudonymous_users|users.*?)_(20\d\d\d\d\d\d)$')
        match = pattern.search(string)
        if match:
            project, dataset, table_type, shard = match.groups()
            return project, dataset, table_type, shard
        else:
            raise ValueError("String format is incorrect")

    def valid_dataset(self):
        return self.dataset in self.config.keys()

    def flatten_nested_tables(self):
        tables_config = self.config[self.dataset]["tables_to_flatten"]

        logging.info(
            f"table type is {self.table_type}")

        if self.table_type == "pseudonymous_users":

            tables = list(set(tables_config) & set(["pseudo_users",
                                                    "pseudo_user_properties",
                                                    "pseudo_user_audiences"]))
        elif self.table_type == "events":

            tables = list(set(tables_config) & set(["events",
                                                    "event_params",
                                                    "user_properties",
                                                    "items"]))

        elif self.table_type == "users":

            tables = list(set(tables_config) & set(["users",
                                                    "users_user_properties",
                                                    "users_user_audiences"]))

        return tables

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

        if self.table_type == "pseudonymous_users" or self.table_type=="users":
            self.date_field_name = "`date`"
        elif self.table_type == "events" or self.table_type == "events_intraday":
            self.date_field_name = "event_date"

    def source_table_is_intraday(self):
        return "intraday" in self.table_type

    def source_table_exists(self, table_type_param):
        """
         https://stackoverflow.com/questions/28731102/bigquery-check-if-table-already-exists
         """

        client = bigquery.Client(project=self.gcp_project)

        full_table_path = f"{self.gcp_project}.{self.dataset}.{table_type_param}_{self.date_shard}"
        table_id = bigquery.Table(full_table_path)
        try:
            client.get_table(table_id)
            return True
        except NotFound:
            logging.info(f"table {full_table_path} does not exist.")
            return False

    def get_temp_table_query(self):
        """
        build unique primary key
        """

        qry = ""

        if self.table_type in ("events", "events_intraday" ):

            qry = f"""
                      CREATE OR REPLACE TEMP TABLE temp_events AS (
                      SELECT          
                            CONCAT(
                                  stream_id, 
                                  "_",
                                  IFNULL(user_pseudo_id, ""), 
                                  "_",
                                  event_name,
                                  "_",
                                  event_timestamp, 
                                  "_",
                                  IFNULL(CAST(batch_event_index AS STRING),  ""),
                                  "_",
                                  IFNULL(CAST(batch_page_id AS STRING),  ""),
                                  "_",
                                  IFNULL(CAST(batch_ordering_id AS STRING), ""),
                                  "_",
                                
                                  ROW_NUMBER() OVER (PARTITION BY CONCAT(
                                                                  stream_id, 
                                                                  IFNULL(user_pseudo_id, ""), 
                                                                  event_name,
                                                                  event_timestamp, 
                                                                  IFNULL(CAST(batch_event_index AS STRING),  ""),
                                                                  IFNULL(CAST(batch_page_id AS STRING),  ""),
                                                                  IFNULL(CAST(batch_ordering_id AS STRING), "")
                                ))) event_id,
                            
                        *
                      FROM
                        `{self.gcp_project}.{self.dataset}.{self.table_type}_*`
                      WHERE _TABLE_SUFFIX = "{self.date_shard}"                           
                    )
                    ;
                  """
        elif self.table_type == "users":
            qry = f"""
            CREATE OR REPLACE TEMP TABLE temp_users AS (
            SELECT
                GENERATE_UUID() row_id,
                PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) `date`,
                * 
                FROM
                `{self.gcp_project}.{self.dataset}.{self.table_type}_*`
                  WHERE _TABLE_SUFFIX = "{self.date_shard}"  
            )
            ;
            """

        elif self.table_type == "pseudonymous_users":
            qry = f"""
                CREATE OR REPLACE TEMP TABLE temp_pseudo_users AS (
                SELECT
                    GENERATE_UUID() row_id,
                    PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) `date`,
                    * 
                    FROM
                    `{self.gcp_project}.{self.dataset}.{self.table_type}_*`
                      WHERE _TABLE_SUFFIX = "{self.date_shard}"  
                )
                ;
                """
        return qry


    def get_events_query_select_statement(self):
        qry = f"""SELECT
                    PARSE_DATE('%Y%m%d', event_date) AS event_date,
                    event_id,
                    CONCAT(user_pseudo_id, "_",(SELECT value.int_value from UNNEST(event_params) WHERE key = 'ga_session_id')) as session_id,
                    CONCAT(user_pseudo_id, "_",(SELECT value.int_value from UNNEST(event_params) WHERE key = 'ga_session_number')) as session_id_1,
                
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
                    ecommerce.transaction_id AS ecommerce_transaction_id,
                 
                    collected_traffic_source.manual_campaign_id AS collected_traffic_source_manual_campaign_id,
                    collected_traffic_source.manual_campaign_name AS collected_traffic_source_manual_campaign_name,
                    collected_traffic_source.manual_source AS collected_traffic_source_manual_source,
                    collected_traffic_source.manual_medium AS collected_traffic_source_manual_medium,
                    collected_traffic_source.manual_term AS collected_traffic_source_manual_term,
                    collected_traffic_source.manual_content AS collected_traffic_source_manual_content,
                    collected_traffic_source.manual_source_platform AS collected_traffic_source_manual_source_platform, 
                    collected_traffic_source.manual_creative_format AS collected_traffic_source_manual_creative_format, 
                    collected_traffic_source.manual_marketing_tactic AS collected_traffic_source_manual_marketing_tactic,
                    collected_traffic_source.gclid AS collected_traffic_source_gclid,
                    collected_traffic_source.dclid AS collected_traffic_source_dclid,
                    collected_traffic_source.srsltid AS collected_traffic_source_srsltid,
                    is_active_user AS is_active_user,
                    
                    batch_event_index AS batch_event_index, 
                    batch_page_id AS batch_page_id, 
                    batch_ordering_id AS batch_ordering_id, 
                
                    session_traffic_source_last_click.manual_campaign.campaign_id AS session_traffic_source_last_click_manual_campaign_campaign_id, 
                    session_traffic_source_last_click.manual_campaign.campaign_name AS session_traffic_source_last_click_manual_campaign_campaign_name, 
                    session_traffic_source_last_click.manual_campaign.`source` AS session_traffic_source_last_click_manual_campaign_source, 
                    session_traffic_source_last_click.manual_campaign.medium AS session_traffic_source_last_click_manual_campaign_medium, 
                    session_traffic_source_last_click.manual_campaign.term AS session_traffic_source_last_click_manual_campaign_term, 
                    session_traffic_source_last_click.manual_campaign.content AS session_traffic_source_last_click_manual_campaign_content, 
                    session_traffic_source_last_click.manual_campaign.source_platform AS session_traffic_source_last_click_manual_campaign_source_platform, 
                    session_traffic_source_last_click.manual_campaign.creative_format AS session_traffic_source_last_click_manual_campaign_creative_format, 
                    session_traffic_source_last_click.manual_campaign.marketing_tactic AS session_traffic_source_last_click_manual_campaign_marketing_tactic,
                    
                    session_traffic_source_last_click.google_ads_campaign.customer_id AS session_traffic_source_last_click_google_ads_campaign_customer_id,
                    session_traffic_source_last_click.google_ads_campaign.account_name AS session_traffic_source_last_click_google_ads_campaign_account_name, 
                    session_traffic_source_last_click.google_ads_campaign.campaign_id AS session_traffic_source_last_click_google_ads_campaign_campaign_id, 
                    session_traffic_source_last_click.google_ads_campaign.campaign_name AS session_traffic_source_last_click_google_ads_campaign_campaign_name, 
                    session_traffic_source_last_click.google_ads_campaign.ad_group_id AS session_traffic_source_last_click_google_ads_campaign_ad_group_id, 
                    session_traffic_source_last_click.google_ads_campaign.ad_group_name AS session_traffic_source_last_click_google_ads_campaign_ad_group_name,
                       
                    session_traffic_source_last_click.cross_channel_campaign.campaign_id AS session_traffic_source_last_click_cross_channel_campaign_campaign_id, 
                    session_traffic_source_last_click.cross_channel_campaign.campaign_name AS session_traffic_source_last_click_cross_channel_campaign_campaign_name, 
                    session_traffic_source_last_click.cross_channel_campaign.`source` AS session_traffic_source_last_click_cross_channel_campaign_source, 
                    session_traffic_source_last_click.cross_channel_campaign.medium AS session_traffic_source_last_click_cross_channel_campaign_medium, 
                    session_traffic_source_last_click.cross_channel_campaign.source_platform AS session_traffic_source_last_click_cross_channel_campaign_source_platform, 
                    session_traffic_source_last_click.cross_channel_campaign.default_channel_group AS session_traffic_source_last_click_cross_channel_campaign_default_channel_group, 
                    session_traffic_source_last_click.cross_channel_campaign.primary_channel_group AS session_traffic_source_last_click_cross_channel_campaign_primary_channel_group,
                    
                    session_traffic_source_last_click.sa360_campaign.campaign_id AS session_traffic_source_last_click_sa360_campaign_campaign_id, 
                    session_traffic_source_last_click.sa360_campaign.campaign_name AS session_traffic_source_last_click_sa360_campaign_campaign_name, 
                    session_traffic_source_last_click.sa360_campaign.`source` AS session_traffic_source_last_click_sa360_campaign_source, 
                    session_traffic_source_last_click.sa360_campaign.medium AS session_traffic_source_last_click_sa360_campaign_medium, 
                    session_traffic_source_last_click.sa360_campaign.ad_group_id AS session_traffic_source_last_click_sa360_campaign_ad_group_id, 
                    session_traffic_source_last_click.sa360_campaign.ad_group_name AS session_traffic_source_last_click_sa360_campaign_ad_group_name, 
                    session_traffic_source_last_click.sa360_campaign.creative_format AS session_traffic_source_last_click_sa360_campaign_creative_format, 
                    session_traffic_source_last_click.sa360_campaign.engine_account_name AS session_traffic_source_last_click_sa360_campaign_engine_account_name, 
                    session_traffic_source_last_click.sa360_campaign.engine_account_type AS session_traffic_source_last_click_sa360_campaign_engine_account_type, 
                    session_traffic_source_last_click.sa360_campaign.manager_account_name AS session_traffic_source_last_click_sa360_campaign_manager_account_name,
                    
                    session_traffic_source_last_click.cm360_campaign.campaign_id AS session_traffic_source_last_click_cm360_campaign_campaign_id, 
                    session_traffic_source_last_click.cm360_campaign.campaign_name AS session_traffic_source_last_click_cm360_campaign_campaign_name, 
                    session_traffic_source_last_click.cm360_campaign.`source` AS session_traffic_source_last_click_cm360_campaign_source, 
                    session_traffic_source_last_click.cm360_campaign.medium AS session_traffic_source_last_click_cm360_campaign_medium, 
                    session_traffic_source_last_click.cm360_campaign.account_id AS session_traffic_source_last_click_cm360_campaign_account_id, 
                    session_traffic_source_last_click.cm360_campaign.account_name AS session_traffic_source_last_click_cm360_campaign_account_name, 
                    session_traffic_source_last_click.cm360_campaign.advertiser_id AS session_traffic_source_last_click_cm360_campaign_advertiser_id, 
                    session_traffic_source_last_click.cm360_campaign.advertiser_name AS session_traffic_source_last_click_cm360_campaign_advertiser_name, 
                    session_traffic_source_last_click.cm360_campaign.creative_id AS session_traffic_source_last_click_cm360_campaign_creative_id, 
                    session_traffic_source_last_click.cm360_campaign.creative_format AS session_traffic_source_last_click_cm360_campaign_creative_format, 
                    session_traffic_source_last_click.cm360_campaign.creative_name AS session_traffic_source_last_click_cm360_campaign_creative_name, 
                    session_traffic_source_last_click.cm360_campaign.creative_type AS session_traffic_source_last_click_cm360_campaign_creative_type, 
                    session_traffic_source_last_click.cm360_campaign.creative_type_id AS session_traffic_source_last_click_cm360_campaign_creative_type_id, 
                    session_traffic_source_last_click.cm360_campaign.creative_version AS session_traffic_source_last_click_cm360_campaign_creative_version, 
                    session_traffic_source_last_click.cm360_campaign.placement_id AS session_traffic_source_last_click_cm360_campaign_placement_id, 
                    session_traffic_source_last_click.cm360_campaign.placement_cost_structure AS session_traffic_source_last_click_cm360_campaign_placement_cost_structure, 
                    session_traffic_source_last_click.cm360_campaign.placement_name AS session_traffic_source_last_click_cm360_campaign_placement_name, 
                    session_traffic_source_last_click.cm360_campaign.rendering_id AS session_traffic_source_last_click_cm360_campaign_rendering_id, 
                    session_traffic_source_last_click.cm360_campaign.site_id AS session_traffic_source_last_click_cm360_campaign_site_id, 
                    session_traffic_source_last_click.cm360_campaign.site_name AS session_traffic_source_last_click_cm360_campaign_site_name,
                    
                    session_traffic_source_last_click.dv360_campaign.campaign_id AS session_traffic_source_last_click_dv360_campaign_campaign_id, 
                    session_traffic_source_last_click.dv360_campaign.campaign_name AS session_traffic_source_last_click_dv360_campaign_campaign_name, 
                    session_traffic_source_last_click.dv360_campaign.`source` AS session_traffic_source_last_click_dv360_campaign_source, 
                    session_traffic_source_last_click.dv360_campaign.medium AS session_traffic_source_last_click_dv360_campaign_medium, 
                    session_traffic_source_last_click.dv360_campaign.advertiser_id AS session_traffic_source_last_click_dv360_campaign_advertiser_id, 
                    session_traffic_source_last_click.dv360_campaign.advertiser_name AS session_traffic_source_last_click_dv360_campaign_advertiser_name, 
                    session_traffic_source_last_click.dv360_campaign.creative_id AS session_traffic_source_last_click_dv360_campaign_creative_id, 
                    session_traffic_source_last_click.dv360_campaign.creative_format AS session_traffic_source_last_click_dv360_campaign_creative_format, 
                    session_traffic_source_last_click.dv360_campaign.creative_name AS session_traffic_source_last_click_dv360_campaign_creative_name, 
                    session_traffic_source_last_click.dv360_campaign.exchange_id AS session_traffic_source_last_click_dv360_campaign_exchange_id, 
                    session_traffic_source_last_click.dv360_campaign.exchange_name AS session_traffic_source_last_click_dv360_campaign_exchange_name, 
                    session_traffic_source_last_click.dv360_campaign.insertion_order_id AS session_traffic_source_last_click_dv360_campaign_insertion_order_id, 
                    session_traffic_source_last_click.dv360_campaign.insertion_order_name AS session_traffic_source_last_click_dv360_campaign_insertion_order_name, 
                    session_traffic_source_last_click.dv360_campaign.line_item_id AS session_traffic_source_last_click_dv360_campaign_line_item_id, 
                    session_traffic_source_last_click.dv360_campaign.line_item_name AS session_traffic_source_last_click_dv360_campaign_line_item_name, 
                    session_traffic_source_last_click.dv360_campaign.partner_id AS session_traffic_source_last_click_dv360_campaign_partner_id, 
                    session_traffic_source_last_click.dv360_campaign.partner_name AS session_traffic_source_last_click_dv360_campaign_partner_name,
                    
                    publisher.ad_revenue_in_usd AS publisher_ad_revenue_in_usd, 
                    publisher.ad_format AS publisher_ad_format, 
                    publisher.ad_source_name AS publisher_ad_source_name, 
                    publisher.ad_unit_id AS publisher_ad_unit_id

                    """

        qry += f""" ,{self.source_table_type} AS source_table_type
        FROM temp_events
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
                    event_params.value.string_value AS event_params_string_value,
                    event_params.value.int_value AS event_params_int_value,
                    {self.source_table_type} AS source_table_type
              FROM 
                temp_events
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
                temp_events
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
                        
                FROM temp_events
                ,UNNEST (items) AS items;"""

        return qry


    def get_pseudo_users_select_statement(self):

        qry = f"""
            SELECT
               row_id,
               `date`,  

              pseudo_user_id,
              stream_id,
            
              user_info.last_active_timestamp_micros AS user_info_last_active_timestamp_micros,
              user_info.user_first_touch_timestamp_micros AS user_info_user_first_touch_timestamp_micros,
              user_info.first_purchase_date AS user_info_first_purchase_date,
            
              device.operating_system AS device_operating_system,
              device.category AS device_category,
              device.mobile_brand_name AS device_mobile_brand_name,
              device.mobile_model_name AS device_mobile_model_name,
              device.unified_screen_name AS device_unified_screen_name,
            
              geo.city AS geo_city,
              geo.country AS geo_country,
              geo.continent AS geo_continent,
              geo.region AS geo_region,
            
              user_ltv.revenue_in_usd AS user_ltv_revenue_in_usd,
              user_ltv.sessions AS user_ltv_sessions,
              user_ltv.engagement_time_millis AS user_ltv_engagement_time_millis,
              user_ltv.purchases AS user_ltv_purchases,
              user_ltv.engaged_sessions AS user_ltv_engaged_sessions,
              user_ltv.session_duration_micros AS user_ltv_session_duration_micros,
            
              predictions.in_app_purchase_score_7d AS predictions_in_app_purchase_score_7d,
              predictions.purchase_score_7d AS predictions_purchase_score_7d,
              predictions.churn_score_7d AS predictions_churn_score_7d,
              predictions.revenue_28d_in_usd AS predictions_revenue_28d_in_usd,
            
              privacy_info.is_limited_ad_tracking AS privacy_info_is_limited_ad_tracking,
              privacy_info.is_ads_personalization_allowed AS privacy_info_is_ads_personalization_allowed,
            
              occurrence_date,
              last_updated_date
            FROM
               temp_pseudo_users             
            ;"""

        return qry


    def get_pseudo_user_properties_select_statement(self):

        qry = f"""
            SELECT
              row_id,
             `date`,  

              pseudo_user_id,
              up.key user_property_key,
              up.value.string_value user_property_value,
              up.value.set_timestamp_micros user_property_set_timestamp_micros,
              up.value.user_property_name
            FROM
             temp_pseudo_users
              ,UNNEST(user_properties) up                        
            ;"""

        return qry

    def get_pseudo_user_audiences_select_statement(self):

        qry = f"""
            SELECT
             row_id,
             `date`,  

              pseudo_user_id,
              a.id audience_id,
              a.name audience_name,
              a.membership_start_timestamp_micros audience_membership_start_timestamp_micros,
              a.membership_expiry_timestamp_micros audience_membership_expiry_timestamp_micros,
              a.npa audience_npa
            FROM
            temp_pseudo_users
              ,UNNEST(audiences) a    
            ;"""

        return qry


    def get_users_select_statement(self):
        qry = f"""
        SELECT
          row_id,
          `date`,
        
          user_id,
        
          user_info.last_active_timestamp_micros as user_info_last_active_timestamp_micros,
          user_info.user_first_touch_timestamp_micros as user_info_user_first_touch_timestamp_micros,
          user_info.first_purchase_date as user_info_first_purchase_date,
        
          device.operating_system as device_operating_system,
          device.category as device_category,
          device.mobile_brand_name as device_mobile_brand_name,
          device.mobile_model_name as device_mobile_model_name,
          device.unified_screen_name as device_unified_screen_name,
        
          geo.city as geo_city,
          geo.country as geo_country,
          geo.continent as geo_continent,
          geo.region as geo_region,
        
          user_ltv.revenue_in_usd as user_ltv_revenue_in_usd,
          user_ltv.sessions as user_ltv_sessions,
          user_ltv.engagement_time_millis as user_ltv_engagement_time_millis,
          user_ltv.purchases as user_ltv_purchases,
          user_ltv.engaged_sessions as user_ltv_engaged_sessions,
          user_ltv.session_duration_micros as user_ltv_session_duration_micros,
        
          predictions.in_app_purchase_score_7d as predictions_in_app_purchase_score_7d,
          predictions.purchase_score_7d as predictions_purchase_score_7d,
          predictions.churn_score_7d as predictions_churn_score_7d,
          predictions.revenue_28d_in_usd as predictions_revenue_28d_in_usd,
        
          privacy_info.is_limited_ad_tracking as privacy_info_is_limited_ad_tracking,
          privacy_info.is_ads_personalization_allowed as privacy_info_is_ads_personalization_allowed,
        
          occurrence_date,
          last_updated_date
        FROM
          temp_users
          ;
        """
        return qry

    def get_users_user_properties_select_statement(self):
        qry = f"""
        SELECT
          row_id,
          `date`,
          user_id,
          up.key user_property_key,
          up.value.string_value user_property_value,
          up.value.set_timestamp_micros user_property_set_timestamp_micros,
          up.value.user_property_name
        FROM
            temp_users
           ,UNNEST(user_properties) up
          ;
        """
        return qry

    def get_users_user_audiences_select_statement(self):
        qry = f"""
        SELECT
          row_id,
          `date`,
          user_id,
          a.id audience_id,
          a.name audience_name,
          a.membership_start_timestamp_micros audience_membership_start_timestamp_micros,
          a.membership_expiry_timestamp_micros audience_membership_expiry_timestamp_micros,
          a.npa audience_npa
        FROM
          temp_users
          ,UNNEST(audiences) a
            ;
        """
        return qry

    def get_flat_table_update_query(self, select_statement, flat_table, sharded_output_required=True, partitioned_output_required=False):

        assert flat_table in ["flat_events",
                              "flat_event_params",
                              "flat_user_properties",
                              "flat_items",
                              "flat_pseudo_users",
                              "flat_pseudo_user_properties",
                              "flat_pseudo_user_audiences",
                              "flat_users",
                              "flat_users_user_properties",
                              "flat_users_user_audiences"]

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

            query += f"""CREATE TABLE IF NOT EXISTS `{self.gcp_project}.{self.dataset}.{flat_table}` 
                     PARTITION BY {self.date_field_name} 
                     AS {select_statement}
                     """

            query += f"""DELETE FROM `{dest_table_name}` 
                        WHERE {self.date_field_name} = PARSE_DATE('%Y%m%d', '{self.date_shard}');"""

            query += f"""
                        INSERT INTO `{dest_table_name}`
                        """
            query += select_statement

        return query

    def get_select_statement(self, flat_table):

        assert flat_table in ["flat_events",
                              "flat_event_params",
                              "flat_user_properties",
                              "flat_items",
                              "flat_pseudo_users",
                              "flat_pseudo_user_properties",
                              "flat_pseudo_user_audiences",
                              "flat_users",
                              "flat_users_user_properties",
                              "flat_users_user_audiences"
                              ]

        query = ""

        if flat_table == "flat_events":
            query += self.get_events_query_select_statement()

        elif flat_table == "flat_event_params":
            query += self.get_event_params_query_select_statement()

        elif flat_table == "flat_user_properties":
            query += self.get_user_properties_query_select_statement()

        elif flat_table == "flat_items":
            query += self.get_items_query_select_statement()

        elif flat_table == "flat_pseudo_users":
            query += self.get_pseudo_users_select_statement()

        elif flat_table == "flat_pseudo_user_properties":
            query += self.get_pseudo_user_properties_select_statement()

        elif flat_table == "flat_pseudo_user_audiences":
            query += self.get_pseudo_user_audiences_select_statement()

        elif flat_table == "flat_users":
            query += self.get_users_select_statement()

        elif flat_table == "flat_users_user_properties":
            query += self.get_users_user_properties_select_statement()

        elif flat_table == "flat_users_user_audiences":
            query += self.get_users_user_audiences_select_statement()

        return query

    def build_full_query(self, sharded_output_required=True, partitioned_output_required=False,
                         list_of_flat_tables=["flat_events",
                                              "flat_event_params",
                                              "flat_user_properties",
                                              "flat_items",
                                              "flat_pseudo_users",
                                              "flat_pseudo_user_properties",
                                              "flat_pseudo_user_audiences",
                                              "flat_users",
                                              "flat_users_user_properties",
                                              "flat_users_user_audiences"
                                              ]):

        assert len(list_of_flat_tables) >= 1, "At least 1 flat table needs to be included in the config file"
        query = ""

        if self.table_type in ("events", "events_intraday", "users", "pseudonymous_users"):

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

        if self.source_table_is_intraday():
            logging.info(
                f"Requested flattening of intraday table: {self.gcp_project}.{self.dataset}.{self.table_type}_{self.date_shard}`")
            if self.source_table_exists("events"):
                logging.warning(
                    f"Daily table for that date exists: {self.gcp_project}.{self.dataset}.events_{self.date_shard}`")
                logging.warning(
                    f"Skipping execution")
                return

        if self.source_table_exists(self.table_type):
            try:
                # Configure the query job
                job_config = bigquery.QueryJobConfig(
                    use_query_cache=True,  # Use cached results if available
                    labels={"queryfunction": "ga4flattener"}  # Add labels for the query job
                )

                # Run the query
                query_job = client.query(query, job_config=job_config)

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
        sharded_output_required = output_config["sharded"]
        partitioned_output_required = output_config["partitioned"]

        ga_source = GaExportedNestedDataStorage(gcp_project=input_event.gcp_project,
                                                dataset=input_event.dataset,
                                                table_type=input_event.table_type,
                                                date_shard=input_event.table_date_shard)

        tables = input_event.flatten_nested_tables()
        flat_tables = []
        for table in tables:
            flat_tables.append(f"flat_{table}")

        if len(flat_tables) == 0:
            logging.info(
                f"{input_event.dataset}.{input_event.table_type}_{input_event.table_date_shard} is not configured for flattening. Exiting the function.")
            return

        logging.info(f"GA4 flattener is writing data to flat tables {flat_tables} in dataset {input_event.dataset} for date {input_event.table_date_shard}")

        query = ga_source.build_full_query(sharded_output_required=sharded_output_required,
                                                 partitioned_output_required=partitioned_output_required,
                                                 list_of_flat_tables=flat_tables)

        ga_source.run_query_job(query, wait_for_the_query_job_to_complete=False)

    else:
        logging.warning(f"Dataset {input_event.dataset} not configured for flattening")