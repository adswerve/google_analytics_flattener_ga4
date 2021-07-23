import base64
import json
from google.cloud import bigquery
from google.cloud import storage
import re
import os
import tempfile


class InputValidator(object):
    def __init__(self, event):
        try:
            message_payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
            bq_destination_table = \
            message_payload['protoPayload']['serviceData']['jobCompletedEvent']['job']['jobConfiguration']['load'][
                'destinationTable']
            self.gcp_project = bq_destination_table['projectId']
            self.dataset = bq_destination_table['datasetId']
            self.table_date_shard = re.search('_(20\d\d\d\d\d\d)$', bq_destination_table['tableId']).group(1)
            self.table_name = re.search('(ga_.*)_20\d\d\d\d\d\d$', bq_destination_table['tableId']).group(1)
        except AttributeError:
            print(f'invalid message: {message_payload}')
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(os.environ["config_bucket_name"])
            blob = bucket.blob(os.environ["config_filename"])
            downloaded_file = os.path.join(tempfile.gettempdir(), "tmp.json")
            blob.download_to_filename(downloaded_file)
            with open(downloaded_file, "r") as config_json:
                self.config = json.load(config_json)
        except Exception as e:
            print(f'flattener configuration error: {e}')

    def valid_dataset(self):
        return self.dataset in self.config.keys()

    def flatten_nested_table(self, nested_table):
        return nested_table in self.config[self.dataset]


# TODO: ga4 - update
class GaExportedNestedDataStorage(object):
    def __init__(self, gcp_project, dataset, table_name, date_shard, type='DAILY'):
        self.gcp_project = gcp_project
        self.dataset = dataset
        self.date_shard = date_shard
        self.table_name = table_name
        self.type = type
        self.ALIAS_HITS = "hit" #TODO: update alias for ga4
        self.alias = {"hits": self.ALIAS_HITS
            , "product": "%sProduct" % self.ALIAS_HITS
            , "promotion": "%sPromotion" % self.ALIAS_HITS
            , "experiment": "%sExperiment" % self.ALIAS_HITS}

        # column names to be used in select statement - source from GA Export Schema documentation

        self.unique_event_id_fields = [
            "stream_id",
            "user_pseudo_id",
            "event_name",
            "event_timestamp"
        ]

        self.event_params_fields = [
            "event_params.key",

            "event_params.value.string_value",
            "event_params.value.int_value",
            "event_params.value.float_value",
            "event_params.value.double_value"
        ]

        self.user_properties_fields = [
            "user_properties.key",

            "user_properties.value.string_value",
            "user_properties.value.int_value",
            "user_properties.value.float_value",
            "user_properties.value.double_value",

            "user_properties.value.set_timestamp_micros"
        ]

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
            "app_info.fire",
            "app_info.install_source",

            "traffic_source.name",
            "traffic_source.medium",
            "traffic_source.source",
            "stream_id",
            "platform",

            "event_dimensions.hostname",

            "ecommerce.total_item_quantity",
            "ecommerce.purc",
            "ecommerce.purc",
            "ecommerce.refund_value_in_usd",
            "ecommerce.refund_value",
            "ecommerce.shipping_value_in_usd",
            "ecommerce.shipping_value",
            "ecommerce.tax_value_in_usd",
            "ecommerce.tax_value",
            "ecommerce.unique_items",
            "ecommerce.transaction_id"
        ]

    def get_unnest_alias(self, key):
        return self.alias[key]

    def get_unique_event_id(self, unique_event_id_fields):
        return 'CONCAT(%s, "_", %s, "_", %s, "_", %s) as session_id' % (unique_event_id_fields[0],
                                                                        unique_event_id_fields[1],
                                                                        unique_event_id_fields[2],
                                                                        unique_event_id_fields[2])

    def get_event_params_query(self):
        qry = "SELECT "

        qry += self.get_unique_event_id(self.unique_event_id_fields)

        qry += ",%s as %s" % (self.event_params_fields[0], self.event_params_fields[0].replace(".", "_"))

        qry += ",CONCAT(IFNULL(%s, ''), IFNULL(CAST(%s AS STRING), ''), IFNULL(CAST(%s AS STRING), ''), IFNULL(CAST(%s AS STRING), '')) AS event_params_value" \
               % (self.event_params_fields[1], self.event_params_fields[2], self.event_params_fields[3], self.event_params_fields[4])


        qry += " FROM `{p}.{ds}.{t}_{d}`".format(p=self.gcp_project, ds=self.dataset, t=self.table_name,
                                                 d=self.date_shard)

        qry += ",UNNEST (event_params) AS event_params"

        return qry

    def get_user_properties_query(self):
        qry = "SELECT "

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

        qry += self.get_unique_event_id(self.unique_event_id_fields)

        for f in self.items_fields:
            qry += ",%s as %s" % (f, f.replace(".", "_"))

        qry += " FROM `{p}.{ds}.{t}_{d}`".format(p=self.gcp_project, ds=self.dataset, t=self.table_name,
                                                 d=self.date_shard)

        qry += ",UNNEST (items) AS items"



        return qry

    def get_events_query(self):
        pass

    def _createValidBigQueryFieldName(self, pField):
        '''
        BQ Fields must contain only letters, numbers, and underscores, start with a letter or underscore,
        and be at most 128 characters long.
        :param pField: starting point of the field
        :return: cleaned big query field name
        '''
        r = ""
        for char in pField.lower():
            if char.isalnum():
                r += char
            else:
                r += "_"
        if r[0].isdigit():
            r = "_%s" % r
        return r[:127]

    def run_query_job(self, query, table_type='flat'):
        client = bigquery.Client()
        table_name = "{p}.{ds}.{t}_{d}" \
            .format(p=self.gcp_project, ds=self.dataset, t=table_type, d=self.date_shard)
        table_id = bigquery.Table(table_name)
        query_job_config = bigquery.QueryJobConfig(
            destination=table_id
            , dry_run=False
            , use_query_cache=False
            , labels={"queryfunction": "flatteningquery"}  # todo: apply proper labels
            , write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        query_job = client.query(query,
                                 job_config=query_job_config)
        # query_job.result()  # Waits for job to complete.
        return


def flatten_ga_data(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
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
            ga_source.run_query_job(query=ga_source.get_event_params_query(), table_type="ga_flat_event_params")
            print(f'Ran {os.environ["EVENT_PARAMS"]} flattening query for {input_event.dataset}')
        else:
            print(f'{os.environ["EVENT_PARAMS"]} flattening query for {input_event.dataset} not configured to run')

        # USER_PROPERTIES
        if input_event.flatten_nested_table(nested_table=os.environ["USER_PROPERTIES"]):
            ga_source.run_query_job(query=ga_source.get_user_properties_query(), table_type="ga_flat_user_properties")
            print(f'Ran {os.environ["USER_PROPERTIES"]} flattening query for {input_event.dataset}')
        else:
            print(f'{os.environ["USER_PROPERTIES"]} flattening query for {input_event.dataset} not configured to run')

        # ITEMS
        if input_event.flatten_nested_table(nested_table=os.environ["ITEMS"]):
            ga_source.run_query_job(query=ga_source.get_items_query(), table_type="ga_flat_items")
            print(f'Ran {os.environ["ITEMS"]} flattening query for {input_event.dataset}')
        else:
            print(f'{os.environ["ITEMS"]} flattening query for {input_event.dataset} not configured to run')

        # EVENTS
        if input_event.flatten_nested_table(nested_table=os.environ["EVENTS"]):
            ga_source.run_query_job(query=ga_source.get_events_query(), table_type="ga_flat_events")
            print(f'Ran {os.environ["EVENTS"]} flattening query for {input_event.dataset}')
        else:
            print(f'{os.environ["EVENTS"]} flattening query for {input_event.dataset} not configured to run')

    else:
        print(f'Dataset {input_event.dataset} not configured for flattening')
