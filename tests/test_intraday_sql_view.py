from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cfintradaysqlview.main import InputValidatorIntraday, IntradaySQLView, manage_intraday_sql_view
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from testfixtures import log_capture
import json
import base64
class TestCFIntradaySQLView(BaseUnitTest):
    c = Context()
    ga_source_intraday = IntradaySQLView(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_name=c.env["table_type_intraday"],
                                            date_shard= c.env["date_intraday"],
                                            )

    def setUp(self):
        self.delete_all_flat_views_from_dataset()

    def tbl_exists(self, dataset, table_name):
        """
         https://stackoverflow.com/questions/28731102/bigquery-check-if-table-already-exists
         """
        client = bigquery.Client(project=self.ga_source_intraday.gcp_project)

        full_table_path = f"{self.ga_source_intraday.gcp_project}.{dataset}.{table_name}"
        table_id = bigquery.Table(full_table_path)
        try:
            client.get_table(table_id)
            return True
        except NotFound:
            return False

    # EVENT_PARAMS
    def test_create_sql_view_intraday_event_params(self):
        self.ga_source_intraday.create_intraday_sql_views(query=self.ga_source_intraday.get_event_params_query(),
                                     table_type="flat_event_params",
                                     wait_for_the_query_job_to_complete=True)

        assert self.tbl_exists(dataset=self.ga_source_intraday.dataset,
                               table_name=f"view_flat_event_params_{self.ga_source_intraday.date_shard}")

    # USER_PROPERTIES
    def test_create_sql_view_intraday_user_properties(self):
        self.ga_source_intraday.create_intraday_sql_views(query=self.ga_source_intraday.get_user_properties_query(),
                                     table_type="flat_user_properties",
                                     wait_for_the_query_job_to_complete=True)

        assert self.tbl_exists(dataset=self.ga_source_intraday.dataset,
                               table_name=f"view_flat_user_properties_{self.ga_source_intraday.date_shard}")

    # ITEMS
    def test_create_sql_view_intraday_items(self):
        self.ga_source_intraday.create_intraday_sql_views(query=self.ga_source_intraday.get_items_query(),
                                     table_type="flat_items",
                                     wait_for_the_query_job_to_complete=True)

        assert self.tbl_exists(dataset=self.ga_source_intraday.dataset,
                               table_name=f"view_flat_items_{self.ga_source_intraday.date_shard}")

    # EVENTS
    def test_create_sql_view_intraday_events(self):
        self.ga_source_intraday.create_intraday_sql_views(query=self.ga_source_intraday.get_events_query(),
                                     table_type="flat_events",
                                     wait_for_the_query_job_to_complete=True)

        assert self.tbl_exists(dataset=self.ga_source_intraday.dataset,
                               table_name=f"view_flat_events_{self.ga_source_intraday.date_shard}")

    def tearDown(self):
        self.delete_all_flat_views_from_dataset()

class TestManageIntradaySQLView(BaseUnitTest):

    c = Context()
    project_id = c.env["project"]
    project_number = c.env["project_number"]
    dataset_id = c.env["dataset"]
    date_shard = c.env["date_intraday"]
    table_type = c.env["table_type_intraday"]

    c = Context()
    ga_source_intraday = IntradaySQLView(gcp_project=project_id,
                                            dataset=dataset_id,
                                            table_name=table_type,  #TODO: make table_name and table_type var names consistent
                                            date_shard=date_shard,
                                            )

    SAMPLE_LOG_INTRADAY_TABLE_CREATED = {
        "protoPayload": {
            "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
            "status": {},
            "authenticationInfo": {
                "principalEmail": "firebase-measurement@system.gserviceaccount.com"
            },
            "requestMetadata": {
                "requestAttributes": {},
                "destinationAttributes": {}
            },
            "serviceName": "bigquery.googleapis.com",
            "methodName": "tableservice.insert",
            "authorizationInfo": [
                {
                    "resource": f"projects/{project_id}/datasets/{dataset_id}",
                    "permission": "bigquery.tables.create",
                    "granted": True,
                    "resourceAttributes": {}
                }
            ],
            "resourceName": f"projects/{project_number}/datasets/{dataset_id}/tables",
            "serviceData": {
                "@type": "type.googleapis.com/google.cloud.bigquery.logging.v1.AuditData",
                "tableInsertRequest": {
                    "resource": {
                        "tableName": {
                            "projectId": project_number,
                            "datasetId": dataset_id,
                            "tableId": f"{table_type}_{date_shard}"
                        },
                        "info": {},
                        "view": {},
                        "schemaJson": "{\n  \"fields\": [{\n    \"name\": \"event_date\",\n    \"type\": \"STRING\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_timestamp\",\n    \"type\": \"INTEGER\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_name\",\n    \"type\": \"STRING\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_params\",\n    \"type\": \"RECORD\",\n    \"mode\": \"REPEATED\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"key\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"value\",\n        \"type\": \"RECORD\",\n        \"mode\": \"NULLABLE\",\n        \"schema\": {\n          \"fields\": [{\n            \"name\": \"string_value\",\n            \"type\": \"STRING\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"int_value\",\n            \"type\": \"INTEGER\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"float_value\",\n            \"type\": \"FLOAT\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"double_value\",\n            \"type\": \"FLOAT\",\n            \"mode\": \"NULLABLE\"\n          }]\n        }\n      }]\n    }\n  }, {\n    \"name\": \"event_previous_timestamp\",\n    \"type\": \"INTEGER\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_value_in_usd\",\n    \"type\": \"FLOAT\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_bundle_sequence_id\",\n    \"type\": \"INTEGER\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_server_timestamp_offset\",\n    \"type\": \"INTEGER\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"user_id\",\n    \"type\": \"STRING\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"user_pseudo_id\",\n    \"type\": \"STRING\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"privacy_info\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"analytics_storage\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"ads_storage\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"uses_transient_token\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"user_properties\",\n    \"type\": \"RECORD\",\n    \"mode\": \"REPEATED\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"key\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"value\",\n        \"type\": \"RECORD\",\n        \"mode\": \"NULLABLE\",\n        \"schema\": {\n          \"fields\": [{\n            \"name\": \"string_value\",\n            \"type\": \"STRING\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"int_value\",\n            \"type\": \"INTEGER\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"float_value\",\n            \"type\": \"FLOAT\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"double_value\",\n            \"type\": \"FLOAT\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"set_timestamp_micros\",\n            \"type\": \"INTEGER\",\n            \"mode\": \"NULLABLE\"\n          }]\n        }\n      }]\n    }\n  }, {\n    \"name\": \"user_first_touch_timestamp\",\n    \"type\": \"INTEGER\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"user_ltv\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"revenue\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"currency\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"device\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"category\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"mobile_brand_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"mobile_model_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"mobile_marketing_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"mobile_os_hardware_model\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"operating_system\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"operating_system_version\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"vendor_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"advertising_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"language\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"is_limited_ad_tracking\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"time_zone_offset_seconds\",\n        \"type\": \"INTEGER\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"browser\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"browser_version\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"web_info\",\n        \"type\": \"RECORD\",\n        \"mode\": \"NULLABLE\",\n        \"schema\": {\n          \"fields\": [{\n            \"name\": \"browser\",\n            \"type\": \"STRING\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"browser_version\",\n            \"type\": \"STRING\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"hostname\",\n            \"type\": \"STRING\",\n            \"mode\": \"NULLABLE\"\n          }]\n        }\n      }]\n    }\n  }, {\n    \"name\": \"geo\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"continent\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"country\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"region\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"city\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"sub_continent\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"metro\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"app_info\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"version\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"install_store\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"firebase_app_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"install_source\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"traffic_source\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"medium\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"source\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"stream_id\",\n    \"type\": \"STRING\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"platform\",\n    \"type\": \"STRING\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_dimensions\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"hostname\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"ecommerce\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"total_item_quantity\",\n        \"type\": \"INTEGER\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"purchase_revenue_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"purchase_revenue\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"refund_value_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"refund_value\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"shipping_value_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"shipping_value\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"tax_value_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"tax_value\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"unique_items\",\n        \"type\": \"INTEGER\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"transaction_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"items\",\n    \"type\": \"RECORD\",\n    \"mode\": \"REPEATED\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"item_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_brand\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_variant\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_category\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_category2\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_category3\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_category4\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_category5\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"price_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"price\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"quantity\",\n        \"type\": \"INTEGER\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_revenue_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_revenue\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_refund_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_refund\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"coupon\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"affiliation\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"location_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_list_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_list_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_list_index\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"promotion_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"promotion_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"creative_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"creative_slot\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }]\n}"
                    }
                },
                "tableInsertResponse": {
                    "resource": {
                        "tableName": {
                            "projectId": project_id,
                            "datasetId": dataset_id,
                            "tableId": f"{table_type}_{date_shard}"
                        },
                        "info": {},
                        "view": {},
                        "createTime": "2021-10-11T07:00:17.787Z",
                        "schemaJson": "{\n  \"fields\": [{\n    \"name\": \"event_date\",\n    \"type\": \"STRING\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_timestamp\",\n    \"type\": \"INTEGER\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_name\",\n    \"type\": \"STRING\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_params\",\n    \"type\": \"RECORD\",\n    \"mode\": \"REPEATED\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"key\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"value\",\n        \"type\": \"RECORD\",\n        \"mode\": \"NULLABLE\",\n        \"schema\": {\n          \"fields\": [{\n            \"name\": \"string_value\",\n            \"type\": \"STRING\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"int_value\",\n            \"type\": \"INTEGER\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"float_value\",\n            \"type\": \"FLOAT\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"double_value\",\n            \"type\": \"FLOAT\",\n            \"mode\": \"NULLABLE\"\n          }]\n        }\n      }]\n    }\n  }, {\n    \"name\": \"event_previous_timestamp\",\n    \"type\": \"INTEGER\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_value_in_usd\",\n    \"type\": \"FLOAT\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_bundle_sequence_id\",\n    \"type\": \"INTEGER\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_server_timestamp_offset\",\n    \"type\": \"INTEGER\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"user_id\",\n    \"type\": \"STRING\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"user_pseudo_id\",\n    \"type\": \"STRING\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"privacy_info\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"analytics_storage\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"ads_storage\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"uses_transient_token\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"user_properties\",\n    \"type\": \"RECORD\",\n    \"mode\": \"REPEATED\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"key\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"value\",\n        \"type\": \"RECORD\",\n        \"mode\": \"NULLABLE\",\n        \"schema\": {\n          \"fields\": [{\n            \"name\": \"string_value\",\n            \"type\": \"STRING\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"int_value\",\n            \"type\": \"INTEGER\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"float_value\",\n            \"type\": \"FLOAT\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"double_value\",\n            \"type\": \"FLOAT\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"set_timestamp_micros\",\n            \"type\": \"INTEGER\",\n            \"mode\": \"NULLABLE\"\n          }]\n        }\n      }]\n    }\n  }, {\n    \"name\": \"user_first_touch_timestamp\",\n    \"type\": \"INTEGER\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"user_ltv\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"revenue\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"currency\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"device\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"category\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"mobile_brand_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"mobile_model_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"mobile_marketing_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"mobile_os_hardware_model\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"operating_system\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"operating_system_version\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"vendor_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"advertising_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"language\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"is_limited_ad_tracking\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"time_zone_offset_seconds\",\n        \"type\": \"INTEGER\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"browser\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"browser_version\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"web_info\",\n        \"type\": \"RECORD\",\n        \"mode\": \"NULLABLE\",\n        \"schema\": {\n          \"fields\": [{\n            \"name\": \"browser\",\n            \"type\": \"STRING\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"browser_version\",\n            \"type\": \"STRING\",\n            \"mode\": \"NULLABLE\"\n          }, {\n            \"name\": \"hostname\",\n            \"type\": \"STRING\",\n            \"mode\": \"NULLABLE\"\n          }]\n        }\n      }]\n    }\n  }, {\n    \"name\": \"geo\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"continent\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"country\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"region\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"city\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"sub_continent\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"metro\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"app_info\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"version\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"install_store\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"firebase_app_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"install_source\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"traffic_source\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"medium\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"source\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"stream_id\",\n    \"type\": \"STRING\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"platform\",\n    \"type\": \"STRING\",\n    \"mode\": \"NULLABLE\"\n  }, {\n    \"name\": \"event_dimensions\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"hostname\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"ecommerce\",\n    \"type\": \"RECORD\",\n    \"mode\": \"NULLABLE\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"total_item_quantity\",\n        \"type\": \"INTEGER\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"purchase_revenue_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"purchase_revenue\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"refund_value_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"refund_value\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"shipping_value_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"shipping_value\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"tax_value_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"tax_value\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"unique_items\",\n        \"type\": \"INTEGER\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"transaction_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }, {\n    \"name\": \"items\",\n    \"type\": \"RECORD\",\n    \"mode\": \"REPEATED\",\n    \"schema\": {\n      \"fields\": [{\n        \"name\": \"item_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_brand\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_variant\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_category\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_category2\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_category3\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_category4\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_category5\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"price_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"price\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"quantity\",\n        \"type\": \"INTEGER\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_revenue_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_revenue\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_refund_in_usd\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_refund\",\n        \"type\": \"FLOAT\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"coupon\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"affiliation\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"location_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_list_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_list_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"item_list_index\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"promotion_id\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"promotion_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"creative_name\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }, {\n        \"name\": \"creative_slot\",\n        \"type\": \"STRING\",\n        \"mode\": \"NULLABLE\"\n      }]\n    }\n  }]\n}",
                        "updateTime": "2021-10-11T07:00:17.844Z"
                    }
                }
            }
        },
        "insertId": "-gms7aed23p8",
        "resource": {
            "type": "bigquery_resource",
            "labels": {
                "project_id": project_id
            }
        },
        "timestamp": "2021-10-11T07:00:17.888767Z",
        "severity": "NOTICE",
        "logName": f"projects/{project_id}/logs/cloudaudit.googleapis.com%2Factivity",
        "receiveTimestamp": "2021-10-11T07:00:17.901039177Z"
    }

    SAMPLE_LOAD_DATA_INTRADAY_TABLE_DELETED = {
        "protoPayload": {
            "@type": "type.googleapis.com/google.cloud.audit.AuditLog",
            "status": {},
            "authenticationInfo": {
                "principalEmail": "firebase-measurement@system.gserviceaccount.com"
            },
            "requestMetadata": {
                "requestAttributes": {},
                "destinationAttributes": {}
            },
            "serviceName": "bigquery.googleapis.com",
            "methodName": "tableservice.delete",
            "authorizationInfo": [
                {
                    "resource": f"projects/{project_id}/datasets/{dataset_id}/tables/events_intraday_{date_shard}",
                    "permission": "bigquery.tables.delete",
                    "granted": True,
                    "resourceAttributes": {}
                }
            ],
            "resourceName": f"projects/{project_number}/datasets/{dataset_id}/tables/{table_type}_{date_shard}"
        },
        "insertId": "-ef5jvyd24ld",
        "resource": {
            "type": "bigquery_resource",
            "labels": {
                "project_id": project_id
            }
        },
        "timestamp": "2021-10-11T16:55:00.193897Z",
        "severity": "NOTICE",
        "logName": f"projects/{project_id}/logs/cloudaudit.googleapis.com%2Factivity",
        "receiveTimestamp": "2021-10-11T16:55:00.433230860Z"
    }

    #TODO: remove repetition, store tbl_exists in a helper module
    def tbl_exists(self, dataset, table_name):
        """
         https://stackoverflow.com/questions/28731102/bigquery-check-if-table-already-exists
         """
        client = bigquery.Client(project=self.ga_source_intraday.gcp_project)

        full_table_path = f"{self.ga_source_intraday.gcp_project}.{dataset}.{table_name}"
        table_id = bigquery.Table(full_table_path)
        try:
            client.get_table(table_id)
            return True
        except NotFound:
            return False

    @log_capture()
    def test_create_intraday_sql_view(self, logcapture):
        """
        - delete SQL views
        - assert that it doesn't exist
        - generate DEFAULT config file
        - send input to function which says an intraday
        - assert that view does exist
        """
        self.delete_all_flat_views_from_dataset()

        expected_views = ["view_flat_events", "view_flat_event_params", "view_flat_items", "view_flat_user_properties"]


        for partial_table_name in expected_views:

            assert not self.tbl_exists(dataset=self.ga_source_intraday.dataset,
                                   table_name=f"{partial_table_name}_{self.ga_source_intraday.date_shard}")

        self.restore_default_config()

        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(self.SAMPLE_LOG_INTRADAY_TABLE_CREATED).encode('utf-8'))}

        # iv = InputValidatorIntraday(SAMPLE_PUBSUB_MESSAGE)

        manage_intraday_sql_view(SAMPLE_PUBSUB_MESSAGE)

        for partial_table_name in expected_views:

            assert self.tbl_exists(dataset=self.ga_source_intraday.dataset,
                                   table_name=f"{partial_table_name}_{self.ga_source_intraday.date_shard}")
