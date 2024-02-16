from tests.test_base import BaseUnitTest
import json
from tests.test_base import Context
import base64
from datetime import datetime
from pytz import timezone
import pytest
from google.cloud import scheduler
from testfixtures import log_capture

from cfintraday.main import manage_intraday_schedule
from cfconfigbuilder.main import FlattenerDatasetConfig
from cfconfigbuilder.main import FlattenerDatasetConfigStorage
from cfintraday.main import InputValidatorIntraday


class TestManageIntradayFlatteningSchedule(BaseUnitTest):
    c = Context()
    project_id = c.env["project"]
    project_number = c.env["project_number"]
    dataset_id = c.env["dataset"]

    now = datetime.now(timezone("America/Denver"))
    today = now.date()
    date_shard = today.strftime("%Y%m%d")

    table_type = "events_intraday"

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

    def test_intraday_input_validator_table_created(self):
        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(self.SAMPLE_LOG_INTRADAY_TABLE_CREATED).encode('utf-8'))}

        iv = InputValidatorIntraday(SAMPLE_PUBSUB_MESSAGE)

        self.assertEqual(self.date_shard, iv.table_date_shard)
        self.assertEqual(self.project_id, iv.gcp_project)
        self.assertEqual(self.dataset_id, iv.dataset)
        self.assertEqual(self.table_type, iv.table_name)
        assert isinstance(iv.valid_dataset(), bool)
        self.assertTrue(True)

    def test_intraday_input_validator_table_deleted(self):
        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(self.SAMPLE_LOAD_DATA_INTRADAY_TABLE_DELETED).encode('utf-8'))}

        iv = InputValidatorIntraday(SAMPLE_PUBSUB_MESSAGE)

        self.assertEqual(self.date_shard, iv.table_date_shard)
        self.assertEqual(self.project_id, iv.gcp_project)
        self.assertEqual(self.dataset_id, iv.dataset)
        self.assertEqual(self.table_type, iv.table_name)
        assert isinstance(iv.valid_dataset(), bool)
        self.assertTrue(True)

    @log_capture()
    def test_create_intraday_flattening_schedule_minutes(self, logcapture):
        """
        - generate config file which says we want to flatten intraday table every X mins
        - make sure the schedule job doesn't exist
        - create scheduler job
        - check cron string
        - check logs - should say scheduler job has been created
        """
        # generate config again
        # this dataset needs to be configured for intraday flattening
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.reformat_config(json_config)
        json_config = config.add_intraday_params_into_config(json_config, intraday_flat_tables_schedule={
                                                                                                      "frequency": 30,
                                                                                                      "units": "minutes"
                                                                                                    })
        json_config = config.add_output_format_params_into_config(json_config)
        store.upload_config(config=json_config)

        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(self.SAMPLE_LOG_INTRADAY_TABLE_CREATED).encode('utf-8'))}

        # make sure the scheduler job doesn't exist
        iv = InputValidatorIntraday(SAMPLE_PUBSUB_MESSAGE)

        job_id_full_path, _ = iv.contruct_scheduler_job_id_full_path()
        client = scheduler.CloudSchedulerClient()
        try:
            client.delete_job(name=job_id_full_path)
        except Exception as e:  # it already doesn't exist
            print(e)

        # create scheduler job
        manage_intraday_schedule(SAMPLE_PUBSUB_MESSAGE)

        response_get_job = client.get_job(
            request={
                "name": job_id_full_path
            })
        self.assertEqual(job_id_full_path, response_get_job.name)
        self.assertEqual('*/30 * * * *', response_get_job.schedule)
        expected = f"b\'{{\"protoPayload\": {{\"resourceName\": \"projects/{self.project_id}/datasets/{self.dataset_id}/tables/events_intraday_{self.date_shard}\"}}}}\'"
        actual = str(response_get_job.pubsub_target.data)
        assert expected == actual

        # check log
        expected_log = ('root', 'INFO',
                        f"Created Scheduler job: {job_id_full_path}")

        logcapture.check_present(expected_log, )

        # try creating the job again
        manage_intraday_schedule(SAMPLE_PUBSUB_MESSAGE)

        # check log
        expected_log = ('root', 'ERROR',
                        f"Error creating a Scheduler job {job_id_full_path} (the job probably already exists): 409 Job {job_id_full_path} already exists.")

        logcapture.check_present(expected_log, )

    @log_capture()
    def test_try_creating_intraday_flattening_schedule_with_default_config(self, logcapture):
        """
        - generate DEFAULT config file. No flattening should happen
        - make sure the schedule job doesn't exist
        - try creating scheduler job
        - check logs - should say that intraday table in this dataset is not configured for flattening
        - scheduler job wouldn't be created in this case
        """
        # generate config again
        # this dataset will NOT be configured for intraday flattening
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.reformat_config(json_config)
        json_config = config.add_intraday_params_into_config(json_config)
        json_config = config.add_output_format_params_into_config(json_config)
        store.upload_config(config=json_config)

        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(self.SAMPLE_LOG_INTRADAY_TABLE_CREATED).encode('utf-8'))}

        # make sure the scheduler job doesn't exist
        iv = InputValidatorIntraday(SAMPLE_PUBSUB_MESSAGE)

        job_id_full_path, _ = iv.contruct_scheduler_job_id_full_path()
        client = scheduler.CloudSchedulerClient()
        try:
            client.delete_job(name=job_id_full_path)
        except Exception as e:  # it already doesn't exist
            print(e)

        # try creating scheduler job
        manage_intraday_schedule(SAMPLE_PUBSUB_MESSAGE)

        # check log
        expected_log = ('root', 'WARNING',
                        f"Dataset {self.dataset_id} is not configured for intraday table flattening")

        logcapture.check_present(expected_log, )

    @log_capture()
    def test_create_intraday_flattening_schedule_hours(self, logcapture):
        """
        - generate config file which says we want to flatten intraday table every Y hours
        - make sure the schedule job doesn't exist
        - create scheduler job
        - check cron string
        - check logs - should say scheduler job has been created
        """
        # generate config again
        # this dataset needs to be configured for intraday flattening
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.reformat_config(json_config)
        json_config = config.add_intraday_params_into_config(json_config, intraday_flat_tables_schedule={
                                                                                                          "frequency": 1,
                                                                                                          "units": "hours"
                                                                                                        })
        json_config = config.add_output_format_params_into_config(json_config)
        store.upload_config(config=json_config)

        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(self.SAMPLE_LOG_INTRADAY_TABLE_CREATED).encode('utf-8'))}

        # make sure the scheduler job doesn't exist
        iv = InputValidatorIntraday(SAMPLE_PUBSUB_MESSAGE)

        job_id_full_path, _ = iv.contruct_scheduler_job_id_full_path()
        client = scheduler.CloudSchedulerClient()
        try:
            client.delete_job(name=job_id_full_path)
        except Exception as e:  # it already doesn't exist
            print(e)

        # create scheduler job
        manage_intraday_schedule(SAMPLE_PUBSUB_MESSAGE)

        response_get_job = client.get_job(
            request={
                "name": job_id_full_path
            })
        self.assertEqual(job_id_full_path, response_get_job.name)
        self.assertEqual('0 */1 * * *', response_get_job.schedule)

        # check log
        expected_log = ('root', 'INFO',
                        f"Created Scheduler job: {job_id_full_path}")

        logcapture.check_present(expected_log, )

    def test_create_intraday_flattening_schedule_invalid(self):
        """
        - generate config file with a schedule which GCP won't accept
        - check that assertion error is raised
        """

        # generate config again
        # this dataset needs to be configured for intraday flattening
        config = FlattenerDatasetConfig()
        store = FlattenerDatasetConfigStorage()
        json_config = config.get_ga_datasets()
        json_config = config.reformat_config(json_config)

        # GCP won't accept this cron schedule
        json_config = config.add_intraday_params_into_config(json_config, intraday_flat_tables_schedule= {
                                                                                                      "frequency": 60,
                                                                                                      "units": "minutes"
                                                                                                    })
        json_config = config.add_output_format_params_into_config(json_config)
        store.upload_config(config=json_config)

        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(self.SAMPLE_LOG_INTRADAY_TABLE_CREATED).encode('utf-8'))}

        with pytest.raises(AssertionError,
                           match="Config file error: if intraday schedule units are minutes, then the frequency should be between 1 and 59"):
            manage_intraday_schedule(SAMPLE_PUBSUB_MESSAGE)

    @log_capture()
    def test_delete_intraday_flattening_schedule(self, logcapture):
        """
        - check if scheduler job exists
            - if it doesn't exist, create scheduler job
                - before you do it, rebuild a config file which would allow us to create a scheduler job
        - delete job
        - check logs - should say job has been deleted
        - try deleting it again
        - check logs - should have a warning that it doesn't exist
        - revert config file to default
        """

        # does scheduler job exist?
        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(self.SAMPLE_LOG_INTRADAY_TABLE_CREATED).encode('utf-8'))}
        iv = InputValidatorIntraday(SAMPLE_PUBSUB_MESSAGE)
        job_id_full_path, _ = iv.contruct_scheduler_job_id_full_path()

        client = scheduler.CloudSchedulerClient()
        try:
            # check if job exists
            response_get_job = client.get_job(
                request={
                    "name": job_id_full_path
                })
        except Exception as e:

            # generate config again
            # this dataset needs to be configured for intraday flattening
            config = FlattenerDatasetConfig()
            store = FlattenerDatasetConfigStorage()
            json_config = config.get_ga_datasets()
            json_config = config.reformat_config(json_config)
            json_config = config.add_intraday_params_into_config(json_config, intraday_flat_tables_schedule={
                                                                                                      "frequency": 1,
                                                                                                      "units": "hours"
                                                                                                    })
            json_config = config.add_output_format_params_into_config(json_config)
            store.upload_config(config=json_config)

            # create scheduler job
            manage_intraday_schedule(SAMPLE_PUBSUB_MESSAGE)

        # delete the job
        SAMPLE_PUBSUB_MESSAGE = {'@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage', 'attributes':
            {'origin': 'python-unit-test', 'username': 'gcp'}
            , 'data': base64.b64encode(json.dumps(self.SAMPLE_LOAD_DATA_INTRADAY_TABLE_DELETED).encode('utf-8'))}
        manage_intraday_schedule(SAMPLE_PUBSUB_MESSAGE)

        # check log
        expected_log = ('root', 'INFO',
                        f"Deleted Scheduler job: {job_id_full_path}")

        logcapture.check_present(expected_log, )

        # try deleting the job again
        manage_intraday_schedule(SAMPLE_PUBSUB_MESSAGE)
        expected_log = ('root', 'WARNING',
                        f"Error deleting a Scheduler job {job_id_full_path} (the job probably doesn't exist): 404 Job not found.")

        logcapture.check_present(expected_log, )

    # TODO: split large tests into multiple small tests?

    def tearDown(self):
        self.restore_default_config()
        # test_delete_intraday_flattening_schedule will be the last one to run in this test class
        # so we don't need to worry about
        # deleting a Scheduler job
