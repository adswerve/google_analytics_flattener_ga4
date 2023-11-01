"""
The purpose of this file is to test the intraday Cloud Function in the cloud, on GCP (as opposed to locally).
Send a message to the Pub/Sub topic to simulate a situation that an intraday table got deleted or created.
This will trigger your intraday Cloud Function which will create or delete a Cloud Scheduler job
"""
from google.cloud import pubsub_v1
import json
from tests.test_base import Context
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
c = Context()

# To authenticate, run the following command.  The account you choose will execute this python script
# gcloud auth application-default login

'''*****************************'''
''' Configuration Section Start '''
'''*****************************'''
topic_id = "ga4-flattener-deployment-topic-intraday"  # pubsub topic your cloud function is subscribed to Example: [Deployment Name]-topic
project_id = "as-dev-ga4-flattener-320623"  # GCP project ID, example:  [PROJECT_ID]
project_number = "464892960897"
dataset_id = "analytics_222460912"
date_shard = "20220531"

table_type = "events_intraday"
message_type = "CREATE" # "CREATE" or "DELETE"
dry_run = False  # set to False to Backfill.  Setting to True will not pubish any messages to pubsub, but simply show what would have been published.
'''*****************************'''
'''  Configuration Section End  '''
'''*****************************'''

json_schema_dict = {"fields": [{
      "name": "event_date",
    "type": "STRING",
    "mode": "NULLABLE"
  }, {
      "name": "event_timestamp",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }, {
      "name": "event_name",
    "type": "STRING",
    "mode": "NULLABLE"
  }, {
      "name": "event_params",
    "type": "RECORD",
    "mode": "REPEATED",
    "schema": {
        "fields": [{
          "name": "key",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "value",
        "type": "RECORD",
        "mode": "NULLABLE",
        "schema": {
            "fields": [{
              "name": "string_value",
            "type": "STRING",
            "mode": "NULLABLE"
          }, {
              "name": "int_value",
            "type": "INTEGER",
            "mode": "NULLABLE"
          }, {
              "name": "float_value",
            "type": "FLOAT",
            "mode": "NULLABLE"
          }, {
              "name": "double_value",
            "type": "FLOAT",
            "mode": "NULLABLE"
          }]
        }
      }]
    }
  }, {
      "name": "event_previous_timestamp",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }, {
      "name": "event_value_in_usd",
    "type": "FLOAT",
    "mode": "NULLABLE"
  }, {
      "name": "event_bundle_sequence_id",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }, {
      "name": "event_server_timestamp_offset",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }, {
      "name": "user_id",
    "type": "STRING",
    "mode": "NULLABLE"
  }, {
      "name": "user_pseudo_id",
    "type": "STRING",
    "mode": "NULLABLE"
  }, {
      "name": "privacy_info",
    "type": "RECORD",
    "mode": "NULLABLE",
    "schema": {
        "fields": [{
          "name": "analytics_storage",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "ads_storage",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "uses_transient_token",
        "type": "STRING",
        "mode": "NULLABLE"
      }]
    }
  }, {
      "name": "user_properties",
    "type": "RECORD",
    "mode": "REPEATED",
    "schema": {
        "fields": [{
          "name": "key",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "value",
        "type": "RECORD",
        "mode": "NULLABLE",
        "schema": {
            "fields": [{
              "name": "string_value",
            "type": "STRING",
            "mode": "NULLABLE"
          }, {
              "name": "int_value",
            "type": "INTEGER",
            "mode": "NULLABLE"
          }, {
              "name": "float_value",
            "type": "FLOAT",
            "mode": "NULLABLE"
          }, {
              "name": "double_value",
            "type": "FLOAT",
            "mode": "NULLABLE"
          }, {
              "name": "set_timestamp_micros",
            "type": "INTEGER",
            "mode": "NULLABLE"
          }]
        }
      }]
    }
  }, {
      "name": "user_first_touch_timestamp",
    "type": "INTEGER",
    "mode": "NULLABLE"
  }, {
      "name": "user_ltv",
    "type": "RECORD",
    "mode": "NULLABLE",
    "schema": {
        "fields": [{
          "name": "revenue",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "currency",
        "type": "STRING",
        "mode": "NULLABLE"
      }]
    }
  }, {
      "name": "device",
    "type": "RECORD",
    "mode": "NULLABLE",
    "schema": {
        "fields": [{
          "name": "category",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "mobile_brand_name",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "mobile_model_name",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "mobile_marketing_name",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "mobile_os_hardware_model",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "operating_system",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "operating_system_version",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "vendor_id",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "advertising_id",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "language",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "is_limited_ad_tracking",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "time_zone_offset_seconds",
        "type": "INTEGER",
        "mode": "NULLABLE"
      }, {
          "name": "browser",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "browser_version",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "web_info",
        "type": "RECORD",
        "mode": "NULLABLE",
        "schema": {
            "fields": [{
              "name": "browser",
            "type": "STRING",
            "mode": "NULLABLE"
          }, {
              "name": "browser_version",
            "type": "STRING",
            "mode": "NULLABLE"
          }, {
              "name": "hostname",
            "type": "STRING",
            "mode": "NULLABLE"
          }]
        }
      }]
    }
  }, {
      "name": "geo",
    "type": "RECORD",
    "mode": "NULLABLE",
    "schema": {
        "fields": [{
          "name": "continent",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "country",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "region",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "city",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "sub_continent",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "metro",
        "type": "STRING",
        "mode": "NULLABLE"
      }]
    }
  }, {
      "name": "app_info",
    "type": "RECORD",
    "mode": "NULLABLE",
    "schema": {
        "fields": [{
          "name": "id",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "version",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "install_store",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "firebase_app_id",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "install_source",
        "type": "STRING",
        "mode": "NULLABLE"
      }]
    }
  }, {
      "name": "traffic_source",
    "type": "RECORD",
    "mode": "NULLABLE",
    "schema": {
        "fields": [{
          "name": "name",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "medium",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "source",
        "type": "STRING",
        "mode": "NULLABLE"
      }]
    }
  }, {
      "name": "stream_id",
    "type": "STRING",
    "mode": "NULLABLE"
  }, {
      "name": "platform",
    "type": "STRING",
    "mode": "NULLABLE"
  }, {
      "name": "event_dimensions",
    "type": "RECORD",
    "mode": "NULLABLE",
    "schema": {
        "fields": [{
          "name": "hostname",
        "type": "STRING",
        "mode": "NULLABLE"
      }]
    }
  }, {
      "name": "ecommerce",
    "type": "RECORD",
    "mode": "NULLABLE",
    "schema": {
        "fields": [{
          "name": "total_item_quantity",
        "type": "INTEGER",
        "mode": "NULLABLE"
      }, {
          "name": "purchase_revenue_in_usd",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "purchase_revenue",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "refund_value_in_usd",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "refund_value",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "shipping_value_in_usd",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "shipping_value",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "tax_value_in_usd",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "tax_value",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "unique_items",
        "type": "INTEGER",
        "mode": "NULLABLE"
      }, {
          "name": "transaction_id",
        "type": "STRING",
        "mode": "NULLABLE"
      }]
    }
  }, {
      "name": "items",
    "type": "RECORD",
    "mode": "REPEATED",
    "schema": {
        "fields": [{
          "name": "item_id",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "item_name",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "item_brand",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "item_variant",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "item_category",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "item_category2",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "item_category3",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "item_category4",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "item_category5",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "price_in_usd",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "price",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "quantity",
        "type": "INTEGER",
        "mode": "NULLABLE"
      }, {
          "name": "item_revenue_in_usd",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "item_revenue",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "item_refund_in_usd",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "item_refund",
        "type": "FLOAT",
        "mode": "NULLABLE"
      }, {
          "name": "coupon",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "affiliation",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "location_id",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "item_list_id",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "item_list_name",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "item_list_index",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "promotion_id",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "promotion_name",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "creative_name",
        "type": "STRING",
        "mode": "NULLABLE"
      }, {
          "name": "creative_slot",
        "type": "STRING",
        "mode": "NULLABLE"
      }]
    }
  }]
}

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
                    "schemaJson": json.dumps(json_schema_dict),
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
                    "schemaJson":  json.dumps(json_schema_dict),
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



publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(project_id, topic_id)

logging.info(f"Publishing a {message_type} intraday message to topic {topic_id} for {project_id}.{dataset_id}.events_{date_shard}")

if not dry_run:
    if message_type == "CREATE":
        # create scheduler job
        publisher.publish(topic_path, json.dumps(SAMPLE_LOG_INTRADAY_TABLE_CREATED).encode('utf-8'), origin='python-unit-test'
                          , username='gcp')
    elif message_type == "DELETE":
        # delete scheduler job
        publisher.publish(topic_path, json.dumps(SAMPLE_LOAD_DATA_INTRADAY_TABLE_DELETED).encode('utf-8'),
                          origin='python-unit-test'
                          , username='gcp')
