sample_events_query = """
SELECT 
    PARSE_DATE('%%Y%%m%%d', event_date) AS event_date,
    CONCAT(stream_id, '_' , user_pseudo_id, '_' ,  event_name,  '_' , event_timestamp) AS event_id,
    
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
    
    'daily' AS source_table_type
    
 FROM `gcp-project.dataset.events_date_shard` 
"""

sample_events_query_on_and_after_20230503 = """
SELECT 
    PARSE_DATE('%%Y%%m%%d', event_date) AS event_date,
    CONCAT(stream_id, '_' , user_pseudo_id, '_' ,  event_name,  '_' , event_timestamp) AS event_id,

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
    collected_traffic_source.gclid AS collected_traffic_source_gclid,
    collected_traffic_source.dclid AS collected_traffic_source_dclid,
    collected_traffic_source.srsltid AS collected_traffic_source_srsltid,

    'daily' AS source_table_type

 FROM `gcp-project.dataset.events_date_shard` 
"""


sample_events_query_on_and_after_20230717 = """
SELECT 
    PARSE_DATE('%%Y%%m%%d', event_date) AS event_date,
    CONCAT(stream_id, '_' , user_pseudo_id, '_' ,  event_name,  '_' , event_timestamp) AS event_id,

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
    collected_traffic_source.gclid AS collected_traffic_source_gclid,
    collected_traffic_source.dclid AS collected_traffic_source_dclid,
    collected_traffic_source.srsltid AS collected_traffic_source_srsltid,
  
    is_active_user AS is_active_user,

    'daily' AS source_table_type

 FROM `gcp-project.dataset.events_date_shard` 
"""

sample_event_params_query = """
SELECT 
    PARSE_DATE('%%Y%%m%%d', event_date) AS event_date,
    CONCAT(stream_id, '_' , user_pseudo_id, '_' ,  event_name,  '_' , event_timestamp) AS event_id,
    event_params.key as event_params_key,     
    COALESCE(event_params.value.string_value,
        CAST(event_params.value.int_value AS STRING),
        CAST(event_params.value.float_value AS STRING),
        CAST(event_params.value.double_value AS STRING) 
    ) AS event_params_value,
    
    'daily' AS source_table_type
                            
FROM `gcp-project.dataset.events_date_shard` 
,UNNEST (event_params) AS event_params
"""


sample_user_properties_query = """
SELECT 
    PARSE_DATE('%%Y%%m%%d', event_date) AS event_date,
    CONCAT(stream_id, '_' , user_pseudo_id, '_' ,  event_name,  '_' , event_timestamp) AS event_id,
    user_properties.key	AS user_properties_key,            
    COALESCE(user_properties.value.string_value,
        CAST(user_properties.value.int_value AS STRING),
        CAST(user_properties.value.float_value AS STRING),
        CAST(user_properties.value.double_value AS STRING) 
    ) AS user_properties_value,
                
    user_properties.value.set_timestamp_micros AS user_properties_value_set_timestamp_micros,
    
    'daily' AS source_table_type
    
 FROM `gcp-project.dataset.events_date_shard` 
  ,UNNEST (user_properties) AS user_properties
"""

sample_items_query = """
SELECT 
    
    PARSE_DATE('%%Y%%m%%d', event_date) AS event_date,
    CONCAT(stream_id, '_' , user_pseudo_id, '_' ,  event_name,  '_' , event_timestamp) AS event_id,
    
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
    
    'daily' AS source_table_type

 FROM `gcp-project.dataset.events_date_shard` 
  ,UNNEST(items) AS items
"""