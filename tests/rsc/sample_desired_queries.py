#TODO: don't maintain 2 copies of hte same queries (main vs. sample desired queries file).
# Pull them from sample file into main file. Don't hardcode them in the main file. Simplify unit tests.
sample_events_query = """
SELECT 
    PARSE_DATE('%%Y%%m%%d', event_date) AS event_date,
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

    'daily' AS source_table_type
    
 FROM temp_events;
"""

sample_event_params_query = """
SELECT 
    PARSE_DATE('%%Y%%m%%d', event_date) AS event_date,
    event_id,
    event_params.key as event_params_key,     
    COALESCE(event_params.value.string_value,
        CAST(event_params.value.int_value AS STRING),
        CAST(event_params.value.float_value AS STRING),
        CAST(event_params.value.double_value AS STRING) 
    ) AS event_params_value,
    event_params.value.string_value AS event_params_string_value,
    event_params.value.int_value AS event_params_int_value,
    
    'daily' AS source_table_type
                            
FROM temp_events 
,UNNEST (event_params) AS event_params ; 
"""


sample_user_properties_query = """
SELECT 
    PARSE_DATE('%%Y%%m%%d', event_date) AS event_date,
    event_id,
    user_properties.key	AS user_properties_key,            
    COALESCE(user_properties.value.string_value,
        CAST(user_properties.value.int_value AS STRING),
        CAST(user_properties.value.float_value AS STRING),
        CAST(user_properties.value.double_value AS STRING) 
    ) AS user_properties_value,
                
    user_properties.value.set_timestamp_micros AS user_properties_value_set_timestamp_micros,
    
    'daily' AS source_table_type
    
 FROM temp_events
  ,UNNEST (user_properties) AS user_properties ;
"""

sample_items_query = """
SELECT 
    
    PARSE_DATE('%%Y%%m%%d', event_date) AS event_date,
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
    
    'daily' AS source_table_type

 FROM temp_events
  ,UNNEST(items) AS items ;
"""

sample_pseudo_users_query = """
SELECT
  PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) `date`,  
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
  `gcp-project.dataset.pseudonymous_users_*`
WHERE _TABLE_SUFFIX = "date_shard"   
  ;
"""

sample_pseudo_user_properties_query = """
SELECT
  PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) `date`,  
  
  pseudo_user_id,
  up.key user_property_key,
  up.value.string_value user_property_value,
  up.value.set_timestamp_micros user_property_set_timestamp_micros,
  up.value.user_property_name
FROM
    `gcp-project.dataset.pseudonymous_users_*`,
    UNNEST(user_properties) up
WHERE _TABLE_SUFFIX = "date_shard"    
  ;
"""

sample_pseudo_user_audiences_query = """
SELECT
  PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) `date`,  
  pseudo_user_id,
  a.id audience_id,
  a.name audience_name,
  a.membership_start_timestamp_micros audience_membership_start_timestamp_micros,
  a.membership_expiry_timestamp_micros audience_membership_expiry_timestamp_micros,
  a.npa audience_npa
FROM
    `gcp-project.dataset.pseudonymous_users_*`,
  UNNEST(audiences) a
WHERE _TABLE_SUFFIX = "date_shard" 
    ;
"""

sample_users_query = """
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

sample_users_user_properties_query = """
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

sample_users_user_audiences_query = """
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
