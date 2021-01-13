import base64
import json
from google.cloud import bigquery
from google.cloud import storage
import re
import os
import tempfile


class InputValidator(object):
    def __init__(self,event):
        try:
            message_payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
            bq_destination_table = message_payload['protoPayload']['serviceData']['jobCompletedEvent']['job']['jobConfiguration']['load']['destinationTable']
            self.gcp_project = bq_destination_table['projectId']
            self.dataset = bq_destination_table['datasetId']
            self.table_date_shard = re.search('_(20\d\d\d\d\d\d)$', bq_destination_table['tableId']).group(1)
            self.table_name = re.search('(ga_.*)_20\d\d\d\d\d\d$', bq_destination_table['tableId']).group(1)
        except AttributeError:
            print('invalid message: {msg}'.format(msg=message_payload))

    def valid_dataset(self):
        storage_client = storage.Client()
        bucket = storage_client.bucket(os.environ["config_bucket_name"])
        blob = bucket.blob(os.environ["config_filename"])
        downloaded_file = os.path.join(tempfile.gettempdir(), "tmp.json")
        blob.download_to_filename(downloaded_file)
        with open(downloaded_file, "r") as config_json:
            config = json.load(config_json)
        return (self.dataset in config["datasets"])


class GaExportedNestedDataStorage(object):
    def __init__(self, gcp_project, dataset, table_name,date_shard, type='DAILY'):
        self.gcp_project = gcp_project
        self.dataset = dataset
        self.date_shard = date_shard
        self.table_name = table_name
        self.type = type
        self.ALIAS_HITS = "hit"
        self.alias = {"hits": self.ALIAS_HITS
                , "product": "%sProduct" % self.ALIAS_HITS
                , "promotion": "%sPromotion" % self.ALIAS_HITS
                , "experiment": "%sExperiment" % self.ALIAS_HITS}

        # column names to be used in select statement - source from GA Export Schema documentation
        self.session_fields = [
            'fullVisitorId'
            , 'visitStartTime'
            , 'visitNumber'
            , 'visitId'
            , 'userId'
            # , 'clientId' # will be added if date_shard >= '20180523'
            , 'channelGrouping'
            , 'socialEngagementType'
            , 'date'
            , 'totals.visits'
            , 'totals.hits'
            , 'totals.pageviews'
            , 'totals.timeOnSite'
            , 'totals.bounces'
            , 'totals.transactions'
            , 'totals.transactionRevenue'
            , 'totals.newVisits'
            , 'totals.screenviews'
            , 'totals.uniqueScreenviews'
            , 'totals.timeOnScreen'
            , 'totals.totalTransactionRevenue'
            # , 'totals.sessionQualityDim' # will be added if date_shard >= '20170711'
            , 'trafficSource.referralPath'
            , 'trafficSource.campaign'
            , 'trafficSource.source'
            , 'trafficSource.medium'
            , 'trafficSource.keyword'
            , 'trafficSource.adContent'
            , 'trafficSource.adwordsClickInfo.campaignId'
            , 'trafficSource.adwordsClickInfo.adGroupId'
            , 'trafficSource.adwordsClickInfo.creativeId'
            , 'trafficSource.adwordsClickInfo.criteriaId'
            , 'trafficSource.adwordsClickInfo.page'
            , 'trafficSource.adwordsClickInfo.slot'
            , 'trafficSource.adwordsClickInfo.criteriaParameters'
            , 'trafficSource.adwordsClickInfo.gclId'
            , 'trafficSource.adwordsClickInfo.customerId'
            , 'trafficSource.adwordsClickInfo.adNetworkType'
            , 'trafficSource.adwordsClickInfo.targetingCriteria.boomUserlistId'
            , 'trafficSource.adwordsClickInfo.isVideoAd'
            , 'trafficSource.isTrueDirect'
            , 'trafficSource.campaignCode'
            , 'device.browser'
            , 'device.browserVersion'
            , 'device.browserSize'
            , 'device.operatingSystem'
            , 'device.operatingSystemVersion'
            , 'device.isMobile'
            , 'device.mobileDeviceBranding'
            , 'device.mobileDeviceModel'
            , 'device.mobileInputSelector'
            , 'device.mobileDeviceInfo'
            , 'device.mobileDeviceMarketingName'
            , 'device.flashVersion'
            , 'device.javaEnabled'
            , 'device.language'
            , 'device.screenColors'
            , 'device.screenResolution'
            , 'device.deviceCategory'
            , 'geoNetwork.continent'
            , 'geoNetwork.subContinent'
            , 'geoNetwork.country'
            , 'geoNetwork.region'
            , 'geoNetwork.metro'
            , 'geoNetwork.city'
            , 'geoNetwork.cityId'
            , 'geoNetwork.networkDomain'
            , 'geoNetwork.latitude'
            , 'geoNetwork.longitude'
            , 'geoNetwork.networkLocation'
            , 'visitorId'
        ]

        if self.date_shard >= '20170711':
            self.session_fields.insert(20, 'totals.sessionQualityDim')

        if self.date_shard >= '20180523':
            self.session_fields.insert(5, 'clientId')

        self.hit_fields = [
            'hits.hitNumber'
            , 'hits.time'
            , 'hits.hour'
            , 'hits.minute'
            , 'hits.isSecure'
            , 'hits.isInteraction'
            , 'hits.isEntrance'
            , 'hits.isExit'
            , 'hits.referer'
            , 'hits.type'
            , 'hits.page.pagePath'
            , 'hits.page.hostname'
            , 'hits.page.pageTitle'
            , 'hits.page.searchKeyword'
            , 'hits.page.searchCategory'
            , 'hits.page.pagePathLevel1'
            , 'hits.page.pagePathLevel2'
            , 'hits.page.pagePathLevel3'
            , 'hits.page.pagePathLevel4'
            , 'hits.contentInfo.contentDescription'
            , 'hits.eventInfo.eventCategory'
            , 'hits.eventInfo.eventAction'
            , 'hits.eventInfo.eventLabel'
            , 'hits.eventInfo.eventValue'
            , 'hits.sourcePropertyInfo.sourcePropertyDisplayName'
            , 'hits.sourcePropertyInfo.sourcePropertyTrackingId'
            , 'hits.promotionActionInfo.promoIsView'
            , 'hits.promotionActionInfo.promoIsClick'
            # , 'hits.dataSource' # will be added if date_shard >= '20161114'
        ]

        if self.date_shard >= '20170711':
            self.hit_fields.insert(28, 'hits.dataSource')

        self.hit_transaction_fields = [
            'hits.transaction.transactionId'
            , 'hits.transaction.transactionRevenue'
            , 'hits.transaction.transactionTax'
            , 'hits.transaction.transactionShipping'
            , 'hits.transaction.affiliation'
            , 'hits.transaction.currencyCode'
            , 'hits.transaction.localTransactionRevenue'
            , 'hits.transaction.localTransactionTax'
            , 'hits.transaction.localTransactionShipping'
            , 'hits.transaction.transactionCoupon'

        ]
        self.hit_item_fields = [
            'hits.item.transactionId'
            , 'hits.item.productName'
            , 'hits.item.productCategory'
            , 'hits.item.productSku'
            , 'hits.item.itemQuantity'
            , 'hits.item.itemRevenue'
            , 'hits.item.currencyCode'
            , 'hits.item.localItemRevenue'
        ]
        self.hit_app_info_fields = [
            'hits.appInfo.name'
            , 'hits.appInfo.version'
            , 'hits.appInfo.id'
            , 'hits.appInfo.installerId'
            , 'hits.appInfo.appInstallerId'
            , 'hits.appInfo.appName'
            , 'hits.appInfo.appVersion'
            , 'hits.appInfo.appId'
            , 'hits.appInfo.screenName'
            , 'hits.appInfo.landingScreenName'
            , 'hits.appInfo.exitScreenName'
            , 'hits.appInfo.screenDepth'
        ]
        self.hit_exception_info_fields = [
            'hits.exceptionInfo.description'
            , 'hits.exceptionInfo.isFatal'
            , 'hits.exceptionInfo.exceptions'
            , 'hits.exceptionInfo.fatalExceptions'
        ]
        self.hit_refund_fields = [
            'hits.refund.refundAmount'
            , 'hits.refund.localRefundAmount'
        ]
        self.hit_ecommerce_action_fields = [
            'hits.eCommerceAction.action_type'
            , 'hits.eCommerceAction.step'
            , 'hits.eCommerceAction.option'
        ]
        self.hit_social_fields = [
            'hits.social.socialInteractionNetwork'
            , 'hits.social.socialInteractionAction'
            , 'hits.social.socialInteractions'
            , 'hits.social.socialInteractionTarget'
            , 'hits.social.socialNetwork'
            , 'hits.social.uniqueSocialInteractions'
            , 'hits.social.hasSocialSourceReferral'
            , 'hits.social.socialInteractionNetworkAction'
        ]
        self.hit_latency_tracking_fields = [
            'hits.latencyTracking.pageLoadSample'
            , 'hits.latencyTracking.pageLoadTime'
            , 'hits.latencyTracking.pageDownloadTime'
            , 'hits.latencyTracking.redirectionTime'
            , 'hits.latencyTracking.speedMetricsSample'
            , 'hits.latencyTracking.domainLookupTime'
            , 'hits.latencyTracking.serverConnectionTime'
            , 'hits.latencyTracking.serverResponseTime'
            , 'hits.latencyTracking.domLatencyMetricsSample'
            , 'hits.latencyTracking.domInteractiveTime'
            , 'hits.latencyTracking.domContentLoadedTime'
            , 'hits.latencyTracking.userTimingValue'
            , 'hits.latencyTracking.userTimingSample'
            , 'hits.latencyTracking.userTimingVariable'
            , 'hits.latencyTracking.userTimingCategory'
            , 'hits.latencyTracking.userTimingLabel'
        ]
        self.hit_content_group_fields = [
            'hits.contentGroup.contentGroup1'
            , 'hits.contentGroup.contentGroup2'
            , 'hits.contentGroup.contentGroup3'
            , 'hits.contentGroup.contentGroup4'
            , 'hits.contentGroup.contentGroup5'
            , 'hits.contentGroup.previousContentGroup1'
            , 'hits.contentGroup.previousContentGroup2'
            , 'hits.contentGroup.previousContentGroup3'
            , 'hits.contentGroup.previousContentGroup4'
            , 'hits.contentGroup.previousContentGroup5'
            , 'hits.contentGroup.contentGroupUniqueViews1'
            , 'hits.contentGroup.contentGroupUniqueViews2'
            , 'hits.contentGroup.contentGroupUniqueViews3'
            , 'hits.contentGroup.contentGroupUniqueViews4'
            , 'hits.contentGroup.contentGroupUniqueViews5'
        ]
        self.hit_publisher_fields = [
            'hits.publisher.dfpClicks'
            , 'hits.publisher.dfpImpressions'
            , 'hits.publisher.dfpMatchedQueries'
            , 'hits.publisher.dfpMeasurableImpressions'
            , 'hits.publisher.dfpQueries'
            , 'hits.publisher.dfpRevenueCpm'
            , 'hits.publisher.dfpRevenueCpc'
            , 'hits.publisher.dfpViewableImpressions'
            , 'hits.publisher.dfpPagesViewed'
            , 'hits.publisher.adsenseBackfillDfpClicks'
            , 'hits.publisher.adsenseBackfillDfpImpressions'
            , 'hits.publisher.adsenseBackfillDfpMatchedQueries'
            , 'hits.publisher.adsenseBackfillDfpMeasurableImpressions'
            , 'hits.publisher.adsenseBackfillDfpQueries'
            , 'hits.publisher.adsenseBackfillDfpRevenueCpm'
            , 'hits.publisher.adsenseBackfillDfpRevenueCpc'
            , 'hits.publisher.adsenseBackfillDfpViewableImpressions'
            , 'hits.publisher.adsenseBackfillDfpPagesViewed'
            , 'hits.publisher.adxBackfillDfpClicks'
            , 'hits.publisher.adxBackfillDfpImpressions'
            , 'hits.publisher.adxBackfillDfpMatchedQueries'
            , 'hits.publisher.adxBackfillDfpMeasurableImpressions'
            , 'hits.publisher.adxBackfillDfpQueries'
            , 'hits.publisher.adxBackfillDfpRevenueCpm'
            , 'hits.publisher.adxBackfillDfpRevenueCpc'
            , 'hits.publisher.adxBackfillDfpViewableImpressions'
            , 'hits.publisher.adxBackfillDfpPagesViewed'
            , 'hits.publisher.adxClicks'
            , 'hits.publisher.adxImpressions'
            , 'hits.publisher.adxMatchedQueries'
            , 'hits.publisher.adxMeasurableImpressions'
            , 'hits.publisher.adxQueries'
            , 'hits.publisher.adxRevenue'
            , 'hits.publisher.adxViewableImpressions'
            , 'hits.publisher.adxPagesViewed'
            , 'hits.publisher.adsViewed'
            , 'hits.publisher.adsUnitsViewed'
            , 'hits.publisher.adsUnitsMatched'
            , 'hits.publisher.viewableAdsViewed'
            , 'hits.publisher.measurableAdsViewed'
            , 'hits.publisher.adsPagesViewed'
            , 'hits.publisher.adsClicked'
            , 'hits.publisher.adsRevenue'
            , 'hits.publisher.dfpAdGroup'
            , 'hits.publisher.dfpAdUnits'
            , 'hits.publisher.dfpNetworkId'
        ]
        self.hit_publisher_infos_fields = [
            'hits.publisher_infos.dfpClicks'
            , 'hits.publisher_infos.dfpImpressions'
            , 'hits.publisher_infos.dfpMatchedQueries'
            , 'hits.publisher_infos.dfpMeasurableImpressions'
            , 'hits.publisher_infos.dfpQueries'
            , 'hits.publisher_infos.dfpRevenueCpm'
            , 'hits.publisher_infos.dfpRevenueCpc'
            , 'hits.publisher_infos.dfpViewableImpressions'
            , 'hits.publisher_infos.dfpPagesViewed'
            , 'hits.publisher_infos.adsenseBackfillDfpClicks'
            , 'hits.publisher_infos.adsenseBackfillDfpImpressions'
            , 'hits.publisher_infos.adsenseBackfillDfpMatchedQueries'
            , 'hits.publisher_infos.adsenseBackfillDfpMeasurableImpressions'
            , 'hits.publisher_infos.adsenseBackfillDfpQueries'
            , 'hits.publisher_infos.adsenseBackfillDfpRevenueCpm'
            , 'hits.publisher_infos.adsenseBackfillDfpRevenueCpc'
            , 'hits.publisher_infos.adsenseBackfillDfpViewableImpressions'
            , 'hits.publisher_infos.adsenseBackfillDfpPagesViewed'
            , 'hits.publisher_infos.adxBackfillDfpClicks'
            , 'hits.publisher_infos.adxBackfillDfpImpressions'
            , 'hits.publisher_infos.adxBackfillDfpMatchedQueries'
            , 'hits.publisher_infos.adxBackfillDfpMeasurableImpressions'
            , 'hits.publisher_infos.adxBackfillDfpQueries'
            , 'hits.publisher_infos.adxBackfillDfpRevenueCpm'
            , 'hits.publisher_infos.adxBackfillDfpRevenueCpc'
            , 'hits.publisher_infos.adxBackfillDfpViewableImpressions'
            , 'hits.publisher_infos.adxBackfillDfpPagesViewed'
            , 'hits.publisher_infos.adxClicks'
            , 'hits.publisher_infos.adxImpressions'
            , 'hits.publisher_infos.adxMatchedQueries'
            , 'hits.publisher_infos.adxMeasurableImpressions'
            , 'hits.publisher_infos.adxQueries'
            , 'hits.publisher_infos.adxRevenue'
            , 'hits.publisher_infos.adxViewableImpressions'
            , 'hits.publisher_infos.adxPagesViewed'
            , 'hits.publisher_infos.adsViewed'
            , 'hits.publisher_infos.adsUnitsViewed'
            , 'hits.publisher_infos.adsUnitsMatched'
            , 'hits.publisher_infos.viewableAdsViewed'
            , 'hits.publisher_infos.measurableAdsViewed'
            , 'hits.publisher_infos.adsPagesViewed'
            , 'hits.publisher_infos.adsClicked'
            , 'hits.publisher_infos.adsRevenue'
            , 'hits.publisher_infos.dfpAdGroup'
            , 'hits.publisher_infos.dfpAdUnits'
            , 'hits.publisher_infos.dfpNetworkId'

        ]
        self.hit_product_fields = [
            'hits.product.productSKU'
            , 'hits.product.v2ProductName'
            , 'hits.product.v2ProductCategory'
            , 'hits.product.productVariant'
            , 'hits.product.productBrand'
            , 'hits.product.productRevenue'
            , 'hits.product.localProductRevenue'
            , 'hits.product.productPrice'
            , 'hits.product.localProductPrice'
            , 'hits.product.productQuantity'
            , 'hits.product.productRefundAmount'
            , 'hits.product.localProductRefundAmount'
            , 'hits.product.isImpression'
            , 'hits.product.isClick'
            , 'hits.product.productListName'
            , 'hits.product.productListPosition'
            # , 'hits.product.productCouponCode'  # will be added if date_shard >= '20180423'

        ]
        if self.date_shard >= '20180424':
            self.hit_product_fields.insert(16, 'hits.product.productCouponCode')

        self.hit_promotion_fields = [
            'hits.promotion.promoId'
            , 'hits.promotion.promoName'
            , 'hits.promotion.promoCreative'
            , 'hits.promotion.promoPosition'
        ]
        self.hit_experiment_fields = [
            'hits.experiment.experimentId'
            , 'hits.experiment.experimentVariant'
        ]

    def get_unnest_alias(self, key):
        return self.alias[key]

    def get_session_query(self):
        qry = "select "
        qry += 'CONCAT(%s, ".", CAST(%s as STRING), ".", CAST(%s as STRING)) as session_id' % (self.session_fields[0],
                                                                                               self.session_fields[1],
                                                                                               self.session_fields[2])
        for f in self.session_fields:
            qry += ",%s as %s" % (f, f.replace(".", "_"))
        qry += ', CONCAT("{",( SELECT STRING_AGG(CONCAT(\'"\',CAST(cd.index AS STRING),\'"\',":",\'"\',' \
               'cd.value,\'"\')) FROM UNNEST(customDimensions) as cd WHERE NOT cd.value=""),' \
               '"}") session_custom_dimensions '
        qry += " from `{p}.{ds}.{t}_{d}`".format(p=self.gcp_project,ds=self.dataset,t=self.table_name,d=self.date_shard)
        return qry

    def get_hit_query(self, custom_vars=False):
        qry = "select "
        qry += 'CONCAT(%s, ".", CAST(%s as STRING), ".", CAST(%s as STRING)) as session_id' % (self.session_fields[0],
                                                                                               self.session_fields[1],
                                                                                               self.session_fields[2])
        qry += ',CONCAT(%s, ".", CAST(%s as STRING), ".", CAST(%s as STRING), ".", CAST(%s as STRING)) as hit_id' \
               % (self.session_fields[0]
                  , self.session_fields[1]
                  , self.session_fields[2]
                  , self.hit_fields[0].replace("hits", self.alias["hits"]))
        for f in self.hit_fields:
            qry += ",%s as %s" % (f.replace("hits", self.alias["hits"]), f.replace(".", "_"))
        for f in self.hit_ecommerce_action_fields:
            qry += ",%s as %s" % (f.replace("hits", self.alias["hits"]), f.replace(".", "_"))
        for f in self.hit_transaction_fields:
            qry += ",%s as %s" % (f.replace("hits", self.alias["hits"]), f.replace(".", "_"))
        for f in self.hit_refund_fields:
            qry += ",%s as %s" % (f.replace("hits", self.alias["hits"]), f.replace(".", "_"))
        for f in self.hit_item_fields:
            qry += ",%s as %s" % (f.replace("hits", self.alias["hits"]), f.replace(".", "_"))
        for f in self.hit_app_info_fields:
            qry += ",%s as %s" % (f.replace("hits", self.alias["hits"]), f.replace(".", "_"))
        for f in self.hit_exception_info_fields:
            qry += ",%s as %s" % (f.replace("hits", self.alias["hits"]), f.replace(".", "_"))
        for f in self.hit_social_fields:
            qry += ",%s as %s" % (f.replace("hits", self.alias["hits"]), f.replace(".", "_"))
        for f in self.hit_latency_tracking_fields:
            qry += ",%s as %s" % (f.replace("hits", self.alias["hits"]), f.replace(".", "_"))
        if self.date_shard >= '20161025':
            for f in self.hit_content_group_fields:
                qry += ",%s as %s" % (f.replace("hits", self.alias["hits"]), f.replace(".", "_"))
        if custom_vars:
            # Maximum of nr_custom_vars allowed per property
            for i in range(self.nr_custom_vars)[1:]:
                qry += ",(SELECT MAX(IF(index=%s, customVarName, NULL)) FROM UNNEST(%s.customVariables)) " \
                       "AS customVariableName%s,(SELECT MAX(IF(index=%s, customVarValue, NULL)) FROM " \
                       "UNNEST(%s.customVariables)) AS customVariableValue%s" % \
                       (str(i), self.ALIAS_HITS, str(i), str(i), self.ALIAS_HITS, str(i))
        qry += ' , CONCAT("{",( SELECT STRING_AGG(CONCAT(\'"\',CAST(cd.index AS STRING),\'"\',":",\'"\',' \
               'cd.value,\'"\')) FROM UNNEST(%s.customDimensions) as cd WHERE NOT cd.value=""),' \
               '"}") hit_custom_dimensions' % \
               self.alias["hits"]
        qry += ' , CONCAT("{",( SELECT STRING_AGG(CONCAT(\'"\',CAST(cm.index AS STRING),\'"\',":",\'"\',' \
               'CAST(cm.value AS STRING),\'"\')) FROM  UNNEST(%s.customMetrics) as cm),"}") hit_custom_metrics' % \
               self.alias["hits"]
        qry += " from `{p}.{ds}.{t}_{d}`".format(p=self.gcp_project, ds=self.dataset, t=self.table_name,
                                                 d=self.date_shard)
        qry += ",unnest(hits) as %s" % self.alias["hits"]

        return qry

    def get_hit_product_query(self):
        qry = "select "
        qry += 'CONCAT(%s, ".", CAST(%s as STRING), ".", CAST(%s as STRING)) as session_id' % (self.session_fields[0],
                                                                                               self.session_fields[1],
                                                                                               self.session_fields[2])
        qry += ',CONCAT(%s, ".", CAST(%s as STRING), ".", CAST(%s as STRING), ".", CAST(%s as STRING)) as hit_id' \
               % (self.session_fields[0],
                  self.session_fields[1],
                  self.session_fields[2],
                  self.hit_fields[0].replace("hits", self.alias["hits"]))
        for f in self.hit_product_fields:
            qry += ",%s as %s" % (f.replace("hits.product", self.alias["product"]), f.replace(".", "_"))
        qry += ' , CONCAT("{",( SELECT STRING_AGG(CONCAT(\'"\',CAST(cd.index AS STRING),\'"\',":",\'"\',' \
               'cd.value,\'"\')) FROM UNNEST(%s.customDimensions) as cd WHERE NOT cd.value=""),' \
               '"}") hit_custom_dimensions' % \
               self.alias["product"]
        qry += ' , CONCAT("{",( SELECT STRING_AGG(CONCAT(\'"\',CAST(cm.index AS STRING),\'"\',":",\'"\',' \
               'CAST(cm.value AS STRING),\'"\')) FROM  UNNEST(%s.customMetrics) as cm),"}") hit_custom_metrics' % \
               self.alias["product"]
        qry += " from `{p}.{ds}.{t}_{d}`".format(p=self.gcp_project, ds=self.dataset, t=self.table_name,
                                                 d=self.date_shard)
        qry += ",unnest(hits) as %s" % self.alias["hits"]
        qry += ",unnest(%s.product) as %s" % (self.alias['hits'], self.alias["product"])
        return qry
    def get_hit_promotion_query(self):
        qry = "select "
        qry += 'CONCAT(%s, ".", CAST(%s as STRING), ".", CAST(%s as STRING)) as session_id' % (self.session_fields[0],
                                                                                               self.session_fields[1],
                                                                                               self.session_fields[2])
        qry += ',CONCAT(%s, ".", CAST(%s as STRING), ".", CAST(%s as STRING), ".", CAST(%s as STRING)) as hit_id' \
               % (self.session_fields[0],
                  self.session_fields[1],
                  self.session_fields[2],
                  self.hit_fields[0].replace("hits", self.alias["hits"]))
        for f in self.hit_promotion_fields:
            qry += ",%s as %s" % (f.replace("hits.promotion", self.alias["promotion"]), f.replace(".", "_"))
        qry += " from `{p}.{ds}.{t}_{d}`".format(p=self.gcp_project, ds=self.dataset, t=self.table_name,
                                                 d=self.date_shard)
        qry += ",unnest(hits) as %s" % self.alias["hits"]
        qry += ",unnest(%s.promotion) as %s" % (self.alias['hits'], self.alias["promotion"])
        return qry
    def get_hit_experiment_query(self):
        qry = "select "
        qry += 'CONCAT(%s, ".", CAST(%s as STRING), ".", CAST(%s as STRING)) as session_id' % (self.session_fields[0],
                                                                                               self.session_fields[1],
                                                                                               self.session_fields[2])
        qry += ',CONCAT(%s, ".", CAST(%s as STRING), ".", CAST(%s as STRING), ".", CAST(%s as STRING)) as hit_id' \
               % (self.session_fields[0],
                  self.session_fields[1],
                  self.session_fields[2],
                  self.hit_fields[0].replace("hits", self.alias["hits"]))
        for f in self.hit_experiment_fields:
            qry += ",%s as %s" % (f.replace("hits.experiment", self.alias["experiment"]), f.replace(".", "_"))
        qry += " from `{p}.{ds}.{t}_{d}`".format(p=self.gcp_project, ds=self.dataset, t=self.table_name,
                                                 d=self.date_shard)
        qry += ",unnest(hits) as %s" % self.alias["hits"]
        qry += ",unnest(%s.experiment) as %s" % (self.alias['hits'], self.alias["experiment"])
        return qry

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
        table_name = "{p}.{ds}.{t}_{d}"\
            .format(p=self.gcp_project, ds=self.dataset,t=table_type, d=self.date_shard)
        table_id = bigquery.Table(table_name)
        query_job_config = bigquery.QueryJobConfig(
            destination=table_id
            ,dry_run=False
            ,use_query_cache=False
            ,labels={"queryfunction":"flatteningquery"}  #todo: apply proper labels
            ,write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
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
    print("flatten_ga_data: start - function")
    input_event = InputValidator(event)

    if input_event.valid_dataset():
        ga_source = GaExportedNestedDataStorage(gcp_project=input_event.gcp_project,
                                                dataset=input_event.dataset,
                                                table_name=input_event.table_name,
                                                date_shard=input_event.table_date_shard)
        ga_source.run_query_job(query=ga_source.get_session_query(), table_type="ga_flat_sessions")
        ga_source.run_query_job(query=ga_source.get_hit_query(), table_type="ga_flat_hits")
        ga_source.run_query_job(query=ga_source.get_hit_product_query(), table_type="ga_flat_products")
        ga_source.run_query_job(query=ga_source.get_hit_experiment_query(), table_type="ga_flat_experiments")
        ga_source.run_query_job(query=ga_source.get_hit_promotion_query(), table_type="ga_flat_promotions")
    else:
        print('Dataset {ds} not configured for flattening'.format(ds=input_event.dataset))

    print("flatten_ga_data: done - function")
