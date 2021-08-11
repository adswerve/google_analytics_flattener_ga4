from tests.test_base import BaseUnitTest
from intraday.main import create_intraday_schedule
import json

class TestBuildIntradayFlatteningSchedule(BaseUnitTest):

    def test_build_intraday_flattening_schedule(self):

        event_payload = '{"bucket": "ga-flattener-deployment-464892960897-adswerve-ga-flat-config", "contentLanguage": "en", "contentType": "application/json", "crc32c": "VpMchQ==", "etag": "CO7Xw/aSp/ICEAE=", "generation": "1628622319315950", "id": "ga-flattener-deployment-464892960897-adswerve-ga-flat-config/config_datasets.json/1628622319315950", "kind": "storage#object", "md5Hash": "XDXO3lGD0bdZXDEDj+ZtGA==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/ga-flattener-deployment-464892960897-adswerve-ga-flat-config/o/config_datasets.json?generation=1628622319315950&alt=media", "metageneration": "1", "name": "config_datasets.json", "selfLink": "https://www.googleapis.com/storage/v1/b/ga-flattener-deployment-464892960897-adswerve-ga-flat-config/o/config_datasets.json", "size": "234", "storageClass": "STANDARD", "timeCreated": "2021-08-10T19:05:19.391Z", "timeStorageClassUpdated": "2021-08-10T19:05:19.391Z", "updated": "2021-08-10T19:05:19.391Z"}'

        event_payload = json.loads(event_payload)

        create_intraday_schedule(event=event_payload, context="context")

        self.assertTrue(True)