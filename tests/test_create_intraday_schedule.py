from tests.test_base import BaseUnitTest
from intraday.main import create_intraday_schedule
import json

class TestBuildIntradayFlatteningSchedule(BaseUnitTest):

    def test_build_intraday_flattening_schedule(self):

        event_payload = '{"bucket": "ga-flattener-deployment-464892960897-adswerve-ga-flat-config", "contentLanguage": "en", "contentType": "application/json", "crc32c": "VpMchQ==", "etag": "CO7Xw/aSp/ICEAE=", "generation": "1628622319315950", "id": "ga-flattener-deployment-464892960897-adswerve-ga-flat-config/config_datasets.json/1628622319315950", "kind": "storage#object", "md5Hash": "XDXO3lGD0bdZXDEDj+ZtGA==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/ga-flattener-deployment-464892960897-adswerve-ga-flat-config/o/config_datasets.json?generation=1628622319315950&alt=media", "metageneration": "1", "name": "config_datasets.json", "selfLink": "https://www.googleapis.com/storage/v1/b/ga-flattener-deployment-464892960897-adswerve-ga-flat-config/o/config_datasets.json", "size": "234", "storageClass": "STANDARD", "timeCreated": "2021-08-10T19:05:19.391Z", "timeStorageClassUpdated": "2021-08-10T19:05:19.391Z", "updated": "2021-08-10T19:05:19.391Z"}'

        event_payload = json.loads(event_payload)

        # TODO(developer): Uncomment and set the following variables
        project_id = "as-dev-ga4-flattener-320623"
        location_id = "us-central1"
        #  google.api_core.exceptions.InvalidArgument: 400 Location must equal us-central1 because the App Engine app that is associated with this project is located in us-central1
        service_id = 'my-service'

        create_intraday_schedule(project_id=project_id, location_id=location_id, service_id=service_id)

        self.assertTrue(True)