from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage
from tests.rsc import sample_desired_queries


class TestCFFlattenMethods(BaseUnitTest):

    def test_flatten_ga_data(self):
        c = Context()
        ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                                dataset=BaseUnitTest.DATASET,
                                                table_name=BaseUnitTest.TABLE_TYPE,
                                                date_shard=BaseUnitTest.DATE,
                                                )
        ga_source.run_query_job(query=ga_source.get_event_params_query(), table_type="flat_event_params")
        ga_source.run_query_job(query=ga_source.get_user_properties_query(), table_type="flat_user_properties")
        ga_source.run_query_job(query=ga_source.get_items_query(), table_type="flat_items")
        ga_source.run_query_job(query=ga_source.get_events_query(), table_type="flat_events")
        self.assertTrue(True)
