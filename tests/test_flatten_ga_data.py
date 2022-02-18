from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage


class TestCFFlattenMethods(BaseUnitTest):
    c = Context()
    ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_name=c.env["table_type"],
                                            date_shard=c.env["date"],
                                            )

    def test_flatten_ga_data(self):
        self.ga_source.run_query_job(query=self.ga_source.get_event_params_query(), table_type="flat_event_params")
        self.ga_source.run_query_job(query=self.ga_source.get_events_query(), table_type="flat_events")
        self.ga_source.run_query_job(query=self.ga_source.get_items_query(), table_type="flat_items")
        self.ga_source.run_query_job(query=self.ga_source.get_user_properties_query(),
                                     table_type="flat_user_properties")

        self.assertTrue(True)
        #TODO: test intraday flattening

    def tearDown(self):
        self.delete_all_flat_tables_from_dataset()