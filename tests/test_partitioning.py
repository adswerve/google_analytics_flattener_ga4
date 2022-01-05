#TODO: do we need a more extended unit test for partitioning?
# load config file, run the flattening function, verify inputs and outputs?

# TODO: check that schemas match in sharded and partitioned table

#TODO: make sure that event_date col is a DATE, not TIMESTAMP

# TODO: verify partitioning

# TODO: test to verify idempotent nature of append operation

# TODO: test backfill with diff config options (sharding vs partitioning vs both)

# TODO: test with the intraday feature


from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage


class TestPartitioning(BaseUnitTest):
    c = Context()
    ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_name=c.env["table_type"],
                                            date_shard=c.env["date"],
                                            )

    def test_flatten_ga_data_config_output_type_partitioned_only(self):
        self.ga_source.run_query_job(query=self.ga_source.get_event_params_query(), table_type="flat_event_params",
                                     sharded_output_required=False, partitioned_output_required=True)

        self.ga_source.run_query_job(query=self.ga_source.get_events_query(), table_type="flat_events",
                                     sharded_output_required=False, partitioned_output_required=True)

        self.ga_source.run_query_job(query=self.ga_source.get_items_query(), table_type="flat_items",
                                     sharded_output_required=False, partitioned_output_required=True)

        self.ga_source.run_query_job(query=self.ga_source.get_user_properties_query(),
                                     table_type="flat_user_properties", sharded_output_required=False,
                                     partitioned_output_required=True)

        self.assertTrue(True)

    def test_flatten_ga_data_config_output_type_sharded_and_partitioned(self):
        self.ga_source.run_query_job(query=self.ga_source.get_event_params_query(), table_type="flat_event_params",
                                     sharded_output_required=True, partitioned_output_required=True)

        self.ga_source.run_query_job(query=self.ga_source.get_events_query(), table_type="flat_events",
                                     sharded_output_required=True, partitioned_output_required=True)

        self.ga_source.run_query_job(query=self.ga_source.get_items_query(), table_type="flat_items",
                                     sharded_output_required=True, partitioned_output_required=True)

        self.ga_source.run_query_job(query=self.ga_source.get_user_properties_query(),
                                     table_type="flat_user_properties", sharded_output_required=True,
                                     partitioned_output_required=True)

        self.assertTrue(True)