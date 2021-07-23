from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage
from tests.rsc import sample_desired_queries

class TestGenerateQuery(BaseUnitTest):

    def test_check_sql_queries(self):
        """Compares dynamically generated queries to hardcoded expected baseline examples"""
        #TODO: refactor so there's less repetition, maybe with some
        #   loop
        #   helper function
        #   matching lookup dict?

        c = Context()
        ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                                dataset=BaseUnitTest.DATASET,
                                                table_name=BaseUnitTest.TABLE_TYPE,
                                                date_shard=BaseUnitTest.DATE,
                                                )

        # EVENTS
        sample_hardcoded_events_query = sample_desired_queries.sample_events_query
        sample_hardcoded_events_query = sample_hardcoded_events_query.upper().replace(" ", "").replace("\n",
                                                                                                       "").replace("\t",
                                                                                                                   "")

        test_events_dynamic_query = ga_source.get_events_query()
        test_events_dynamic_query = test_events_dynamic_query.replace(ga_source.gcp_project, "gcp-project").replace(
            ga_source.dataset, "dataset").replace(ga_source.date_shard, "date_shard")
        test_events_dynamic_query = test_events_dynamic_query.upper().replace(" ", "").replace("\n", "").replace("\t",
                                                                                                                 "")

        assert sample_hardcoded_events_query == test_events_dynamic_query

        # EVENT_PARAMS
        sample_hardcoded_event_params_query = sample_desired_queries.sample_event_params_query
        sample_hardcoded_event_params_query = sample_hardcoded_event_params_query.upper().replace(" ", "").replace("\n",
                                                                                                                   "").replace(
            "\t", "")

        test_event_params_dynamic_query = ga_source.get_event_params_query()
        test_event_params_dynamic_query = test_event_params_dynamic_query.replace(ga_source.gcp_project,
                                                                                  "gcp-project").replace(
            ga_source.dataset, "dataset").replace(ga_source.date_shard, "date_shard")
        test_event_params_dynamic_query = test_event_params_dynamic_query.upper().replace(" ", "").replace("\n",
                                                                                                           "").replace(
            "\t", "")

        assert sample_hardcoded_event_params_query == test_event_params_dynamic_query

        # ITEMS
        sample_hardcoded_items_query = sample_desired_queries.sample_items_query
        sample_hardcoded_items_query = sample_hardcoded_items_query.upper().replace(" ", "").replace("\n", "").replace(
            "\t", "")

        test_items_dynamic_query = ga_source.get_items_query()
        test_items_dynamic_query = test_items_dynamic_query.replace(ga_source.gcp_project, "gcp-project").replace(
            ga_source.dataset, "dataset").replace(ga_source.date_shard, "date_shard")
        test_items_dynamic_query = test_items_dynamic_query.upper().replace(" ", "").replace("\n", "").replace("\t", "")

        assert sample_hardcoded_items_query == test_items_dynamic_query

        # USER PROPERTIES
        sample_hardcoded_user_properties_query = sample_desired_queries.sample_user_properties_query
        sample_hardcoded_user_properties_query = sample_hardcoded_user_properties_query.upper().replace(" ",
                                                                                                        "").replace(
            "\n", "").replace("\t", "")

        test_user_properties_dynamic_query = ga_source.get_user_properties_query()
        test_user_properties_dynamic_query = test_user_properties_dynamic_query.replace(ga_source.gcp_project,
                                                                                        "gcp-project").replace(
            ga_source.dataset, "dataset").replace(ga_source.date_shard, "date_shard")
        test_user_properties_dynamic_query = test_user_properties_dynamic_query.upper().replace(" ", "").replace("\n",
                                                                                                                 "").replace(
            "\t", "")

        assert sample_hardcoded_user_properties_query == test_user_properties_dynamic_query

