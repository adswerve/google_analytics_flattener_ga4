from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage
from cfintradaysqlview.main import IntradaySQLView
from tests.rsc import sample_desired_queries


class TestGenerateQuerySchemaChange1(BaseUnitTest):
    c = Context()
    ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_type=c.env["table_type"],
                                            date_shard=c.env["date_collected_traffic_source_added"],
                                            )

    ga_source_intraday = IntradaySQLView(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_name=c.env["table_type_intraday"],
                                            date_shard=c.env["date_collected_traffic_source_added"],
                                            )

    def helper_clean_up_query(self, query):
        """Cleans up a sample hardcoded query , so this query can be compared to a dynamically generated query
            for testing purposes"""

        query_cleaned_up = query.upper().replace(" ", "").replace("\n", "").replace("\t", "").replace("%%Y%%M%%D",
                                                                                                      "%Y%M%D")

        return query_cleaned_up

    def helper_clean_up_dynamically_generated_query(self, query, ga_source):
        """Cleans up a dynamically generated query, so this query can be compared to a sample hardcoded query
            for testing purposes"""

        query_cleaned_up = query.replace(ga_source.gcp_project, "gcp-project").replace(
            ga_source.dataset, "dataset").replace(ga_source.date_shard, "date_shard")

        query_cleaned_up = self.helper_clean_up_query(query_cleaned_up)

        return query_cleaned_up

    # Compare dynamically generated queries to hardcoded expected baseline examples
    # Compare sample hardcoded vs. dynamically generated query. They should be the same
    def test_check_sql_query_events(self):
        sample_hardcoded_events_query = sample_desired_queries.sample_events_query_on_and_after_20230503
        sample_hardcoded_events_query = self.helper_clean_up_query(sample_hardcoded_events_query)

        test_events_dynamic_query = self.ga_source.get_events_query()
        test_events_dynamic_query = self.helper_clean_up_dynamically_generated_query(test_events_dynamic_query,
                                                                                     self.ga_source)

        test_events_dynamic_query_intraday = self.ga_source_intraday.get_events_query()
        test_events_dynamic_query_intraday = self.helper_clean_up_dynamically_generated_query(test_events_dynamic_query_intraday,
                                                                                     self.ga_source_intraday)

        assert test_events_dynamic_query.endswith("FROM`GCP-PROJECT.DATASET.EVENTS_DATE_SHARD`")
        assert test_events_dynamic_query_intraday.endswith("FROM`GCP-PROJECT.DATASET.EVENTS_INTRADAY_DATE_SHARD`")

        assert "'DAILY'ASSOURCE_TABLE_TYPE" in test_events_dynamic_query
        assert "'INTRADAY'ASSOURCE_TABLE_TYPE" in test_events_dynamic_query_intraday

        assert sample_hardcoded_events_query == test_events_dynamic_query == test_events_dynamic_query_intraday.replace("_INTRADAY_", "_").replace("INTRADAY", "DAILY")

class TestGenerateQuerySchemaChange2(BaseUnitTest):
    c = Context()
    ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_type=c.env["table_type"],
                                            date_shard=c.env["date_is_active_user_added"],
                                            )

    ga_source_intraday = IntradaySQLView(gcp_project=c.env["project"],
                                         dataset=c.env["dataset"],
                                         table_name=c.env["table_type_intraday"],
                                         date_shard=c.env["date_is_active_user_added"],
                                         )

    def helper_clean_up_query(self, query):
        """Cleans up a sample hardcoded query , so this query can be compared to a dynamically generated query
            for testing purposes"""

        query_cleaned_up = query.upper().replace(" ", "").replace("\n", "").replace("\t", "").replace("%%Y%%M%%D",
                                                                                                      "%Y%M%D")

        return query_cleaned_up

    def helper_clean_up_dynamically_generated_query(self, query, ga_source):
        """Cleans up a dynamically generated query, so this query can be compared to a sample hardcoded query
            for testing purposes"""

        query_cleaned_up = query.replace(ga_source.gcp_project, "gcp-project").replace(
            ga_source.dataset, "dataset").replace(ga_source.date_shard, "date_shard")

        query_cleaned_up = self.helper_clean_up_query(query_cleaned_up)

        return query_cleaned_up

    # Compare dynamically generated queries to hardcoded expected baseline examples
    # Compare sample hardcoded vs. dynamically generated query. They should be the same
    def test_check_sql_query_events(self):
        sample_hardcoded_events_query = sample_desired_queries.sample_events_query_on_and_after_20230717
        sample_hardcoded_events_query = self.helper_clean_up_query(sample_hardcoded_events_query)

        test_events_dynamic_query = self.ga_source.get_events_query()
        test_events_dynamic_query = self.helper_clean_up_dynamically_generated_query(test_events_dynamic_query,
                                                                                     self.ga_source)

        test_events_dynamic_query_intraday = self.ga_source_intraday.get_events_query()
        test_events_dynamic_query_intraday = self.helper_clean_up_dynamically_generated_query(
            test_events_dynamic_query_intraday,
            self.ga_source_intraday)

        assert test_events_dynamic_query.endswith("FROM`GCP-PROJECT.DATASET.EVENTS_DATE_SHARD`")
        assert test_events_dynamic_query_intraday.endswith("FROM`GCP-PROJECT.DATASET.EVENTS_INTRADAY_DATE_SHARD`")

        assert "'DAILY'ASSOURCE_TABLE_TYPE" in test_events_dynamic_query
        assert "'INTRADAY'ASSOURCE_TABLE_TYPE" in test_events_dynamic_query_intraday

        assert sample_hardcoded_events_query == test_events_dynamic_query == test_events_dynamic_query_intraday.replace(
            "_INTRADAY_", "_").replace("INTRADAY", "DAILY")

    def tearDown(self):
        pass
