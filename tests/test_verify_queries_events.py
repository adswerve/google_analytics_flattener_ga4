from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage
from cfintradaysqlview.main import IntradaySQLView
from tests.rsc import sample_desired_queries


# test_get_flat_table_update_query_sharded_and_partitioned_output_required_flat_event_params

class TestGenerateQuerySourceTableEvents(BaseUnitTest):
    c = Context()
    ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_type=c.env["table_type"],
                                            date_shard=c.env["date"],
                                            )

    ga_source_intraday = IntradaySQLView(gcp_project=c.env["project"],
                                         dataset=c.env["dataset"],
                                         table_name=c.env["table_type_intraday"],
                                         date_shard=c.env["date_intraday"],
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
        sample_hardcoded_events_query = sample_desired_queries.sample_events_query
        sample_hardcoded_events_query = self.helper_clean_up_query(sample_hardcoded_events_query)

        test_events_dynamic_query = self.ga_source.get_events_query_select_statement()
        test_events_dynamic_query = self.helper_clean_up_dynamically_generated_query(test_events_dynamic_query,
                                                                                     self.ga_source)

        # test_events_dynamic_query_intraday = self.ga_source_intraday.get_events_query()
        # test_events_dynamic_query_intraday = self.helper_clean_up_dynamically_generated_query(
        #     test_events_dynamic_query_intraday,
        #     self.ga_source_intraday)

        assert test_events_dynamic_query.endswith("FROMTEMP_EVENTS;")
        # assert test_events_dynamic_query_intraday.endswith("FROM`GCP-PROJECT.DATASET.EVENTS_INTRADAY_DATE_SHARD`")

        assert "'DAILY'ASSOURCE_TABLE_TYPE" in test_events_dynamic_query
        # assert "'INTRADAY'ASSOURCE_TABLE_TYPE" in test_events_dynamic_query_intraday

        assert sample_hardcoded_events_query == test_events_dynamic_query # == test_events_dynamic_query_intraday.replace(
            # "_INTRADAY_", "_").replace("INTRADAY", "DAILY")

    def test_check_sql_query_event_params(self):
        sample_hardcoded_event_params_query = sample_desired_queries.sample_event_params_query
        sample_hardcoded_event_params_query = self.helper_clean_up_query(sample_hardcoded_event_params_query)

        test_event_params_dynamic_query = self.ga_source.get_event_params_query_select_statement()
        test_event_params_dynamic_query = self.helper_clean_up_dynamically_generated_query(
            test_event_params_dynamic_query, self.ga_source)

        # test_event_params_dynamic_query_intraday = self.ga_source_intraday.get_event_params_query()
        # test_event_params_dynamic_query_intraday = self.helper_clean_up_dynamically_generated_query(
        #     test_event_params_dynamic_query_intraday,
        #     self.ga_source_intraday)

        assert test_event_params_dynamic_query.endswith("FROMTEMP_EVENTS,UNNEST(EVENT_PARAMS)ASEVENT_PARAMS;")
        # assert test_event_params_dynamic_query_intraday.endswith(
        #     "FROM`GCP-PROJECT.DATASET.EVENTS_INTRADAY_DATE_SHARD`,UNNEST(EVENT_PARAMS)ASEVENT_PARAMS")

        assert "'DAILY'ASSOURCE_TABLE_TYPE" in test_event_params_dynamic_query
        # assert "'INTRADAY'ASSOURCE_TABLE_TYPE" in test_event_params_dynamic_query_intraday

        assert sample_hardcoded_event_params_query == test_event_params_dynamic_query #== test_event_params_dynamic_query_intraday.replace(
            # "_INTRADAY_", "_").replace("INTRADAY", "DAILY")

    def test_check_sql_query_items(self):
        sample_hardcoded_items_query = sample_desired_queries.sample_items_query
        sample_hardcoded_items_query = self.helper_clean_up_query(sample_hardcoded_items_query)

        test_items_dynamic_query = self.ga_source.get_items_query_select_statement()
        test_items_dynamic_query = self.helper_clean_up_dynamically_generated_query(test_items_dynamic_query,
                                                                                    self.ga_source)
        #
        # test_items_dynamic_query_intraday = self.ga_source_intraday.get_items_query()
        # test_items_dynamic_query_intraday_intraday = self.helper_clean_up_dynamically_generated_query(
        #     test_items_dynamic_query_intraday,
        #     self.ga_source_intraday)

        assert test_items_dynamic_query.endswith("FROMTEMP_EVENTS,UNNEST(ITEMS)ASITEMS;")
        # assert test_items_dynamic_query_intraday_intraday.endswith(
        #     "FROM`GCP-PROJECT.DATASET.EVENTS_INTRADAY_DATE_SHARD`,UNNEST(ITEMS)ASITEMS")

        assert "'DAILY'ASSOURCE_TABLE_TYPE" in test_items_dynamic_query
        # assert "'INTRADAY'ASSOURCE_TABLE_TYPE" in test_items_dynamic_query_intraday_intraday

        assert sample_hardcoded_items_query == test_items_dynamic_query #== test_items_dynamic_query_intraday_intraday.replace(
            # "_INTRADAY_", "_").replace("INTRADAY", "DAILY")

    def test_check_sql_query_user_properties(self):
        sample_hardcoded_user_properties_query = sample_desired_queries.sample_user_properties_query
        sample_hardcoded_user_properties_query = self.helper_clean_up_query(sample_hardcoded_user_properties_query)

        test_user_properties_dynamic_query = self.ga_source.get_user_properties_query_select_statement()
        test_user_properties_dynamic_query = self.helper_clean_up_dynamically_generated_query(
            test_user_properties_dynamic_query, self.ga_source)

        # test_user_properties_dynamic_query_intraday = self.ga_source_intraday.get_user_properties_query()
        # test_user_properties_dynamic_query_intraday = self.helper_clean_up_dynamically_generated_query(
        #     test_user_properties_dynamic_query_intraday,
        #     self.ga_source_intraday)

        assert test_user_properties_dynamic_query.endswith(
            "FROMTEMP_EVENTS,UNNEST(USER_PROPERTIES)ASUSER_PROPERTIES;")
        # assert test_user_properties_dynamic_query_intraday.endswith(
        #     "FROM`GCP-PROJECT.DATASET.EVENTS_INTRADAY_DATE_SHARD`,UNNEST(USER_PROPERTIES)ASUSER_PROPERTIES")

        assert "'DAILY'ASSOURCE_TABLE_TYPE" in test_user_properties_dynamic_query
        # assert "'INTRADAY'ASSOURCE_TABLE_TYPE" in test_user_properties_dynamic_query_intraday

        assert sample_hardcoded_user_properties_query == test_user_properties_dynamic_query# == test_user_properties_dynamic_query_intraday.replace(
            # "_INTRADAY_", "_").replace("INTRADAY", "DAILY")

    def test_get_select_statement(self):
        events = self.ga_source.get_select_statement(flat_table="flat_events")
        event_params = self.ga_source.get_select_statement(flat_table="flat_event_params")
        user_properties = self.ga_source.get_select_statement(flat_table="flat_user_properties")
        items = self.ga_source.get_select_statement(flat_table="flat_items")

        assert "event_name" in events
        assert "event_params" in event_params
        assert "user_properties" in user_properties
        assert "items" in items

    def test_get_flat_table_update_query_sharded_output_required(self):
        select_statement = self.ga_source.get_select_statement(flat_table="flat_events")
        result  =  self.ga_source.get_flat_table_update_query(select_statement=select_statement, flat_table="flat_events",
                                              sharded_output_required=True, partitioned_output_required=False)

        expected_query = f"""CREATE OR REPLACE TABLE `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_events_{self.ga_source.date_shard}`
                            AS
                            {select_statement}"""

        self.assertEqual(result.replace(" ", "").replace("\n", "").upper(), expected_query.replace(" ", "").replace("\n", "").upper() )

    def test_get_flat_table_update_query_partitioned_output_required(self):
        select_statement = self.ga_source.get_select_statement(flat_table="flat_events")
        result = self.ga_source.get_flat_table_update_query(select_statement=select_statement, flat_table="flat_events",
                                                            sharded_output_required=False,
                                                            partitioned_output_required=True)

        expected_query = f"""
        
        CREATE TABLE IF NOT EXISTS   `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_events` 
        
        PARTITION BY event_date
        
        AS {select_statement}
        
        
        DELETE FROM `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_events` WHERE event_date = PARSE_DATE('%Y%m%d','{self.ga_source.date_shard}');
                          INSERT INTO `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_events`
                          {select_statement}"""

        self.assertEqual(result.replace(" ", "").replace("\n", "").upper(),
                         expected_query.replace(" ", "").replace("\n", "").upper())

    def test_get_flat_table_update_query_sharded_and_partitioned_output_required_flat_events(self):
        select_statement = self.ga_source.get_select_statement(flat_table="flat_events")
        result = self.ga_source.get_flat_table_update_query(select_statement=select_statement, flat_table="flat_events",
                                                            sharded_output_required=True,
                                                            partitioned_output_required=True)

        expected_query = f"""
        CREATE OR REPLACE TABLE `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_events_{self.ga_source.date_shard}`
                            AS
                            {select_statement}
                            
        CREATE TABLE IF NOT EXISTS  `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_events`   
        
        PARTITION BY event_date
        
        AS {select_statement}                       
                            
        DELETE FROM `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_events` WHERE event_date = PARSE_DATE('%Y%m%d','{self.ga_source.date_shard}');
                          INSERT INTO `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_events`
                          {select_statement}"""

        expected_query_cleaned =  expected_query.replace(" ", "").replace("\n", "").upper()
        result_cleaned = result.replace(" ", "").replace("\n", "").upper()


        self.assertEqual(expected_query_cleaned, result_cleaned)

    def test_get_flat_table_update_query_sharded_and_partitioned_output_required_flat_event_params(self):
        select_statement = self.ga_source.get_select_statement(flat_table="flat_event_params")
        result = self.ga_source.get_flat_table_update_query(select_statement=select_statement, flat_table="flat_event_params",
                                                            sharded_output_required=True,
                                                            partitioned_output_required=True)

        expected_query = f"""
        CREATE OR REPLACE TABLE `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_event_params_{self.ga_source.date_shard}`
                            AS
                            {select_statement}
                            
        
        CREATE TABLE IF NOT EXISTS   `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_event_params` 
        
        PARTITION BY event_date
        
        AS {select_statement}

                                    
        DELETE FROM `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_event_params` WHERE event_date = PARSE_DATE('%Y%m%d','{self.ga_source.date_shard}');
                          INSERT INTO `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_event_params`
                          {select_statement}"""

        self.assertEqual(result.replace(" ", "").replace("\n", "").upper(),
                         expected_query.replace(" ", "").replace("\n", "").upper())



    def test_build_full_query(self):
        sharded_output_required = True
        partitioned_output_required = False

        _1 = self.ga_source.get_temp_table_query()
        _2 = self.ga_source.get_flat_table_update_query(select_statement=self.ga_source.get_select_statement(flat_table="flat_events"), flat_table="flat_events",
                                                            sharded_output_required=sharded_output_required,
                                                            partitioned_output_required=partitioned_output_required)
        _3 = self.ga_source.get_flat_table_update_query(select_statement=self.ga_source.get_select_statement(flat_table="flat_event_params"), flat_table="flat_event_params",
                                                            sharded_output_required=sharded_output_required,
                                                            partitioned_output_required=partitioned_output_required)
        _4 = self.ga_source.get_flat_table_update_query(select_statement=self.ga_source.get_select_statement(flat_table="flat_user_properties"), flat_table="flat_user_properties",
                                                            sharded_output_required=sharded_output_required,
                                                            partitioned_output_required=partitioned_output_required)
        _5 = self.ga_source.get_flat_table_update_query(select_statement=self.ga_source.get_select_statement(flat_table="flat_items"), flat_table="flat_items",
                                                            sharded_output_required=sharded_output_required,
                                                            partitioned_output_required=partitioned_output_required)


        expected_query = f"""{_1}
                            {_2}
                            {_3}
                            {_4}
                            {_5}

                            """
        result = self.ga_source.build_full_query(sharded_output_required=sharded_output_required,
                                                 partitioned_output_required=partitioned_output_required,
                                                 list_of_flat_tables=["flat_events", "flat_event_params",
                                                                      "flat_user_properties",
                                                                      "flat_items"])
        self.assertEqual(result.replace(" ", "").replace("\n", "").upper(),
                         expected_query.replace(" ", "").replace("\n", "").upper())


    def tearDown(self):
        pass
