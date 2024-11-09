from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage
from cfintradaysqlview.main import IntradaySQLView
from tests.rsc import sample_desired_queries


# test_get_flat_table_update_query_sharded_and_partitioned_output_required_flat_event_params

class TestGenerateQuerySourceTablePseudoUsers(BaseUnitTest):
    c = Context()
    ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_type="pseudonymous_users",
                                            date_shard="20240611",
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
    def test_check_sql_query_pseudo_users(self):
        sample_hardcoded_query = sample_desired_queries.sample_pseudo_users_query
        sample_hardcoded_query = self.helper_clean_up_query(sample_hardcoded_query)

        test_dynamic_query = self.ga_source.get_pseudo_users_select_statement()
        test_dynamic_query = self.helper_clean_up_dynamically_generated_query(test_dynamic_query,
                                                                                     self.ga_source)

        assert test_dynamic_query.endswith('FROM`GCP-PROJECT.DATASET.PSEUDONYMOUS_USERS_*`WHERE_TABLE_SUFFIX="DATE_SHARD";')

        assert sample_hardcoded_query == test_dynamic_query

    def test_check_sql_query_pseudo_user_properties(self):
        sample_hardcoded_query = sample_desired_queries.sample_pseudo_user_properties_query
        sample_hardcoded_query = self.helper_clean_up_query(sample_hardcoded_query)

        test_dynamic_query = self.ga_source.get_pseudo_user_properties_select_statement()
        test_dynamic_query = self.helper_clean_up_dynamically_generated_query(test_dynamic_query,
                                                                                     self.ga_source)

        assert test_dynamic_query.endswith('FROM`GCP-PROJECT.DATASET.PSEUDONYMOUS_USERS_*`,UNNEST(USER_PROPERTIES)UPWHERE_TABLE_SUFFIX="DATE_SHARD";')

        assert sample_hardcoded_query == test_dynamic_query


    def test_check_sql_query_pseudo_user_audiences(self):
        sample_hardcoded_query = sample_desired_queries.sample_pseudo_user_audiences_query
        sample_hardcoded_query = self.helper_clean_up_query(sample_hardcoded_query)

        test_dynamic_query = self.ga_source.get_pseudo_user_audiences_select_statement()
        test_dynamic_query = self.helper_clean_up_dynamically_generated_query(test_dynamic_query,
                                                                                     self.ga_source)

        assert test_dynamic_query.endswith('FROM`GCP-PROJECT.DATASET.PSEUDONYMOUS_USERS_*`,UNNEST(AUDIENCES)AWHERE_TABLE_SUFFIX="DATE_SHARD";')

        assert sample_hardcoded_query == test_dynamic_query

    def test_get_select_statement(self):

        pseudo_users = self.ga_source.get_select_statement(flat_table="flat_pseudo_users")
        pseudo_user_properties = self.ga_source.get_select_statement(flat_table="flat_pseudo_user_properties")
        pseudo_user_audiences = self.ga_source.get_select_statement(flat_table="flat_pseudo_user_audiences")

        assert "pseudo_user_id" in pseudo_users
        assert "user_properties" in pseudo_user_properties and "pseudo_user_id" in pseudo_user_properties
        assert "audiences" in pseudo_user_audiences and "pseudo_user_id" in pseudo_users

    def test_get_flat_table_update_query_sharded_output_required(self):
        select_statement = self.ga_source.get_select_statement(flat_table="flat_pseudo_users")
        result = self.ga_source.get_flat_table_update_query(select_statement=select_statement,
                                                            flat_table="flat_pseudo_users",
                                                            sharded_output_required=True,
                                                            partitioned_output_required=False)

        expected_query = f"""CREATE OR REPLACE TABLE `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_pseudo_users_{self.ga_source.date_shard}`
                            AS
                            {select_statement}"""

        self.assertEqual(result.replace(" ", "").replace("\n", "").upper(),
                         expected_query.replace(" ", "").replace("\n", "").upper())

    def test_get_flat_table_update_query_partitioned_output_required(self):
        select_statement = self.ga_source.get_select_statement(flat_table="flat_pseudo_users")
        result = self.ga_source.get_flat_table_update_query(select_statement=select_statement,
                                                            flat_table="flat_pseudo_users",
                                                            sharded_output_required=False,
                                                            partitioned_output_required=True)

        expected_query = f"""
        CREATE TABLE IF NOT EXISTS   `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_pseudo_users` 
        
        PARTITION BY `date`
        
        AS {select_statement}      
        
        DELETE FROM `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_pseudo_users` WHERE `date` = PARSE_DATE('%Y%m%d','{self.ga_source.date_shard}');
                          INSERT INTO `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_pseudo_users`
                          {select_statement}"""

        expected_query_cleaned =  expected_query.replace(" ", "").replace("\n", "").upper()
        result_cleaned = result.replace(" ", "").replace("\n", "").upper()


        self.assertEqual(expected_query_cleaned, result_cleaned)

    def test_get_flat_table_update_query_sharded_and_partitioned_output_required_flat_pseudo_users(self):
        select_statement = self.ga_source.get_select_statement(flat_table="flat_pseudo_users")
        result = self.ga_source.get_flat_table_update_query(select_statement=select_statement,
                                                            flat_table="flat_pseudo_users",
                                                            sharded_output_required=True,
                                                            partitioned_output_required=True)

        expected_query = f"""
        CREATE OR REPLACE TABLE `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_pseudo_users_{self.ga_source.date_shard}`
                            AS
                            {select_statement}
                            
        CREATE TABLE IF NOT EXISTS   `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_pseudo_users` 
        
        PARTITION BY `date`
        
        AS {select_statement}
        
                                            
        DELETE FROM `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_pseudo_users` WHERE `date` = PARSE_DATE('%Y%m%d','{self.ga_source.date_shard}');
        
      INSERT INTO `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_pseudo_users`
      {select_statement}"""

        expected_query_cleaned =  expected_query.replace(" ", "").replace("\n", "").upper()
        result_cleaned = result.replace(" ", "").replace("\n", "").upper()


        self.assertEqual(expected_query_cleaned, result_cleaned)


    def test_build_full_query(self):
        sharded_output_required = True
        partitioned_output_required = False

        _1 = self.ga_source.get_flat_table_update_query(
            select_statement=self.ga_source.get_select_statement(flat_table="flat_pseudo_users"),
            flat_table="flat_pseudo_users",
            sharded_output_required=sharded_output_required,
            partitioned_output_required=partitioned_output_required)

        _2 = self.ga_source.get_flat_table_update_query(
            select_statement=self.ga_source.get_select_statement(flat_table="flat_pseudo_user_properties"),
            flat_table="flat_pseudo_user_properties",
            sharded_output_required=sharded_output_required,
            partitioned_output_required=partitioned_output_required)

        _3 = self.ga_source.get_flat_table_update_query(
            select_statement=self.ga_source.get_select_statement(flat_table="flat_pseudo_user_audiences"),
            flat_table="flat_pseudo_user_audiences",
            sharded_output_required=sharded_output_required,
            partitioned_output_required=partitioned_output_required)


        expected_query = f"""{_1}
                            {_2}
                            {_3}
                            """
        result = self.ga_source.build_full_query(sharded_output_required=sharded_output_required,
                                                 partitioned_output_required=partitioned_output_required,
                                                 list_of_flat_tables=["flat_pseudo_users",
                                                                      "flat_pseudo_user_properties",
                                                                      "flat_pseudo_user_audiences"])
        self.assertEqual(result.replace(" ", "").replace("\n", "").upper(),
                         expected_query.replace(" ", "").replace("\n", "").upper())

    def tearDown(self):
        pass
