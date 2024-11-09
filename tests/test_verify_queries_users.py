from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage
from tests.rsc import sample_desired_queries


class TestGenerateQuerySourceTableUsers(BaseUnitTest):
    c = Context()
    ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                            dataset="analytics_123456789",
                                            table_type="users",
                                            date_shard="20241107",
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
    def test_check_sql_query_users(self):
        sample_hardcoded_query = sample_desired_queries.sample_users_query
        test_dynamic_query = self.ga_source.get_users_select_statement()

        sample_hardcoded_query = self.helper_clean_up_query(sample_hardcoded_query)

        test_dynamic_query = self.helper_clean_up_dynamically_generated_query(test_dynamic_query,
                                                                              self.ga_source)

        assert test_dynamic_query.endswith(
            'FROMTEMP_USERS;')

        assert sample_hardcoded_query == test_dynamic_query

    def test_check_sql_query_users_user_properties(self):
        sample_hardcoded_query = sample_desired_queries.sample_users_user_properties_query
        test_dynamic_query = self.ga_source.get_users_user_properties_select_statement()

        sample_hardcoded_query = self.helper_clean_up_query(sample_hardcoded_query)

        test_dynamic_query = self.helper_clean_up_dynamically_generated_query(test_dynamic_query,
                                                                              self.ga_source)

        assert test_dynamic_query.endswith(
            'FROMTEMP_USERS,UNNEST(USER_PROPERTIES)UP;')

        assert sample_hardcoded_query == test_dynamic_query

    def test_check_sql_query_users_user_audiences(self):
        sample_hardcoded_query = sample_desired_queries.sample_users_user_audiences_query

        test_dynamic_query = self.ga_source.get_users_user_audiences_select_statement()

        sample_hardcoded_query = self.helper_clean_up_query(sample_hardcoded_query)

        test_dynamic_query = self.helper_clean_up_dynamically_generated_query(test_dynamic_query,
                                                                              self.ga_source)

        assert test_dynamic_query.endswith(
            'FROMTEMP_USERS,UNNEST(AUDIENCES)A;')

        assert sample_hardcoded_query == test_dynamic_query

    def test_get_select_statement(self):
        users = self.ga_source.get_select_statement(flat_table="flat_users")
        user_properties = self.ga_source.get_select_statement(flat_table="flat_users_user_properties")
        user_audiences = self.ga_source.get_select_statement(flat_table="flat_users_user_audiences")

        assert "user_id" in users and "temp_users" in users
        assert "user_properties" in user_properties and "user_id" in user_properties and "temp_users" in user_properties
        assert "audiences" in user_audiences and "user_id" in user_audiences and "temp_users" in user_audiences

    def test_get_flat_table_update_query_sharded_output_required(self):
        select_statement = self.ga_source.get_select_statement(flat_table="flat_users")
        result = self.ga_source.get_flat_table_update_query(select_statement=select_statement,
                                                            flat_table="flat_users",
                                                            sharded_output_required=True,
                                                            partitioned_output_required=False)

        expected_query = f"""CREATE OR REPLACE TABLE `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_users_{self.ga_source.date_shard}`
                            AS
                            {select_statement}"""

        self.assertEqual(result.replace(" ", "").replace("\n", "").upper(),
                         expected_query.replace(" ", "").replace("\n", "").upper())

    def test_get_flat_table_update_query_partitioned_output_required(self):
        select_statement = self.ga_source.get_select_statement(flat_table="flat_users")
        result = self.ga_source.get_flat_table_update_query(select_statement=select_statement,
                                                            flat_table="flat_users",
                                                            sharded_output_required=False,
                                                            partitioned_output_required=True)

        expected_query = f"""
        CREATE TABLE IF NOT EXISTS   `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_users` 

        PARTITION BY `date`

        AS {select_statement}      

        DELETE FROM `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_users` WHERE `date` = PARSE_DATE('%Y%m%d','{self.ga_source.date_shard}');
                          INSERT INTO `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_users`
                          {select_statement}"""

        expected_query_cleaned = expected_query.replace(" ", "").replace("\n", "").upper()
        result_cleaned = result.replace(" ", "").replace("\n", "").upper()

        self.assertEqual(expected_query_cleaned, result_cleaned)

    def test_get_flat_table_update_query_sharded_and_partitioned_output_required_flat_users(self):
        select_statement = self.ga_source.get_select_statement(flat_table="flat_users")
        result = self.ga_source.get_flat_table_update_query(select_statement=select_statement,
                                                            flat_table="flat_users",
                                                            sharded_output_required=True,
                                                            partitioned_output_required=True)

        expected_query = f"""
        CREATE OR REPLACE TABLE `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_users_{self.ga_source.date_shard}`
                            AS
                            {select_statement}

        CREATE TABLE IF NOT EXISTS   `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_users` 

        PARTITION BY `date`

        AS {select_statement}


        DELETE FROM `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_users` WHERE `date` = PARSE_DATE('%Y%m%d','{self.ga_source.date_shard}');

      INSERT INTO `{self.ga_source.gcp_project}.{self.ga_source.dataset}.flat_users`
      {select_statement}"""

        expected_query_cleaned = expected_query.replace(" ", "").replace("\n", "").upper()
        result_cleaned = result.replace(" ", "").replace("\n", "").upper()

        self.assertEqual(expected_query_cleaned, result_cleaned)

    def test_build_full_query(self):
        sharded_output_required = True
        partitioned_output_required = False

        _1 = self.ga_source.get_temp_table_query()

        _2 = self.ga_source.get_flat_table_update_query(
            select_statement=self.ga_source.get_select_statement(flat_table="flat_users"),
            flat_table="flat_users",
            sharded_output_required=sharded_output_required,
            partitioned_output_required=partitioned_output_required)

        _3 = self.ga_source.get_flat_table_update_query(
            select_statement=self.ga_source.get_select_statement(flat_table="flat_users_user_properties"),
            flat_table="flat_users_user_properties",
            sharded_output_required=sharded_output_required,
            partitioned_output_required=partitioned_output_required)

        _4 = self.ga_source.get_flat_table_update_query(
            select_statement=self.ga_source.get_select_statement(flat_table="flat_users_user_audiences"),
            flat_table="flat_users_user_audiences",
            sharded_output_required=sharded_output_required,
            partitioned_output_required=partitioned_output_required)

        expected_query = f"""{_1}
                            {_2}
                            {_3}
                            {_4}
                            """
        assert "pseudo" not in expected_query

        result = self.ga_source.build_full_query(sharded_output_required=sharded_output_required,
                                                 partitioned_output_required=partitioned_output_required,
                                                 list_of_flat_tables=["flat_users",
                                                                      "flat_users_user_properties",
                                                                      "flat_users_user_audiences"])
        self.assertEqual(result.replace(" ", "").replace("\n", "").upper(),
                         expected_query.replace(" ", "").replace("\n", "").upper())

    def tearDown(self):
        pass
