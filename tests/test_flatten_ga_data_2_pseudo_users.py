from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

#TODO: reduce repetition, write a function which takes date shared as param

class TestCFFlattenMethodsPseudoUsers(BaseUnitTest):
    c = Context()
    ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_type="pseudonymous_users",
                                            date_shard="20240611",
                                            )

    def tbl_exists(self, dataset, table_name):
        """
         https://stackoverflow.com/questions/28731102/bigquery-check-if-table-already-exists
         """
        client = bigquery.Client(project=self.ga_source.gcp_project)

        full_table_path = f"{self.ga_source.gcp_project}.{dataset}.{table_name}"
        table_id = bigquery.Table(full_table_path)
        try:
            client.get_table(table_id)
            return True
        except NotFound:
            return False

    def test_flatten_ga_data_sharded(self):
        sharded_output_required = True
        partitioned_output_required = False

        query = self.ga_source.build_full_query(sharded_output_required=sharded_output_required,
                                                partitioned_output_required=partitioned_output_required,
                                                list_of_flat_tables=["flat_pseudo_users", "flat_pseudo_user_properties", "flat_pseudo_user_audiences"])

        self.ga_source.run_query_job(query, wait_for_the_query_job_to_complete=True)

        assert self.tbl_exists(dataset=self.ga_source.dataset,
                               table_name=f"flat_pseudo_users_{self.ga_source.date_shard}")

        assert self.tbl_exists(dataset=self.ga_source.dataset,
                               table_name=f"flat_pseudo_user_properties_{self.ga_source.date_shard}")

        assert self.tbl_exists(dataset=self.ga_source.dataset,
                               table_name=f"flat_pseudo_user_audiences_{self.ga_source.date_shard}")

    def test_flatten_ga_data_partitioned(self):
        sharded_output_required = False
        partitioned_output_required = True

        query = self.ga_source.build_full_query(sharded_output_required=sharded_output_required,
                                                partitioned_output_required=partitioned_output_required,
                                                list_of_flat_tables=["flat_pseudo_users"])

        self.ga_source.run_query_job(query, wait_for_the_query_job_to_complete=True)

        assert self.tbl_exists(dataset=self.ga_source.dataset,
                               table_name=f"flat_pseudo_users")

class TestCFFlattenMethodsPseudoUsersSourceDoesNotExist(BaseUnitTest):
    c = Context()
    ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_type="pseudonymous_users",
                                            date_shard="20240101",
                                            )

    def tbl_exists(self, dataset, table_name):
        """
         https://stackoverflow.com/questions/28731102/bigquery-check-if-table-already-exists
         """
        client = bigquery.Client(project=self.ga_source.gcp_project)

        full_table_path = f"{self.ga_source.gcp_project}.{dataset}.{table_name}"
        table_id = bigquery.Table(full_table_path)
        try:
            client.get_table(table_id)
            return True
        except NotFound:
            return False

    def test_flatten_ga_data_sharded(self):
        sharded_output_required = True
        partitioned_output_required = False

        query = self.ga_source.build_full_query(sharded_output_required=sharded_output_required,
                                                partitioned_output_required=partitioned_output_required,
                                                list_of_flat_tables=["flat_pseudo_users",
                                                                     "flat_pseudo_user_properties",
                                                                     "flat_pseudo_user_audiences"])

        self.ga_source.run_query_job(query, wait_for_the_query_job_to_complete=True)

        assert not self.tbl_exists(dataset=self.ga_source.dataset,
                               table_name=f"flat_pseudo_users_{self.ga_source.date_shard}")

        assert not self.tbl_exists(dataset=self.ga_source.dataset,
                               table_name=f"flat_pseudo_user_properties_{self.ga_source.date_shard}")

        assert not self.tbl_exists(dataset=self.ga_source.dataset,
                               table_name=f"flat_pseudo_user_audiences_{self.ga_source.date_shard}")
    def tearDown(self):
        # self.delete_all_flat_tables_from_dataset()
        pass
