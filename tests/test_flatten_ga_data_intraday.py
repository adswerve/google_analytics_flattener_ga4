from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class TestCFFlattenMethodsIntraday(BaseUnitTest):
    c = Context()
    ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_type=c.env["table_type_intraday"],
                                            date_shard=c.env["date_intraday"],
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

    def test_flatten_ga_data_check_output_flat_event_params(self):
        self.ga_source.run_query_job(query=self.ga_source.get_event_params_query(),
                                     table_type="flat_event_params",
                                     wait_for_the_query_job_to_complete=True)

        assert self.tbl_exists(dataset=self.ga_source.dataset,
                               table_name=f"flat_event_params_{self.ga_source.date_shard}")

    def test_flatten_ga_data_check_output_flat_events(self):
        self.ga_source.run_query_job(query=self.ga_source.get_events_query(),
                                     table_type="flat_events",
                                     wait_for_the_query_job_to_complete=True)

        assert self.tbl_exists(dataset=self.ga_source.dataset,
                               table_name=f"flat_events_{self.ga_source.date_shard}")

    def test_flatten_ga_data_check_output_flat_items(self):
        self.ga_source.run_query_job(query=self.ga_source.get_items_query(),
                                     table_type="flat_items",
                                     wait_for_the_query_job_to_complete=True)

        assert self.tbl_exists(dataset=self.ga_source.dataset,
                               table_name=f"flat_items_{self.ga_source.date_shard}")

    def test_flatten_ga_data_check_output_flat_user_properties(self):
        self.ga_source.run_query_job(query=self.ga_source.get_user_properties_query(),
                                     table_type="flat_user_properties",
                                     wait_for_the_query_job_to_complete=True)

        assert self.tbl_exists(dataset=self.ga_source.dataset,
                               table_name=f"flat_user_properties_{self.ga_source.date_shard}")

    def tearDown(self):
        self.delete_all_flat_tables_from_dataset()
