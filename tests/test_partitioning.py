#TODO: do we need a more extended unit test for partitioning?
# load config file, backfill, verify inputs and outputs?
# test backfill with diff config options (sharding vs partitioning vs both)
# have done this manually and documented results


from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import logging
from http import HTTPStatus
from datetime import datetime
import pandas as pd

from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage


class TestPartitioning(BaseUnitTest):
    c = Context()
    ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_type=c.env["table_type"],
                                            date_shard=c.env["date"],
                                            )

    # SIMPLE TESTS
    def test_flatten_ga_data_config_output_type_partitioned_only(self):
        self.ga_source.run_query_job(query=self.ga_source.get_event_params_query(),
                                     table_type="flat_event_params",
                                     sharded_output_required=False,
                                     partitioned_output_required=True)

        self.ga_source.run_query_job(query=self.ga_source.get_events_query(),
                                     table_type="flat_events",
                                     sharded_output_required=False,
                                     partitioned_output_required=True)

        self.ga_source.run_query_job(query=self.ga_source.get_items_query(),
                                     table_type="flat_items",
                                     sharded_output_required=False,
                                     partitioned_output_required=True)

        self.ga_source.run_query_job(query=self.ga_source.get_user_properties_query(),
                                     table_type="flat_user_properties",
                                     sharded_output_required=False,
                                     partitioned_output_required=True)

        self.assertTrue(True)

    def test_flatten_ga_data_config_output_type_sharded_and_partitioned(self):
        self.ga_source.run_query_job(query=self.ga_source.get_event_params_query(),
                                     table_type="flat_event_params",
                                     sharded_output_required=True,
                                     partitioned_output_required=True)

        self.ga_source.run_query_job(query=self.ga_source.get_events_query(),
                                     table_type="flat_events",
                                     sharded_output_required=True,
                                     partitioned_output_required=True)

        self.ga_source.run_query_job(query=self.ga_source.get_items_query(),
                                     table_type="flat_items",
                                     sharded_output_required=True,
                                     partitioned_output_required=True)

        self.ga_source.run_query_job(query=self.ga_source.get_user_properties_query(),
                                     table_type="flat_user_properties",
                                     sharded_output_required=True,
                                     partitioned_output_required=True)

        self.assertTrue(True)

    # INTEGRATED TESTS
    # check output schema
    def test_flatten_ga_data_check_output_schema_event_params(self):
        self.flatten_ga_data_check_output_schema(table_type="flat_event_params")

    def test_flatten_ga_data_check_output_schema_events(self):
        self.flatten_ga_data_check_output_schema(table_type="flat_events")

    def test_flatten_ga_data_check_output_schema_items(self):
        self.flatten_ga_data_check_output_schema(table_type="flat_items")

    def test_flatten_ga_data_check_output_schema_user_properties(self):
        self.flatten_ga_data_check_output_schema(table_type="flat_user_properties")

    # check output row numbers and dates
    def test_flatten_ga_data_check_number_of_rows_event_params(self):
        self.flatten_ga_data_check_number_of_rows(table_type="flat_event_params")

    def test_flatten_ga_data_check_number_of_rows_events(self):
        self.flatten_ga_data_check_number_of_rows(table_type="flat_events")

    def test_flatten_ga_data_check_number_of_rows_items(self):
        self.flatten_ga_data_check_number_of_rows(table_type="flat_items")

    def test_flatten_ga_data_check_number_of_rows_user_properties(self):
        self.flatten_ga_data_check_number_of_rows(table_type="flat_user_properties")

    # HELPER FUNCTIONS
    def drop_partitioned_table(self, table_type="flat_events"):

        client = bigquery.Client(project=self.ga_source.gcp_project)

        table_path = f"{self.ga_source.gcp_project}.{self.ga_source.dataset}.{table_type}"

        try:
            client.delete_table(table_path)
        except Exception as e:
            if e.code == HTTPStatus.NOT_FOUND:  # 404 Not found
                logging.warning(f"Cannot delete the partition because the table doesn't exist yet: {e}")

    def flatten_ga_data_check_output_schema(self, table_type="flat_events"):
        """
        verify
            partitioning
            event_date col is a DATE, not TIMESTAMP
            schemas match in sharded and partitioned table
            number of rows is correct
            idempotence of append operation
        """
        client = bigquery.Client(project=self.ga_source.gcp_project)
        # drop partitioned table if it exists
        self.drop_partitioned_table(table_type=table_type)

        # we need to do it, because we need to check number of rows later
        table_name_partitioned = f"{self.ga_source.gcp_project}.{self.ga_source.dataset}.{table_type}"
        table_id_partitioned = bigquery.Table(table_name_partitioned)

        # flatten data
        self.flatten_ga_data(table_type=table_type)

        # extract info about partitioned table
        table_partitioned = client.get_table(table_id_partitioned)  # Make an API request.

        # verify partitioning
        self.assertIsNotNone(table_partitioned.time_partitioning)
        self.assertEqual("DAY", table_partitioned.time_partitioning.type_)
        self.assertEqual("event_date", table_partitioned.time_partitioning._properties['field'])

        # verify date field
        self.assertEqual(SchemaField('event_date', 'DATE', 'NULLABLE', None, None, (), None), table_partitioned.schema[0])

        # extract info about sharded output
        table_name_sharded = f"{self.ga_source.gcp_project}.{self.ga_source.dataset}.{table_type}_{self.ga_source.date_shard}"

        table_id_sharded = bigquery.Table(table_name_sharded)
        table_sharded = client.get_table(table_id_sharded)

        # remove event_date, because we know this field type won't match in sharded vs partitioned table
        # in sharded table, it's string, in partitioned table, it's date

        schema_table_sharded = [field for field in table_sharded.schema if field.name != "event_date"]
        schema_table_partitioned = [field for field in table_partitioned.schema if field.name != "event_date"]

        # verify schema
        self.assertEqual(schema_table_sharded, schema_table_partitioned)

        # verify number of rows
        self.assertEqual(table_sharded.num_rows, table_partitioned.num_rows)

        # verify idempotence of operation
        # (partition gets overwritten, we don't get dupes as a result of repeated update to the same partition)
        # flatten data
        self.flatten_ga_data(table_type=table_type)

        # verify number of rows again
        self.assertEqual(table_sharded.num_rows, table_partitioned.num_rows)

    def flatten_ga_data(self, table_type="flat_events"):

        # flatten data
        # we will wait for the job to complete, or else the unit test fails
        # (we need to wait for the correct output to be produced before we verify/check the output)
        if table_type == "flat_event_params":
            self.ga_source.run_query_job(query=self.ga_source.get_event_params_query(),
                                         table_type=table_type,
                                         sharded_output_required=True,
                                         partitioned_output_required=True,
                                         wait_for_the_query_job_to_complete=True)

        elif table_type == "flat_events":
            self.ga_source.run_query_job(query=self.ga_source.get_events_query(),
                                         table_type=table_type,
                                         sharded_output_required=True,
                                         partitioned_output_required=True,
                                         wait_for_the_query_job_to_complete=True)

        elif table_type == "flat_items":
            self.ga_source.run_query_job(query=self.ga_source.get_items_query(),
                                         table_type=table_type,
                                         sharded_output_required=True,
                                         partitioned_output_required=True,
                                         wait_for_the_query_job_to_complete=True)

        elif table_type == "flat_user_properties":
            self.ga_source.run_query_job(query=self.ga_source.get_user_properties_query(),
                                         table_type=table_type,
                                         sharded_output_required=True,
                                         partitioned_output_required=True,
                                         wait_for_the_query_job_to_complete=True)

    def flatten_ga_data_check_number_of_rows(self, dates_list=["20211201", "20211202"],
                                             table_type="flat_events"):

        """verify that the append operation works correctly.

        Check row numbers and dates.

            if we sync two date shards and two partitions:
                - partitioned table number of rows is equal to sharded tables total number of rows
                - breakdown of rows by date is the same in sharded vs partitioned data
        """
        client = bigquery.Client(project=self.ga_source.gcp_project)
        # drop partitioned table if it exists
        # we need to do it, because we need to check number of rows later
        # drop partitioned table if it exists
        self.drop_partitioned_table(table_type=table_type)

        self.assertEqual(2, len(dates_list))

        assert dates_list[0] < dates_list[1]

        num_rows_sharded = 0

        for date in dates_list:
            # reset the date
            self.ga_source.date_shard = date
            self.ga_source.date = datetime.strptime(date, '%Y%m%d')

            # flatten data
            self.flatten_ga_data(table_type=table_type)

            # keep track of number of sharded rows
            table_name_sharded = f"{self.ga_source.gcp_project}.{self.ga_source.dataset}.{table_type}_{self.ga_source.date_shard}"
            table_id_sharded = bigquery.Table(table_name_sharded)
            table_sharded = client.get_table(table_id_sharded)
            num_rows_sharded = num_rows_sharded + table_sharded.num_rows

        # count partitioned rows
        table_name_partitioned = f"{self.ga_source.gcp_project}.{self.ga_source.dataset}.{table_type}"
        table_id_partitioned = bigquery.Table(table_name_partitioned)
        table_partitioned = client.get_table(table_id_partitioned)
        num_rows_partitioned = table_partitioned.num_rows

        self.assertEqual(num_rows_sharded, num_rows_partitioned)

        # make sure that breakdown of rows by date is the same in sharded vs partitioned data
        query_string_partitioned = f"""
            SELECT event_date, count(*) nrow FROM `{self.ga_source.gcp_project}.{self.ga_source.dataset}.{table_type}`
            GROUP BY 1
            ORDER BY 1 
        """

        dataframe_partitioned = (
            client.query(query_string_partitioned)
                .result()
                .to_dataframe(
            )
        )

        query_string_sharded = f"""
            SELECT event_date, count(*) nrow
            FROM `{self.ga_source.gcp_project}.{self.ga_source.dataset}.{table_type}_*`
            WHERE _TABLE_SUFFIX BETWEEN "{dates_list[0]}" AND "{dates_list[1]}"
            GROUP BY 1
            ORDER BY 1
        """

        dataframe_sharded = (
            client.query(query_string_sharded)
                .result()
                .to_dataframe(
            )
        )

        assert dataframe_sharded.equals(dataframe_partitioned)


    def tearDown(self):
        self.delete_all_flat_tables_from_dataset()