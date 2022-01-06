# TODO: verify number of rows when you load 2 dates

# TODO: do we need a more extended unit test for partitioning?
# load config file, backfill, verify inputs and outputs?
# test backfill with diff config options (sharding vs partitioning vs both)

# TODO: test with the intraday feature


from tests.test_base import BaseUnitTest
from tests.test_base import Context
from cf.main import GaExportedNestedDataStorage
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import logging
from http import HTTPStatus
from datetime import datetime


class TestPartitioning(BaseUnitTest):
    c = Context()
    ga_source = GaExportedNestedDataStorage(gcp_project=c.env["project"],
                                            dataset=c.env["dataset"],
                                            table_name=c.env["table_type"],
                                            date_shard=c.env["date"],
                                            )

    # initialize BigQuery client
    client = bigquery.Client()

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

    def test_flatten_ga_data_check_output_schema(self, table_type="flat_events"):
        """
        verify
            partitioning
            event_date col is a DATE, not TIMESTAMP
            schemas match in sharded and partitioned table
            number of rows is correct
            idempotence of append operation
        """

        # drop partitioned table if it exists
        # we need to do it, because we need to check number of rows later
        table_name_partitioned = "{p}.{ds}.{t}" \
            .format(p=self.ga_source.gcp_project, ds=self.ga_source.dataset, t=table_type)
        table_id_partitioned = bigquery.Table(table_name_partitioned)

        table_ref = self.client.dataset(self.ga_source.dataset).table(table_type)

        try:
            self.client.delete_table(table_ref)
        except Exception as e:
            if e.code == HTTPStatus.NOT_FOUND:  # 404 Not found
                logging.warning(f"Cannot delete the partition because the table doesn't exist yet: {e}")

        # flatten data
        self.ga_source.run_query_job(query=self.ga_source.get_events_query(), table_type=table_type,
                                     sharded_output_required=True, partitioned_output_required=True)

        # extract info about partitioned table
        table_partitioned = self.client.get_table(table_id_partitioned)  # Make an API request.

        # verify partitioning
        self.assertIsNotNone(table_partitioned.partitioning_type)
        self.assertEqual("DAY", table_partitioned.time_partitioning._properties['type'])
        self.assertEqual("event_date", table_partitioned.time_partitioning._properties['field'])

        # verify date field
        self.assertEqual(SchemaField('event_date', 'DATE', 'NULLABLE', None, (), None), table_partitioned.schema[0])

        # extract info about sharded output
        table_name_sharded = "{p}.{ds}.{t}_{d}" \
            .format(p=self.ga_source.gcp_project, ds=self.ga_source.dataset, t=table_type, d=self.ga_source.date_shard)

        table_id_sharded = bigquery.Table(table_name_sharded)
        table_sharded = self.client.get_table(table_id_sharded)

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
        self.ga_source.run_query_job(query=self.ga_source.get_events_query(), table_type=table_type,
                                     sharded_output_required=False, partitioned_output_required=True)

        # verify number of rows again
        self.assertEqual(table_sharded.num_rows, table_partitioned.num_rows)

    def test_flatten_ga_data_confirm_number_of_rows(self, dates_list=["20211201", "20211202"],
                                                    table_type="flat_events"):

        """verify that the append operation works correctly
            if we sync two date shards and two partitions, partitioned table number of rows is equal to
            sharded tables total number of rows
        """

        num_rows_sharded = 0

        for date in dates_list:
            # reset the date
            self.ga_source.date_shard = date
            self.ga_source.date = datetime.strptime(date, '%Y%m%d')

            # flatten data
            self.ga_source.run_query_job(query=self.ga_source.get_events_query(), table_type=table_type,
                                         sharded_output_required=True, partitioned_output_required=True)

            # keep track of number of sharded rows
            table_name_sharded = "{p}.{ds}.{t}_{d}" \
                .format(p=self.ga_source.gcp_project, ds=self.ga_source.dataset, t=table_type,
                        d=self.ga_source.date_shard)

            table_id_sharded = bigquery.Table(table_name_sharded)
            table_sharded = self.client.get_table(table_id_sharded)
            num_rows_sharded = num_rows_sharded + table_sharded.num_rows

        # count partitioned rows
        table_name_partitioned = "{p}.{ds}.{t}" \
            .format(p=self.ga_source.gcp_project, ds=self.ga_source.dataset, t=table_type)
        table_id_partitioned = bigquery.Table(table_name_partitioned)
        table_partitioned = self.client.get_table(table_id_partitioned)
        num_rows_partitioned = table_partitioned.num_rows

        self.assertEqual(num_rows_sharded, num_rows_partitioned)

