"""
USE WITH CAUTION!
deletes all flat tables from dataset
purpose: clean up after a unit test
Credit: https://stackoverflow.com/questions/52151185/bigquery-best-way-to-drop-date-sharded-tables
"""

from google.cloud import bigquery
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')

'''*****************************'''
''' Configuration Section Start '''
'''*****************************'''
my_project_id = "as-dev-ga4-flattener-320623"
my_dataset_id = 'analytics_222460912'
table_prefix = "flat_" # will perform the deletion only if the table has the desired prefix
delete = True
'''*****************************'''
'''  Configuration Section End  '''
'''*****************************'''

client = bigquery.Client(project=my_project_id)

tables = list(client.list_tables(my_dataset_id))  # API request(s), now you have the list of tables in this dataset
tables_to_delete = []
logging.info("discovered flat tables:")
for table in tables:
    if table.table_id.startswith(table_prefix):
        tables_to_delete.append(table.table_id)
        logging.info(table.full_table_id)
if delete:
    for table_id in tables_to_delete:
        table_path = f"{my_project_id}.{my_dataset_id}.{table_id}"
        client.delete_table(table_path)
        logging.info(f"deleted table {table_path}")
