"""deletes all flat tables from dataset
purpose: clean up after a unit test
USE WITH CAUTION
Credit: https://stackoverflow.com/questions/52151185/bigquery-best-way-to-drop-date-sharded-tables
"""


from google.cloud import bigquery

'''*****************************'''
''' Configuration Section Start '''
'''*****************************'''
my_project_id = 'as-dev-ga4-flattener-320623'
my_dataset_id = 'analytics_222460912'
delete = True
'''*****************************'''
'''  Configuration Section End  '''
'''*****************************'''


client = bigquery.Client(project=my_project_id)

dataset_ref = client.dataset(my_dataset_id)

tables = list(client.list_tables(dataset_ref))  # API request(s), now you have the list of tables in this dataset
tables_to_delete=[]
for table in tables:
    if table.table_id.startswith("flat_"): #will perform the action only if the table has the desired prefix
        tables_to_delete.append(table.table_id)
        print(table.full_table_id)

if delete:
    for table_id in tables_to_delete:
        table_ref = client.dataset(my_dataset_id).table(table_id)
        client.delete_table(table_ref)
        print("deleted table", table.full_table_id)