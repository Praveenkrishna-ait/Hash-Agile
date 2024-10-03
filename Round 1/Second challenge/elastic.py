import pandas as pd
from elasticsearch import Elasticsearch

dataset_url = 'https://www.kaggle.com/datasets/williamlucas0/employee-sample-data'
es_host = '127.0.0.1'
es_port = 9300

es = Elasticsearch(hosts=[{'host': es_host, 'port': es_port}])

df = pd.read_csv(dataset_url)

data_dict = df.to_dict(orient='records')

index_name = 'employee'
es.indices.create(index=index_name, ignore=400)

mapping = {
    'properties': {
        'Employee_ID': {'type': 'text'},
        'Full Name': {'type': 'text'},
        'Job Title': {'type': 'text'},
        'Department': {'type': 'text'},
        'Business Unit': {'type': 'text'},
        'Gender': {'type': 'text'},
        'Ethinicity': {'type': 'text'},
        'Age': {'type': 'integer'},
        'Hire Date': {'type': 'text'},
        'Annual Salary': {'type': 'text'},
    }
}

es.indices.put_mapping(index=index_name, body=mapping)

for data in data_dict:
    es.index(index=index_name, body=data)
