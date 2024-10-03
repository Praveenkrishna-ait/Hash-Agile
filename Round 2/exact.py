from elasticsearch import Elasticsearch

# Create an Elasticsearch client
es = Elasticsearch()

def createCollection(p_collection_name):
    """
    Create a new collection in Elasticsearch
    """
    es.indices.create(index=p_collection_name, ignore=400)

def indexData(p_collection_name, p_exclude_column):
    """
    Index employee data into the specified collection, excluding the column provided
    """
    # Assume we have a sample employee data in a pandas dataframe
    df = pd.DataFrame({
        'EmployeeID': ['E02001', 'E02002', 'E02003', 'E02004'],
        'Name': ['John Doe', 'Jane Doe', 'Bob Smith', 'Alice Johnson'],
        'Department': ['IT', 'HR', 'IT', 'Finance'],
        'Gender': ['Male', 'Female', 'Male', 'Female'],
        'Phone': ['123-456-7890', '098-765-4321', '555-123-4567', '555-901-2345']
    })
    
    # Exclude the specified column
    df.drop(p_exclude_column, axis=1, inplace=True)
    
    # Convert the dataframe to a dictionary
    data_dict = df.to_dict(orient='records')
    
    # Index the data into the collection
    for data in data_dict:
        es.index(index=p_collection_name, body=data)

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    """
    Search within the specified collection for records where the column matches the value
    """
    query = {
        'query': {
            'match': {p_column_name: p_column_value}
        }
    }
    result = es.search(index=p_collection_name, body=query)
    return result['hits']['hits']

def getEmpCount(p_collection_name):
    """
    Retrieve the count of employees in the specified collection
    """
    result = es.count(index=p_collection_name)
    return result['count']

def delEmpById(p_collection_name, p_employee_id):
    """
    Delete an employee by ID from the specified collection
    """
    es.delete(index=p_collection_name, id=p_employee_id)

def getDepFacet(p_collection_name):
    """
    Retrieve the count of employees grouped by department from the specified collection
    """
    query = {
        'aggs': {
            'department_facet': {
                'terms': {'field': 'Department'}
            }
        }
    }
    result = es.search(index=p_collection_name, body=query)
    return result['aggregations']['department_facet']['buckets']

# Function executions
v_nameCollection = 'Hash_Praveen'
v_phoneCollection = 'Hash_1234'

createCollection(v_nameCollection)
createCollection(v_phoneCollection)

getEmpCount(v_nameCollection)
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')

delEmpById(v_nameCollection, 'E02003')
getEmpCount(v_nameCollection)

searchByColumn(v_nameCollection, 'Department', 'IT')
searchByColumn(v_nameCollection, 'Gender', 'Male')
searchByColumn(v_phoneCollection, 'Department', 'IT')

getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)