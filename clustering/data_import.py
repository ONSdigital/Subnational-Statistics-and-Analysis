#Based on https://github.com/ONSdigital/gcp-starter-pack/blob/main/notebooks/example_code/modules/data_import.py
#Adapted from covid hotspots to ons-luda-analysis-prod
#The functions used in test_storage notebook

from google.cloud import bigquery

def get_data_from_bq(sql):
    """ Gets data from BigQuery based on the given SQL, returns a dataframe"""
    client = bigquery.Client()
    return client.query(sql).to_dataframe()


def save_dataframe_to_wip(df, table_name, schema=[], write_disposition="WRITE_TRUNCATE", 
                          project="project", WIP_dataset ="wip_dataset"):
    """ Simple function to save a dataframe to the WIP dataset
    
    Wraps around the Google Cloud BigQuery Library 
    https://googleapis.dev/python/bigquery/latest/index.html
    
    Args:
        df (pandas dataframe) - the dataframe to save to BigQuery Dataset
        table_name (str) - the name of the BigQuery Table 
        
        schema (list) - list of bigquery.SchemaField types matching 
        see https://googleapis.dev/python/bigquery/latest/reference.html#schema
        
        write_disposition (str) - the write type of the table, ie, action that occurs if 
        the destination table already exists. Allowed values: WRITE_TRUNCATE, 
        WRITE_APPEND, WRITE_EMPTY
    
    Returns:
    Boolean value reflecting if the job has been successfull (True) or not (False)
    """
    # name the table you want to save the dataframe to:   
    full_table_name = f"{project}.{WIP_dataset}.{table_name}"
    
    client = bigquery.Client()

    # the config of the save job
    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=schema,
        write_disposition=write_disposition,
    )

    job = client.load_table_from_dataframe(
        df,  # the datsframe you want to save
        full_table_name,                # the table
        job_config=job_config    # the job config defined above 
    )  # Make an API request.
    try:
        job.result()  # Wait for the job to complete.
    except Exception as e:
        print("Error saving data {e}")
        return False
    return True