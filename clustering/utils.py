from typing import Sequence
from google.cloud import bigquery
import pandas as pd
import geopandas


def get_table_from_path(
    table_name: str,
    run_locally: bool,
    path: str = "",
    create_geodataframe: bool = False,
    cols_to_select: Sequence[str] = "",
    project_name: str = "",
    dataset_name: str = "",
    project_location: str = "",
) -> pd.DataFrame:
    """Reads a table from a local file path or BigQuery table

    Parameters
    ----------
    table_name
        Name of the table to be read.
    run_locally
        Used to bypass BigQuery calls.
    path
        Location of folder for reading data locally.
    create_geodataframe
        Specifies whether to create a GeoDataFrame instead of DataFrame
    cols_to_select
        Columns to select from table in BigQuery.
    project_name
        Name of GCP project.
    dataset_name
        Name of dataset in BigQuery that table is stored in.
    project_location
        Location of GCP project.
    """
    if run_locally:
        if create_geodataframe:
            df = geopandas.read_file(f"{path}/{table_name}.shp")
        else:
            df = pd.read_csv(f"{path}/{table_name}.csv")
        if cols_to_select is not None:
            df = df.loc[:, cols_to_select]
    else:
        # Convert cols_to_select to String for use in query.
        cols_string = ",".join(map(str, cols_to_select))

        client = bigquery.Client()
        query = """SELECT {cols}
        FROM `{project}.{dataset}.{table}`
        """
        query = query.format(
            cols=cols_string,
            project=project_name,
            dataset=dataset_name,
            table=table_name,
        )
        result = client.query(query, location=project_location)

        if create_geodataframe:
            df = result.to_geodataframe()
        else:
            df = result.to_dataframe()

    return df