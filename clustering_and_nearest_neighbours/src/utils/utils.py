from typing import Sequence
import pandas as pd
import geopandas


def get_table_from_path(
    table_name: str,
    path: str = "",
    create_geodataframe: bool = False,
    cols_to_select: Sequence[str] = "",
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
    
    if create_geodataframe:
        df = geopandas.read_file(f"{path}/{table_name}.shp")
    else:
        df = pd.read_csv(f"{path}/{table_name}.csv")
    if cols_to_select is not None:
        df = df.loc[:, cols_to_select]

    return df