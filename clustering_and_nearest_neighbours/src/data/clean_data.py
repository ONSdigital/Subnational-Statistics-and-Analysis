from typing import Dict, Sequence
import pandas as pd
import numpy as np
from src.utils.utils import get_table_from_path


def get_code_column(
    df: pd.DataFrame,
    flag: str = 'E0',
) -> str:
    """Gets codes that start with the substring specified.

    Parameters
    ----------
    df
        Contains the loaded data.
    flag
        Substring to filter area codes by.

    Returns
    -------
    Name of column with matching substring.
    """
    #Note use of groupby here as there was a deprecation warning for all(level=1), 
    #suggesting groupby(level=1).all() is safer.
    #But beware that default behaviour of groupby is to sort alphabetically, which we very much don't want!
    col = pd.Index(["AREACD"])
    for column in df.columns:
        if df.loc[df[column].astype(str).str.contains(flag), column].any():
            col = pd.Index([column])
    if len(col) == 0:
        print("ERROR: No columns that look like the contain geography codes found. Checking if the flag entered matches the expected pattern")

    return col


def number_areas(
    df: pd.DataFrame,
    flag: str = 'E0',
) -> int:
    """Gets the number of unique areas in the area code column.

    Parameters
    ----------
    df
        Contains the loaded data.
    flag
        Substring to filter area codes by.

    Returns
    -------
    Number of unique area codes.
    """
    area_col = get_code_column(df, flag)
    if len(area_col) == 0:
        return 0
    areas = df[area_col].drop_duplicates()
    return len(areas)


def get_all_upper_tier_la(
    upper_to_lower_tier_lookup: pd.DataFrame,
    upper_tier_col: str,
) -> pd.Series:
    """Gets a series of all upper tier local authorities from the lookup data.

    Parameters
    ----------
    upper_to_lower_tier_lookup
        Contains the upper to lower tier lookup data.
    upper_tier_col
        Name of column containing upper tier local authority codes.

    Returns
    -------
    Series containing upper-tier local authority codes.
    """
    df = upper_to_lower_tier_lookup.loc[:, upper_tier_col].drop_duplicates()
    return df


def UT_metric_to_LT(
    metric: pd.DataFrame,
    upper_to_lower_tier_lookup: pd.DataFrame,
    upper_tier_col: str,
    lower_tier_col: str,
    loaded_config: Dict,
) -> pd.DataFrame:
    """Converts upper tier local authorities to lower tier for a given metric.

    Parameters
    ----------
    metric
        Contains the data for a given metric.
    upper_to_lower_tier_lookup
        Contains the upper to lower tier lookup data.
    upper_tier_col
        Name of column containing upper tier local authority codes.
    lower_tier_col
        Name of column containing lower tier local authority codes.

    Returns
    -------
    DataFrame with lower tier local authorities.
    """
    upper_tier_col= loaded_config["upper_tier_code_column_name"]
    lower_tier_col= loaded_config["lower_tier_code_column_name"]
    
    #Se
    UT_list = upper_to_lower_tier_lookup[upper_tier_col].tolist()
    UT_metrics_to_join = metric[metric['AREACD'].isin(UT_list)]
    UT_metrics_to_join.reset_index()
    upper_to_lower_tier_lookup = upper_to_lower_tier_lookup.rename(columns={upper_tier_col: 'AREACD'})
    UT_metric_join = upper_to_lower_tier_lookup.merge(UT_metrics_to_join, on='AREACD', how='left')
    UT_metric = UT_metric_join
    UT_metric["AREANM"] = ""
    UT_metric = UT_metric[[lower_tier_col,"AREANM","Indicator", "Period", "Measure", "Unit", "Value"]]
    UT_metric = UT_metric.rename(columns={lower_tier_col: 'AREACD'})
    metric = metric.dropna(subset=['Value'])
    metric = metric[["AREACD", "AREANM","Indicator", "Period", "Measure", "Unit", "Value"]]
    UT_metric = UT_metric[~UT_metric['AREACD'].isin(metric['AREACD'])]
    full_metric = pd.concat([metric, UT_metric],ignore_index=True)

    
    return full_metric


def drop_index_column(
    df: pd.DataFrame,
    col_to_drop: str = 'index',
) -> pd.DataFrame:
    """Drops the specified column from data.

    Parameters
    ----------
    df
        Contains the loaded data.
    col_to_drop
        Name of column to remove.

    Returns
    -------
    DataFrame with column removed.
    """
    if col_to_drop in df.columns:
        df = df.drop(columns=col_to_drop)

    return df


def harmonise_area_col_name(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Renames area code column to AREACD and puts at front of DataFrame.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    Formatted DataFrame.
    """
    area_col=get_code_column(df)[0]
    col = df.pop(area_col)
    df.insert(0, "AREACD", col)

    return df


def ensure_value_numeric(
    df: pd.DataFrame,
    value_col: str = "Value"
):
    """Changes value column to numeric type.

    Parameters
    ----------
    df
        Contains the loaded data.
    value_col
        Name of column containing values to be made numeric.

    Returns
    -------
    DataFrame with specified column changed to numeric type.
    """
    if not np.issubdtype(df[value_col].dtypes, np.number):
        df.loc[:, value_col] = pd.to_numeric(df[value_col], errors='coerce')

    return df


def all_cleaning(
    loaded_config: Dict,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Cleans data for a given metric.

    Parameters
    ----------
    loaded_config
        Contains the loaded config.
    df
        Contains the loaded data.

    Returns
    -------
    DataFrame with cleaned data.
    """
    ut_to_lt_lookup = get_table_from_path(
        table_name=loaded_config["upper_tier_to_lower_tier_lookup"],
        path=loaded_config["inputs_file_path"],
        create_geodataframe=False,
        cols_to_select=[loaded_config["upper_tier_code_column_name"], loaded_config["lower_tier_code_column_name"]],
    )
    #all unique area codes and names
    df_cdlm = df[["AREACD", "AREANM"]]
    df = UT_metric_to_LT(
         metric = df,
         upper_to_lower_tier_lookup=ut_to_lt_lookup,
         loaded_config=loaded_config,
         upper_tier_col= loaded_config["upper_tier_code_column_name"],
         lower_tier_col= loaded_config["lower_tier_code_column_name"],
    )
    df = drop_index_column(df)
    df = drop_index_column(df, col_to_drop='YEAR')
    df = harmonise_area_col_name(df)
    df = ensure_value_numeric(df)

    
    return df

def clean_groups(
    loaded_config: Dict,
    group: Sequence[pd.DataFrame],
) -> Sequence[pd.DataFrame]:
    """Cleans multiple metrics.

    Parameters
    ----------
    loaded_config
        Contains the loaded config.
    group
        List of metric DataFrames.

    Returns
    -------
    sequence of cleaned metric DataFrames.
    """

    for metric in range(len(group)):
            group[metric] = all_cleaning(loaded_config, group[metric])
    
    return group



def get_desired_geography(
    loaded_config: Dict,
    df: pd.DataFrame,
    desired_geography: str,
    geography_col: str,
) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    loaded_config : Dict
        saved config containing model parameters.
    df : pd.DataFrame
        data frame containing all of the metrics.
    geography_col : str
        The name of the desired geography column in the lookup.

    Returns
    -------
    df : pd.dataframe
        A dataframe with all of the metrics at desired geography.

    """
    lookup = get_table_from_path(
        table_name=loaded_config["Geog_mapper"],
        path=loaded_config["inputs_file_path"],
        create_geodataframe=False,
        cols_to_select=[desired_geography],
    )
    lookup = lookup[[desired_geography]]
    lookup = lookup.rename(columns={desired_geography: 'AREACD'})
    df = lookup.merge(df, right_on = 'AREACD', left_on = geography_col, how='left')
    df = df.rename(columns={geography_col: 'AREACD'})
    return df
    

def metrics_to_table(
        metrics: Sequence[pd.DataFrame]
) -> pd.DataFrame:
    """Puts metrics into one pivot table.

    Parameters
    ----------
    metrics
        List of DataFrames, each containing a metric.

    Returns
    -------
    metrics: pd.dataframe
    DataFrame containing all metrics.
    """
    if len(metrics) > 0:
        metrics = pd.concat(metrics)
        metrics = pd.pivot_table(metrics, values="Value", columns="Indicator", index="AREACD")
    return metrics
