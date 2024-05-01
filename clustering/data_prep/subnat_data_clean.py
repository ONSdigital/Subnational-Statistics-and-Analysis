from typing import Dict, Sequence

import pandas as pd
import numpy as np
import re

from utils import get_table_from_path


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
    #Note use of groupby here as there was a deprecation warning for all(level=1), suggesting groupby(level=1).all() is safer.
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
    lower_metrics = []
    area_col = get_code_column(metric)
    all_UT = get_all_upper_tier_la(upper_to_lower_tier_lookup, upper_tier_col)    
    for row in range(len(metric)):
        #Test to see if this a upper tier LA. If it is, replace it with lower tier.
        if metric.iloc[row].loc[area_col][0] in list(all_UT):
            lower_metric = metric.reset_index().truncate(row,row).merge(upper_to_lower_tier_lookup,
                                                                        left_on=area_col[0], right_on=upper_tier_col,how="left")
            lower_metric = lower_metric.drop(columns=[area_col[0], upper_tier_col])
            col = lower_metric.pop(lower_tier_col)
            lower_metric.insert(1, area_col[0], col)
            lower_metrics.append(lower_metric)
        else:
            lower_metric = metric.reset_index().truncate(row,row).merge(upper_to_lower_tier_lookup, 
                                                                        left_on=area_col[0], right_on=upper_tier_col, how="left")
            lower_metric = lower_metric.drop(columns=[lower_tier_col, upper_tier_col])
            lower_metrics.append(lower_metric)
    
    return pd.concat(lower_metrics)


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
        run_locally=loaded_config["run_locally"],
        path=loaded_config["local_file_path"],
        create_geodataframe=False,
        cols_to_select=[loaded_config["upper_tier_code_column_name"], loaded_config["lower_tier_code_column_name"]],
        project_name=loaded_config["gcp_project_name"],
        dataset_name="ingest_luda",
        project_location=loaded_config["gcp_project_location"],
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
  #Separates out empty values, imputes and then appends to dataset
    df_missing_values=df.loc[df["Value"].isna(), ]
    df_missing_values=df_missing_values.drop("Value", axis = 1)
    df_missing_values=df_missing_values.merge(df[["AREACD", "Value"]], on = "AREACD", how = "left")
    df_missing_values=df_missing_values.loc[df_missing_values["Value"].notna(),]
    df=pd.concat([df, df_missing_values])
    df=df.merge(df_cdlm,on = ["AREACD", "AREANM"], how = "inner")
    df=df.loc[df["Value"].notna(), ]

    
    return df

def hle_cleaning(
    loaded_config: Dict,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Cleans data for hle metrics(special case compared to the others).

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
        run_locally=loaded_config["run_locally"],
        path=loaded_config["local_file_path"],
        create_geodataframe=False,
        cols_to_select=[loaded_config["upper_tier_code_column_name"], loaded_config["lower_tier_code_column_name"]],
        project_name=loaded_config["gcp_project_name"],
        dataset_name="ingest_luda",
        project_location=loaded_config["gcp_project_location"],
    )
    df = UT_metric_to_LT(
        df,
        upper_to_lower_tier_lookup=ut_to_lt_lookup,
        upper_tier_col=loaded_config["upper_tier_code_column_name"],
        lower_tier_col=loaded_config["lower_tier_code_column_name"],
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
        if metric == "Male healthy life expectancy" or metric == "Female healthy life expectancy":
            group[metric] = hle_cleaning(loaded_config, group[metric])
        else:
            group[metric] = all_cleaning(loaded_config, group[metric])
        

    return group


def winsorze(
    df: pd.DataFrame,
    lower_threshold: float,
    upper_threshold: float,
) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    df : pd.DataFrame
        dataframe containing raw metrics.
    lower_threshold : float
        percentile threshold for lower bound in 0-1 format.
    upper_threshold : float
        percentile threshold for upper bound in 0-1 format.

    Returns
    -------
    df : pd.dataframe
        dataframe of winsorized data.

    """
    df = df.set_index("AREACD")
    df = df.clip(lower=df.quantile(lower_threshold), upper=df.quantile(upper_threshold), axis=1)
    df = df.reset_index()
    return df
    

def get_winsorization_thresholds(
    df: pd.DataFrame,
    lower_threshold: float,
    upper_threshold: float,
) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    df : pd.DataFrame
        dataframe containing raw metrics.
    lower_threshold : float
        percentile threshold for lower bound in 0-1 format.
    upper_threshold : float
        percentile threshold for upper bound in 0-1 format.

    Returns
    -------
    df_thresholds : pd.dataframe
        Dataframe of the winsorization thresholds.

    """
    df = df.set_index("AREACD")
    df_thresholds = df.quantile([lower_threshold, upper_threshold])
    df_thresholds = df_thresholds.reset_index()
    return df_thresholds

def get_desired_geography(
    loaded_config: Dict,
    df: pd.DataFrame,
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
        run_locally=loaded_config["run_locally"],
        path=loaded_config["local_file_path"],
        create_geodataframe=False,
        cols_to_select=[geography_col],
        project_name=loaded_config["gcp_project_name"],
        dataset_name=loaded_config["Geog_mapper"],
        project_location=loaded_config["gcp_project_location"],
    )
    df = lookup.merge(df, right_on = 'AREACD', left_on = geography_col, how='left')
    df = df.rename(columns={geography_col: 'AREACD'})
    return df
    
def get_correlation_matrix(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    df : pd.DataFrame
        The data to be fed into the clustering model.

    Returns
    -------
    df_corr : pd.dataframe
        A correlation matrix for all selected variables

    """
    df = df.set_index("AREACD")
    df_corr = df.corr()
    return df_corr


