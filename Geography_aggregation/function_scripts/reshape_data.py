import pandas as pd
from typing import Dict

def unstack_multiple_values(
    df:pd.DataFrame,
    loaded_config: Dict, 
    value_cols,
    area_col:str = "AREACD",
    )->pd.DataFrame:
    """
    
    Parameters
    ----------
    df : pd.DataFrame
        data frame to be unstacked into wide formats.
    loaded_config : Dict
        loaded config including file paths and field names
    value_cols : List
        value columns that need to be preserved in the unstacking.
    area_col : str, optional
        area code column of the data. The default is "AREACD".

    Returns
    -------
    pd.DataFrame unstacked from long to wide.

    """
    #get information from config
    stacked_col = loaded_config["keep_variable"]
    keep_categories = sorted(set(df[stacked_col]))
    
    #clean df
    df = df.rename(columns={"area_col": "AREACD"})
    df = df.dropna(subset = ["AREACD"])
    
    #pivot the data from long to wide
    df = df.pivot_table(index="AREACD", columns = stacked_col, values = value_cols)
    df.columns = [".".join(map(str, col)).strip() for col in df.columns.values]
    df.reset_index(inplace = True)
    
    ordered_columns = ["AREACD"]
    
    #create column names
    for keep_category in keep_categories:
        for col in value_cols:
            ordered_columns.extend([f"{col}.{keep_category}"])
     
    df = df[ordered_columns]
    df = pd.DataFrame(df)
    return df



def stitch_time_series(
        data_frames: list,
) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    data_frames : list
        A list of datasets to be stitched together for time series
        Columns must have unique names highlighting year.

    Returns
    -------
    time_series_df : df
       One dataframe with full time series.

    """
    #merge time series data together
    time_series_df = data_frames[0]
    for df in data_frames[1:]:
        time_series_df = pd.merge(time_series_df, df, on="ID", how="outer")
    return time_series_df

def unstack_data(
        data: pd.DataFrame,
        loaded_config: Dict,         
        value_cols,
) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    data : pd.DataFrame
        dataset containing stacked LAD data.
    loaded_config : Dict
        loaded config including file paths and field names
    value_cols : list
        List of value column names in square brackets.
    Returns
    -------
    unstacked_data : pd.Dataframe
        wide table of data.

    """
    #extract information from config
    stacked_col = loaded_config["keep_variable"]
    area_col = loaded_config["df_areacode"]
    
    #unstack data using pivot
    unstacked_data = pd.pivot_table(data,
                                    index=area_col, 
                                    columns=stacked_col, 
                                    values=value_cols, 
                                    aggfunc= "sum").reset_index()
    return unstacked_data

