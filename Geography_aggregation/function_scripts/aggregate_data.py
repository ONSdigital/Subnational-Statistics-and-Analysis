import pandas as pd
from typing import Dict

def get_all_desired_geographies(
        data: pd.DataFrame,
        loaded_config: Dict,  
) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    data : pd.DataFrame
        A dataset including the data to be aggregated.
    loaded_config: Dict
        config including field names, file paths and value columns
    Returns
    -------
    output
        a stacked dataframe with all desired geographies

    """
    #extract information from the config
    lookuploc = loaded_config["lu"] + loaded_config["lookupfile"]
    lookup = pd.read_excel(lookuploc)
    data_geog_col = loaded_config["df_areacode"]
    value_cols = loaded_config["value_columns"]
    desired_geographies = loaded_config["geoglist"]
    input_geog_col = loaded_config["lookup_lowest_geography_code"]
    
    #create empty dataframe to be filled in the loop below
    output = pd.DataFrame(columns=value_cols)
    output.insert(loc=0, column="AREACD", value="")
    
    #loop through the list of desired geographies, calculate aggregation then append to one dataframe
    for i in desired_geographies:
        lookup_filter = lookup[[input_geog_col, i]]
        data_merged = data.merge(lookup_filter, right_on=input_geog_col, left_on=data_geog_col, how="left")
        temp = data_merged.groupby([i], as_index=False)[value_cols].agg({lambda x: x.sum(skipna=False)})
        temp_reset = temp.reset_index() 
        temp = temp_reset
        temp.columns = temp.columns.get_level_values(0)
        temp = temp.rename(columns={i: "AREACD"})
        output = pd.concat([output,temp],ignore_index=True)
        
    return output

def get_all_desired_geographies_keep_column(
        data: pd.DataFrame,
        loaded_config: Dict, 
) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    data : pd.DataFrame
        A dataset including the data to be aggregated.
    loaded_config: Dict
        config including field names, file paths and value columns
    Returns
    -------
    data_merged_pivot
        data frame aggregated to new geogaraphy.

    """
    #extract information from the config
    lookuploc = loaded_config["lu"] + loaded_config["lookupfile"]
    lookup = pd.read_excel(lookuploc)
    data_geog_col = loaded_config["df_areacode"]
    value_cols = loaded_config["value_columns"]
    desired_geographies = loaded_config["geoglist"]
    input_geog_col = loaded_config["lookup_lowest_geography_code"]
    keep_col = loaded_config["keep_variable"]
    
    #create empty dataframe to be filled in the loop below
    output = pd.DataFrame(columns=value_cols)
    output.insert(loc=0, column="AREACD", value="")
    output.insert(loc=1, column=keep_col, value="")
    
    #loop through the list of desired geographies, calculate aggregation then append to one dataframe
    for i in desired_geographies:
        lookup_filter = lookup[[input_geog_col, i]]
        data_merged = data.merge(lookup_filter, right_on=input_geog_col, left_on=data_geog_col, how="left")
        temp = data_merged.groupby([i, keep_col], as_index=False)[value_cols].agg({lambda x: x.sum(skipna=False)})
        temp_reset = temp.reset_index() 
        temp = temp_reset
        temp.columns = temp.columns.get_level_values(0)
        temp = temp.rename(columns={i: "AREACD"})
        output = pd.concat([output,temp],ignore_index=True)
         
    return output
