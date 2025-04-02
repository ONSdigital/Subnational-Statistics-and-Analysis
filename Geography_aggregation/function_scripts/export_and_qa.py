import pandas as pd
from typing import Dict


def check_missing_geographies(
        data: pd.DataFrame,
        loaded_config: Dict,
) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    data : pd.DataFrame
        Unstacked data that you have created aggregated geographies from
    loaded_config: Dict
        config including field names and file paths

    Returns
    -------
    missing_values : dataframe
        dataframe containing all rows with missing values and aggregated geographies affected

    """
    #extract information from config
    geography_code_col = loaded_config["df_areacode"]
    geog_lookup_col = loaded_config["lookup_lowest_geography_code"]
    geog_lookup_nm_col = loaded_config["lookup_lowest_geography_name"]
    lookuploc = loaded_config["lu"] + loaded_config["lookupfile"]
    geog_list = pd.read_excel(lookuploc)
    
    #subset the geog list and merge to lookup
    geog_list_filtered = geog_list[[geog_lookup_col,geog_lookup_nm_col]]
    all_geogs = geog_list_filtered.merge(data, right_on=geography_code_col, left_on=geog_lookup_col, how="left")
    all_geogs.set_index([geog_lookup_col, geog_lookup_nm_col], inplace=True)
    
    #create dataframe of missing values
    missing_values = all_geogs[all_geogs.isnull().any(axis=1)]
    missing_values.reset_index()
    
    #merge dataframe of missing values to geog list
    missing_values = missing_values.merge(geog_list, right_on=geog_lookup_col, left_on=geography_code_col, how="left")
    return missing_values

def export_to_xlsx(
        frames, 
        file_path, 
        file_name, 
 ):
    """
    

    Parameters
    ----------
    frames : Dict
        A dictionary of items to export and their desired sheet titles.
    file_path : 
        Path to the outputs file.
    file_name : 
        Desired file name of output.

    Returns
    -------
    None.

    """
    writer = pd.ExcelWriter(f"{file_path}/{file_name}.xlsx", engine="xlsxwriter")
    for sheet, frame in frames.items():
        frame.to_excel(writer, sheet_name=sheet)
    else:    
        writer.close()

    return "table exported"