import pandas as pd
from typing import Dict

def update_boundaries(df:pd.DataFrame,
                      loaded_config: Dict,
 ):
    """

    Parameters
    ----------
    df : pd.DataFrame
        A dataset including the data with obsolete geographies, to be updated to current boundaries.
    loaded_config : Dict
        loaded config containing field names, file paths and value columms
    Returns
    -------
    pd.DataFrame
        Dataframe including both new and obsolete geographies.

    """
    #extract the required information from the loaded config
    df_areacode = loaded_config["df_areacode"]
    df_areaname = loaded_config["df_areaname"]
    value_cols = loaded_config["value_columns"]
    boundloc = loaded_config["lu"] + loaded_config["boundaryfile"]
    lookup = pd.read_excel(boundloc)
    lookup_areacode_old = loaded_config["old_area_code"]
    lookup_areaname_old = loaded_config["old_area_name"]
    lookup_areacode_new = loaded_config["new_area_code"]
    lookup_areaname_new = loaded_config["new_area_name"]
    
    #subset the lookup to remove unnecessary columns
    lookup = lookup[[lookup_areacode_old, 
                     lookup_areaname_old,
                     lookup_areacode_new,
                     lookup_areaname_new]]
    
    #merge data onto the lookup on the old area codes   
    new_geog_only = df.merge(lookup, left_on= df_areacode, 
             right_on = lookup_areacode_old, 
             how="inner")
    
    #create estimates for new geographies by grouping by new geography columns
    new_geog_grouped = new_geog_only.groupby([lookup_areacode_new, lookup_areaname_new],
                                             as_index=False)[value_cols].agg({lambda x: x.sum(skipna=False)})
    new_geog_grouped = new_geog_grouped.reset_index() 
    new_geog_grouped.columns = new_geog_grouped.columns.get_level_values(0)
    new_geog_grouped.rename(columns={lookup_areacode_new:df_areacode, lookup_areaname_new:df_areaname}, inplace=True)
    
    #create dataframe including the obsolete values so these are retained if required
    all_other_geogs = df.merge(lookup, 
              left_on=df_areacode,
              right_on = lookup_areacode_old,
              how="outer", 
              indicator=True).drop(columns=[lookup_areacode_old,
                                            lookup_areaname_old,
                                            lookup_areacode_new,
                                            lookup_areaname_new])
    
    all_other_geogs=all_other_geogs[all_other_geogs["_merge"]=="left_only"].drop(columns="_merge")
    
    #merge to create full DataFrame including all new geographies as final output
    updated_boundaries = pd.concat([all_other_geogs, new_geog_grouped], 
                                   ignore_index=True)

    return updated_boundaries.reset_index(drop=True)


def update_boundaries_keep_column(df:pd.DataFrame,
                      loaded_config: Dict,
                      ):
    """

    Parameters
    ----------
    df : pd.DataFrame
        A dataset including the data with obsolete geographies, to be updated to current boundaries.
    loaded_config : Dict
        loaded config containing field names, file paths and value columms
    Returns
    -------
    pd.DataFrame
        Dataframe including both new and obsolete geographies.

    """
    #extract the required information from the loaded config
    df_areacode = loaded_config["df_areacode"]
    df_areaname = loaded_config["df_areaname"]
    value_cols = loaded_config["value_columns"]
    boundloc = loaded_config["lu"] + loaded_config["boundaryfile"]
    lookup = pd.read_excel(boundloc)
    lookup_areacode_old = loaded_config["old_area_code"]
    lookup_areaname_old = loaded_config["old_area_name"]
    lookup_areacode_new = loaded_config["new_area_code"]
    lookup_areaname_new = loaded_config["new_area_name"]
    keep_variable = loaded_config["keep_variable"]
    index_cols = [lookup_areacode_new, lookup_areaname_new, keep_variable]
    
    #subset the lookup to remove unnecessary columns
    lookup = lookup[[lookup_areacode_old, 
                     lookup_areaname_old,
                     lookup_areacode_new,
                     lookup_areaname_new]]
    
    #merge data onto the lookup on the old area codes
    new_geog_only = df.merge(lookup, left_on= df_areacode, 
             right_on = lookup_areacode_old, 
             how="inner")
    
    #create estimates for new geographies by grouping by new geography columns
    new_geog_grouped = new_geog_only.groupby((index_cols),
                                             as_index=False)[value_cols].agg({lambda x: x.sum(skipna=False)})
    new_geog_grouped = new_geog_grouped.reset_index() 
    new_geog_grouped.columns = new_geog_grouped.columns.get_level_values(0)
    new_geog_grouped.rename(columns={lookup_areacode_new:df_areacode, lookup_areaname_new:df_areaname}, inplace=True)
    
    #create dataframe including the obsolete values so these are retained if required
    all_other_geogs = df.merge(lookup, 
              left_on=df_areacode,
              right_on = lookup_areacode_old,
              how="outer", 
              indicator=True).drop(columns=[lookup_areacode_old,
                                            lookup_areaname_old, 
                                            lookup_areacode_new, 
                                            lookup_areaname_new])
    
    all_other_geogs=all_other_geogs[all_other_geogs["_merge"]=="left_only"].drop(columns="_merge")
    
    #merge to create full DataFrame including all new geographies as final output
    updated_boundaries = pd.concat([all_other_geogs, new_geog_grouped], 
                                   ignore_index=True)

    return updated_boundaries.reset_index(drop=True)
