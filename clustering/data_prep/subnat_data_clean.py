#Functions to read in and clean data from the subnational indicators explorer.
def get_code_column(dataset, flag='E0'):
    """Usage: The flag is a substring that we want to identify geography codes. 
    Default is E0, which is the prefix of areas in England, 
    but can be specified otherwise if data isn't at that level."""
    #Note use of groupby here as there was a deprecation warning for all(level=1), suggesting groupby(level=1).all() is safer.
    #But beware that default behaviour of groupby is to sort alphabetically, which we very much don't want!
    col = dataset.columns[dataset.stack().str.contains(flag).groupby(level=1, sort=False).any()]
    if len(col)==0:
        print("ERROR: No columns that look like the contain geography codes found. Checking if the flag entered matches the expected pattern")
    return(col)

#Find the number of areas
def number_areas(dataset, flag='E0'):
    area_col = get_code_column(dataset, flag)
    if len(area_col)==0:
        return(0)
    areas = dataset[area_col].drop_duplicates()
    return(len(areas))

#Want to have methods to convert variables between upper tier and lower tier local authorities.
def get_UTLT_lookup():
#We're defining upp_low_tier lookup as a GLOBAL variable, as we don't want to read this unless is isn't defined.
#Note here: this global is really module global, so defined for functions in this script.
    global upp_low_tier
    import pandas as pd
    upp_low_tier = pd.read_csv("lookup/Lower_Tier_Local_Authority_to_Upper_Tier_Local_Authority__April_2019__Lookup_in_England_and_Wales.csv")
    upp_low_tier = upp_low_tier[["LTLA19CD", "UTLA19CD"]]
    global all_UT
    all_UT = upp_low_tier["UTLA19CD"].drop_duplicates()
        
#Simple way to model this would be when we have UT data, model LT data as all equal to UT.
def UT_metric_to_LT(metric):
    import pandas as pd
    lower_metrics=[]
    area_col = get_code_column(metric)
    #Ensure we have the lookup of upper tier LAs to lower tier LAs in our environment.
    if 'upp_low_tier' not in globals():
        get_UTLT_lookup()
    for row in range(len(metric)):
        #Test to see if this a upper tier LA. If it is, replace it with lower tier.
        if metric.iloc[row].loc[area_col][0] in list(all_UT):
            lower_metric = metric.reset_index().truncate(row,row).merge(upp_low_tier, 
                                                                        left_on=area_col[0], right_on="UTLA19CD",how="left")
            lower_metric = lower_metric.drop(columns=[area_col[0], 'UTLA19CD'])
            col = lower_metric.pop('LTLA19CD')
            lower_metric.insert(1, area_col[0], col)
            lower_metrics.append(lower_metric)
        else:
            lower_metric = metric.reset_index().truncate(row,row).merge(upp_low_tier, left_on=area_col[0], right_on="UTLA19CD", how="left")
            lower_metric = lower_metric.drop(columns=['LTLA19CD', 'UTLA19CD'])
            lower_metrics.append(lower_metric)
    return(pd.concat(lower_metrics))

#Function which drops an unwanted column from a dataframe.
def drop_index_column(metric, col_to_drop='index'):
    if col_to_drop in list(metric):
        return(metric.drop(columns=col_to_drop))
    else:
        return(metric)

#This takes the area column, renames in to 'AREACD' as in consolidated dataset. 
#And sticks it in the front of the dataframe, in case it isn't already
def harmonise_area_col_name(metric):
    area_col=get_code_column(metric)[0]
    col = metric.pop(area_col)
    metric.insert(0, "AREACD", col)
    return(metric)

#Checks if the values are properly numeric and coerces them if they happen to be stringy.
def ensure_value_numeric(metric):
    import numpy as np
    import pandas as pd
    if not np.issubdtype(metric['Value'].dtypes, np.number):
        metric['Value'] = pd.to_numeric(metric['Value'], errors='coerce')
    return(metric)

#All in one function that does all the cleaning. Should work fine, as data that is OK should be left untouched by all components.
def all_cleaning(metric):
    metric = UT_metric_to_LT(metric)
    metric = drop_index_column(metric)
    metric = drop_index_column(metric, col_to_drop = 'YEAR')
    metric = harmonise_area_col_name(metric)
    metric = ensure_value_numeric(metric)
    return(metric)

def clean_groups(group):
    for metric in range(len(group)):
        group[metric] = all_cleaning(group[metric])
    return(group)


