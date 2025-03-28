import pandas as pd
from src.utils.utils import get_table_from_path

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

def read_in_census_21(loaded_config):
    """
    This function reads in 2021 Census population by single year of age for each LAD,
    renames columns to remove spaces and drops unnecessary columns. No parameters needed.
    This data is then used to weight old local authorities to create new data.
    
    """
    #Read in census 21 single year age data
    census_21 = get_table_from_path(
        table_name=loaded_config["census_21_table"],
        path=loaded_config["inputs_file_path"],
        create_geodataframe=False,
        cols_to_select = None
        )
    #Clean census columns
    number_of_rows_to_remove = 6 
    census_21 = census_21.iloc[number_of_rows_to_remove-1:]
    census_21.columns = census_21.iloc[0]
    census_21 = census_21.iloc[1:]
    census_21 = census_21.reset_index(drop=True)
    
    census_21.columns = census_21.columns.str.replace(' ', '_')
    census_21.columns = [x.lower() for x in census_21.columns]
    census_21 = census_21.drop("area", axis=1)
    census_21.rename(columns={"mnemonic":"AREACD", "total:_all_usual_residents":"total_population"}, inplace=True)
    census_21 = census_21.apply(pd.to_numeric, errors = "ignore")
    return(census_21)

def read_in_area_file(loaded_config): 
    """
    This function reads in LAD areas in hectares, drops unnecessary columns and creates two new fields:
    total area in square kilometers and total area in square meters. No parameters needed. 
    This data is then used to weight old local authorities to create new data.       
    """
    #Read in area file
    LAD_area = get_table_from_path(
        table_name=loaded_config["LAD_area_table"],
        path=loaded_config["inputs_file_path"],
        create_geodataframe=False,
        cols_to_select = None,
    )
    
    #Clean area column and create km and m squared
    LAD_area = LAD_area[[loaded_config["Area_LAD_col"], "AREAEHECT"]]
    LAD_area.rename(columns={loaded_config["Area_LAD_col"]:"AREACD"}, inplace=True)
    LAD_area["AREASQKM"] = LAD_area["AREAEHECT"]/100
    LAD_area["AREASQM"] = LAD_area["AREAEHECT"]*10000
    return(LAD_area)

#Function for changing all metrics using 2022 boundaries to 2023 boundaries - uses Census 2021 population

def convert_LAD22_to_LAD23(loaded_config, metric, start_col, denominator = "population", end_col=None):
    """
    This function can be used to calculate values for the LAD that were created
    as a result of the 2023 Local Authority boundary changes 
    (North Yorkshire, Somerset, Cumberland and Westmorland and Furness). 
    The function imputes new values for a new local authority 
    (e.g. North Yorkshire) by taking the data for its constituent former LAD 
    (e.g. Craven, Hambleton, etc.) and "weighing" it based on population or area. 
    
    metric = name of dataset with geographies that need converting. 
    denominator = name of dataset to be used as denominator for conversion, defaults to population. Options are:
        1.population = Census 2021 population by single year of age
        2.area = Land area of LAD in hectares (AREAEHECT, position 2), square km (AREAKM, position 3), 
                 or square meters (AREASQM, position 4) - 

   start_col = Position of the column from denominator dataset to be used as the denominator for new values calculations. 
       For Census total population (no age restrictions) or for total area start_col should be set to 2. 
       Generally, to get the position of the column for an age you should add 2 to the required age 
       (e.g. "aged_18_years" will be in position 20).
    end_col = defaults to None, but should always be set if using a range of columns.
        The position of the end column in the range of columns to sum for the denominator
        (e.g. if population 18-24 is needed, the end_col should be set to the census col 
         "aged_24_years", which means that the parameter should be set to 26 (24+2). 
    """
    import numpy as np

    #read in denominator dataset  
    if denominator == "area":
        den = read_in_area_file(loaded_config)
    else: 
        den = read_in_census_21(loaded_config)
    
    #selects either a single column or the sum of a range of columns(depending on parameters) and renames it Denominator 
    if end_col is not None:
        den["Denominator"] = den.iloc[:, start_col:end_col].sum(axis=1)
    else:
        den["Denominator"] = den.iloc[:, start_col]
        
        
    den = den[["AREACD", "Denominator"]]
    
    #North Yorkshire
    
    #If inactive county value (E10000023) for North Yorkshire exists in dataset, replace it with current UA value (E06000065)
    
    if (metric["AREACD"]=="E10000023").any():
        
        metric.AREACD = metric.AREACD.replace('E10000023', "E06000065")
        LAD23_n_yorkshire = metric.loc[metric["AREACD"]=="E06000065"]
    
    else:
        
        #If inactive county value does not exists for North Yorkshire, calculate current UA value using LAD  
                                                                              
        den_NY = den[(den['AREACD'] == 'E07000163') |
                     (den['AREACD'] == 'E07000164') | 
                     (den['AREACD'] == 'E07000165') | 
                     (den['AREACD'] == 'E07000166') | 
                     (den['AREACD'] == 'E07000167') | 
                     (den['AREACD'] == 'E07000168') | 
                     (den['AREACD'] == 'E07000169')]

        LAD23_n_yorkshire = metric[(metric['AREACD'] == 'E07000163') |
                                   (metric['AREACD'] == 'E07000164') | 
                                   (metric['AREACD'] == 'E07000165') | 
                                   (metric['AREACD'] == 'E07000166') | 
                                   (metric['AREACD'] == 'E07000167') | 
                                   (metric['AREACD'] == 'E07000168') | 
                                   (metric['AREACD'] == 'E07000169')]
        
        LAD23_n_yorkshire = LAD23_n_yorkshire.merge(den_NY, how="outer")
        LAD23_n_yorkshire['Prop_of_den'] = LAD23_n_yorkshire['Denominator'] / LAD23_n_yorkshire['Denominator'].sum()
        LAD23_n_yorkshire['Scaled_Value'] = LAD23_n_yorkshire['Value'] * LAD23_n_yorkshire['Prop_of_den']
        LAD23_n_yorkshire['Value'] = LAD23_n_yorkshire['Scaled_Value'].sum()
        LAD23_n_yorkshire['AREACD'] = 'E06000065'
        LAD23_n_yorkshire = LAD23_n_yorkshire.filter(items=['AREACD', 'Indicator', 'Value'])
        LAD23_n_yorkshire = LAD23_n_yorkshire.drop_duplicates()
                                                                                                                                                                                                 
    #Somerset
     
    #If inactive county value (E10000027) for Somerset exists in dataset, replace it with current UA value (E06000066)
   
    if (metric["AREACD"]=="E10000027").any():
        
        metric.AREACD = metric.AREACD.replace('E10000027', "E06000066")
        LAD23_somerset = metric.loc[metric["AREACD"]=="E06000066"]
    
    else:
        
        #If inactive county value does not exists for Somerset, calculate current UA value using LAD             
        den_somerset = den[(den['AREACD'] == 'E07000187') | 
                           (den['AREACD'] == 'E07000188') | 
                           (den['AREACD'] == 'E07000189') | 
                           (den['AREACD'] == 'E07000246')]
        
        LAD23_somerset = metric[(metric['AREACD'] == 'E07000187') |
                                (metric['AREACD'] == 'E07000188') |
                                (metric['AREACD'] == 'E07000189') | 
                                (metric['AREACD'] == 'E07000246')]
        
        LAD23_somerset = LAD23_somerset.merge(den_somerset, how="outer")
        LAD23_somerset['Prop_of_den'] = LAD23_somerset['Denominator'] / LAD23_somerset['Denominator'].sum()
        LAD23_somerset['Scaled_Value'] = LAD23_somerset['Value'] * LAD23_somerset['Prop_of_den']
        LAD23_somerset['Value'] = LAD23_somerset['Scaled_Value'].sum()
        LAD23_somerset['AREACD'] = 'E06000066'
        LAD23_somerset = LAD23_somerset.filter(items=['AREACD', 'Indicator', 'Value'])
        LAD23_somerset = LAD23_somerset.drop_duplicates()
    
        #Cumbria: Cumberland and Westmorland and Furness - only calculate values if E07s are available 

    den_cumberland = den[(den['AREACD'] == 'E07000026') |
                         (den['AREACD'] == 'E07000028') |
                         (den['AREACD'] == 'E07000029')]
    
    LAD23_cumberland = metric[(metric['AREACD'] == 'E07000026') |
                              (metric['AREACD'] == 'E07000028') |
                              (metric['AREACD'] == 'E07000029')]

    LAD23_cumberland = LAD23_cumberland.merge(den_cumberland, how="outer")
    LAD23_cumberland['Prop_of_den'] = LAD23_cumberland['Denominator'] / LAD23_cumberland['Denominator'].sum()
    LAD23_cumberland['Scaled_Value'] = LAD23_cumberland['Value'] * LAD23_cumberland['Prop_of_den']
    LAD23_cumberland['Value'] = LAD23_cumberland['Scaled_Value'].sum()
    LAD23_cumberland['AREACD'] = 'E06000063'
    LAD23_cumberland = LAD23_cumberland.filter(items=['AREACD', 'Indicator', 'Value'])
    LAD23_cumberland = LAD23_cumberland.drop_duplicates()

    den_wf= den[(den['AREACD'] == 'E07000030') |
                (den['AREACD'] == 'E07000031') |
                (den['AREACD'] == 'E07000027')]
    
    LAD23_wf = metric[(metric['AREACD'] == 'E07000030') |
                      (metric['AREACD'] == 'E07000031') |
                      (metric['AREACD'] == 'E07000027')]

    LAD23_wf = LAD23_wf.merge(den_wf, how="outer")                                                          
    LAD23_wf['Prop_of_den'] = LAD23_wf['Denominator'] / LAD23_wf['Denominator'].sum()
    LAD23_wf['Scaled_Value'] = LAD23_wf['Value'] * LAD23_wf['Prop_of_den']
    LAD23_wf['Value'] = LAD23_wf['Scaled_Value'].sum()
    LAD23_wf['AREACD'] = 'E06000064'
    LAD23_wf = LAD23_wf.filter(items=['AREACD', 'Indicator', 'Value'])
    LAD23_wf = LAD23_wf.drop_duplicates()
    
    #Northamptomshire 
    
    if (metric["AREACD"]=="E07000150").any():
        #North Northamptonshire subset
        LAD21_north_north = metric[(metric['AREACD'] == 'E07000150') |
                                   (metric['AREACD'] == 'E07000152') |
                                   (metric['AREACD'] == 'E07000153') |
                                   (metric['AREACD'] == 'E07000156')]
        
        den_north_north = den[(den['AREACD'] == 'E07000150') |
                              (den['AREACD'] == 'E07000152') |
                              (den['AREACD'] == 'E07000153') |
                              (den['AREACD'] == 'E07000156')]
        
        LAD21_north_north = LAD21_north_north.merge(den_north_north, how="outer")                                                          
        LAD21_north_north['Prop_of_den'] = LAD21_north_north['Denominator'] / LAD21_north_north['Denominator'].sum()
        LAD21_north_north['Scaled_Value'] = LAD21_north_north['Value'] * LAD21_north_north['Prop_of_den']
        LAD21_north_north['Value'] = LAD21_north_north['Scaled_Value'].sum()
        LAD21_north_north['AREACD'] = 'E06000061'
        LAD21_north_north = LAD21_north_north.filter(items=['AREACD', 'Indicator', 'Value'])
        LAD21_north_north = LAD21_north_north.drop_duplicates()

        #West Northamptonshire subset
        LAD21_west_north = metric[(metric['AREACD'] == 'E07000151') |
                                  (metric['AREACD'] == 'E07000154') |
                                  (metric['AREACD'] == 'E07000155')]
        
        den_west_north = den[(den['AREACD'] == 'E07000151') |
                             (den['AREACD'] == 'E07000154') |
                             (den['AREACD'] == 'E07000155')]
        
        LAD21_west_north = LAD21_west_north.merge(den_west_north, how="outer")                                                          
        LAD21_west_north['Prop_of_den'] = LAD21_west_north['Denominator'] / LAD21_west_north['Denominator'].sum()
        LAD21_west_north['Scaled_Value'] = LAD21_west_north['Value'] * LAD21_west_north['Prop_of_den']
        LAD21_west_north['Value'] = LAD21_west_north['Scaled_Value'].sum()
        LAD21_west_north['AREACD'] = 'E06000062'
        LAD21_west_north = LAD21_west_north.filter(items=['AREACD', 'Indicator', 'Value'])
        LAD21_west_north = LAD21_west_north.drop_duplicates()
        
    else:
        LAD21_north_north = pd.DataFrame()

    #Merge 'new' boundaries into dataset 
    LAD23 = metric.merge(LAD23_n_yorkshire, how="outer")
    LAD23 = LAD23.merge(LAD23_somerset, how="outer")
    LAD23 = LAD23.merge(LAD23_cumberland, how="outer")
    LAD23 = LAD23.merge(LAD23_wf, how="outer")
    
    if not LAD21_north_north.empty:
        LAD23 = LAD23.merge(LAD21_north_north, how="outer")
        LAD23 = LAD23.merge(LAD21_west_north, how="outer")
    
    #If old Buckinghamshire upper tier code is in data. replace it with new one (E06000060)
    LAD23.AREACD = LAD23.AREACD.replace('E10000002', "E06000060")
        
    inactive_codes= ["E07000163", 
                     "E07000164", 
                     "E07000165", 
                     "E07000166", 
                     "E07000167", 
                     "E07000168", 
                     "E07000169", 
                     "E07000187", 
                     "E07000188", 
                     "E07000189", 
                     "E07000246", 
                     "E07000026", 
                     "E07000028", 
                     "E07000029", 
                     "E07000030", 
                     "E07000031", 
                     "E07000027"]
    
    LAD23 = LAD23[~LAD23.AREACD.isin(inactive_codes)]
                                                                   
    return(LAD23)