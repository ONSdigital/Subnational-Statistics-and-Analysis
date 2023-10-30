exec(open("data_prep/subnat_data_import_march23.py").read()) 
from data_prep.subnat_data_clean import *
pd.set_option('display.max_rows', None)

import numpy as np

#LAD21 lookup (boundaries for 2022 are identical)
from google.cloud import bigquery
import pandas as pd
client = bigquery.Client(location=" europe-west2")

query = """
    SELECT *
    FROM `ons-luda-data-prod.ingest_luda.LAD_to_Country_Apr2021_UK` 
    
"""
query_job = client.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="europe-west2",
)  # API request - starts the query
LAD21 = query_job.to_dataframe().filter(items=['LAD21CD', 'LAD21NM'])

#PUBLIC TRANSPORT FUNCTION RECODE - function to convert transport metrics (at 2011 boundaries) to LAD21
def covert_transport_LAD21(metric):
    
    import numpy as np

    from google.cloud import bigquery
    import pandas as pd
    client = bigquery.Client(location=" europe-west2")

    query = """
    SELECT *
    FROM `ons-luda-data-prod.ingest_luda.LAD_to_Country_Apr2021_UK` 
    
    """
    query_job = client.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="europe-west2",
    )  # API request - starts the query
    LAD21 = query_job.to_dataframe().filter(items=['LAD21CD', 'LAD21NM'])
    
    #Bournemouth, Christchurch and Poole subset
    transport_bcp = metric[(metric['AREACD'] == 'E06000028') | (metric['AREACD'] == 'E06000029') | (metric['AREACD'] == 'E07000048')]
    transport_bcp['Population'] = np.where(transport_bcp['AREACD']=='E06000028', 183491,
                                    np.where(transport_bcp['AREACD']=='E06000029', 147645,
                                             np.where(transport_bcp['AREACD']=='E07000048', 47752, 0)))
    transport_bcp['Prop_of_pop'] = transport_bcp['Population'] / transport_bcp['Population'].sum()
    transport_bcp['Scaled_Value'] = transport_bcp['Value'] * transport_bcp['Prop_of_pop']
    transport_bcp['Value'] = transport_bcp['Scaled_Value'].sum()
    transport_bcp['AREACD'] = 'E06000058'
    transport_bcp = transport_bcp.filter(items=['AREACD', 'Indicator', 'Value'])
    transport_bcp = transport_bcp.drop_duplicates()

    transport_dorset = metric[(metric['AREACD'] == 'E07000049') | (metric['AREACD'] == 'E07000050') | (metric['AREACD'] == 'E07000051') | (metric['AREACD'] == 'E07000052') | (metric['AREACD'] == 'E07000053')]
    transport_dorset['Population'] = np.where(transport_dorset['AREACD']=='E07000049', 87166,
                                    np.where(transport_dorset['AREACD']=='E07000050', 68583,
                                             np.where(transport_dorset['AREACD']=='E07000051', 44973,
                                                      np.where(transport_dorset['AREACD']=='E07000052', 99264,
                                                               np.where(transport_dorset['AREACD']=='E07000053', 65167, 0)))))
    transport_dorset['Prop_of_pop'] = transport_dorset['Population'] / transport_dorset['Population'].sum()
    transport_dorset['Scaled_Value'] = transport_dorset['Value'] * transport_dorset['Prop_of_pop']
    transport_dorset['Value'] = transport_dorset['Scaled_Value'].sum()
    transport_dorset['AREACD'] = 'E06000059'
    transport_dorset = transport_dorset.filter(items=['AREACD', 'Indicator', 'Value'])
    transport_dorset = transport_dorset.drop_duplicates()

    #Buckinghamshire subset
    transport_buck = metric[(metric['AREACD'] == 'E07000004') | (metric['AREACD'] == 'E07000005') | (metric['AREACD'] == 'E07000006') | (metric['AREACD'] == 'E07000007')]
    transport_buck['Population'] = np.where(transport_buck['AREACD']=='E07000004', 174137,
                                     np.where(transport_buck['AREACD']=='E07000005', 92635,
                                             np.where(transport_buck['AREACD']=='E07000006', 66867,
                                                     np.where(transport_buck['AREACD']=='E07000007', 171644, 0))))
    transport_buck['Prop_of_pop'] = transport_buck['Population'] / transport_buck['Population'].sum()
    transport_buck['Scaled_Value'] = transport_buck['Value'] * transport_buck['Prop_of_pop']
    transport_buck['Value'] = transport_buck['Scaled_Value'].sum()
    transport_buck['AREACD'] = 'E06000060'
    transport_buck = transport_buck.filter(items=['AREACD', 'Indicator', 'Value'])
    transport_buck = transport_buck.drop_duplicates()

    #North Northamptonshire subset
    transport_north_north = metric[(metric['AREACD'] == 'E07000150') | (metric['AREACD'] == 'E07000152') | (metric['AREACD'] == 'E07000153') | (metric['AREACD'] == 'E07000156')]
    transport_north_north['Population'] = np.where(transport_north_north['AREACD']=='E07000150', 61255,
                                     np.where(transport_north_north['AREACD']=='E07000152', 86765,
                                             np.where(transport_north_north['AREACD']=='E07000153', 93475,
                                                     np.where(transport_north_north['AREACD']=='E07000156', 75356, 0))))
    transport_north_north['Prop_of_pop'] = transport_north_north['Population'] / transport_north_north['Population'].sum()
    transport_north_north['Scaled_Value'] = transport_north_north['Value'] * transport_north_north['Prop_of_pop']
    transport_north_north['Value'] = transport_north_north['Scaled_Value'].sum()
    transport_north_north['AREACD'] = 'E06000061'
    transport_north_north = transport_north_north.filter(items=['AREACD', 'Indicator', 'Value'])
    transport_north_north = transport_north_north.drop_duplicates()

    #West Northamptonshire subset
    transport_west_north = metric[(metric['AREACD'] == 'E07000151') | (metric['AREACD'] == 'E07000154') | (metric['AREACD'] == 'E07000155')]
    transport_west_north['Population'] = np.where(transport_west_north['AREACD']=='E07000151', 77843,
                                     np.where(transport_west_north['AREACD']=='E07000154', 212069,
                                             np.where(transport_west_north['AREACD']=='E07000155', 85189, 0)))
    transport_west_north['Prop_of_pop'] = transport_west_north['Population'] / transport_west_north['Population'].sum()
    transport_west_north['Scaled_Value'] = transport_west_north['Value'] * transport_west_north['Prop_of_pop']
    transport_west_north['Value'] = transport_west_north['Scaled_Value'].sum()
    transport_west_north['AREACD'] = 'E06000062'
    transport_west_north = transport_west_north.filter(items=['AREACD', 'Indicator', 'Value'])
    transport_west_north = transport_west_north.drop_duplicates()

    #East Suffolk subset
    transport_e_suffolk = metric[(metric['AREACD'] == 'E07000205') | (metric['AREACD'] == 'E07000206')]
    transport_e_suffolk['Population'] = np.where(transport_e_suffolk['AREACD']=='E07000205', 124298,
                                     np.where(transport_e_suffolk['AREACD']=='E07000206', 115254, 0))
    transport_e_suffolk['Prop_of_pop'] = transport_e_suffolk['Population'] / transport_e_suffolk['Population'].sum()
    transport_e_suffolk['Scaled_Value'] = transport_e_suffolk['Value'] * transport_e_suffolk['Prop_of_pop']
    transport_e_suffolk['Value'] = transport_e_suffolk['Scaled_Value'].sum()
    transport_e_suffolk['AREACD'] = 'E07000244'
    transport_e_suffolk = transport_e_suffolk.filter(items=['AREACD', 'Indicator', 'Value'])
    transport_e_suffolk = transport_e_suffolk.drop_duplicates()

    #West Suffolk subset
    transport_w_suffolk = metric[(metric['AREACD'] == 'E07000201') | (metric['AREACD'] == 'E07000204')]
    transport_w_suffolk['Population'] = np.where(transport_w_suffolk['AREACD']=='E07000201', 59748,
                                     np.where(transport_w_suffolk['AREACD']=='E07000204', 111008, 0))
    transport_w_suffolk['Prop_of_pop'] = transport_w_suffolk['Population'] / transport_w_suffolk['Population'].sum()
    transport_w_suffolk['Scaled_Value'] = transport_w_suffolk['Value'] * transport_w_suffolk['Prop_of_pop']
    transport_w_suffolk['Value'] = transport_w_suffolk['Scaled_Value'].sum()
    transport_w_suffolk['AREACD'] = 'E07000245'
    transport_w_suffolk = transport_w_suffolk.filter(items=['AREACD', 'Indicator', 'Value'])
    transport_w_suffolk = transport_w_suffolk.drop_duplicates()

    #2023 boundaries 
    
    #Somerset - code previously merged West Somerset and Taunton Deane into Somerset West and Taunton - now merges Mendip, Sedgemoor, and South Somerset into Somerset as well 
    
    transport_somerset = metric[(metric['AREACD'] == 'E07000190') | (metric['AREACD'] == 'E07000191') | (metric['AREACD'] == 'E07000187') |     (metric['AREACD'] == 'E07000188') | (metric['AREACD'] == 'E07000189')]
    transport_somerset['Population'] = np.where(transport_somerset['AREACD']=='E07000190', 110187,
                                    np.where(transport_somerset['AREACD']=='E07000191', 34675,
                                             np.where(transport_somerset['AREACD']=='E07000187', 109279,
                                                      np.where(transport_somerset['AREACD']=='E07000188', 114588,
                                                               np.where(transport_somerset['AREACD']=='E07000189', 161243, 0)))))
    transport_somerset['Prop_of_pop'] = transport_somerset['Population'] / transport_somerset['Population'].sum()
    transport_somerset['Scaled_Value'] = transport_somerset['Value'] * transport_somerset['Prop_of_pop']
    transport_somerset['Value'] = transport_somerset['Scaled_Value'].sum()
    transport_somerset['AREACD'] = 'E06000066'
    transport_somerset = transport_somerset.filter(items=['AREACD', 'Indicator', 'Value'])
    transport_somerset = transport_somerset.drop_duplicates()
  
    

    
    #North Yorkshire
     
    transport_n_yorkshire = metric[(metric['AREACD'] == 'E07000163') | (metric['AREACD'] == 'E07000164') | (metric['AREACD'] == 'E07000165') |     (metric['AREACD'] == 'E07000166') | (metric['AREACD'] == 'E07000167')| (metric['AREACD'] == 'E07000168') | (metric['AREACD'] == 'E07000169')]
    transport_n_yorkshire['Population'] = np.where(transport_n_yorkshire['AREACD']=='E07000163', 55409,
                                    np.where(transport_n_yorkshire['AREACD']=='E07000164', 89140,
                                             np.where(transport_n_yorkshire['AREACD']=='E07000165', 157869,
                                                      np.where(transport_n_yorkshire['AREACD']=='E07000166', 51965,
                                                               np.where(transport_n_yorkshire['AREACD']=='E07000167', 51751,
                                                                        np.where(transport_n_yorkshire['AREACD']=='E07000168', 108793,
                                                                                 np.where(transport_n_yorkshire["AREACD"]=='E07000169', 83449, 0)))))))
    transport_n_yorkshire['Prop_of_pop'] = transport_n_yorkshire['Population'] / transport_n_yorkshire['Population'].sum()
    transport_n_yorkshire['Scaled_Value'] = transport_n_yorkshire['Value'] * transport_n_yorkshire['Prop_of_pop']
    transport_n_yorkshire['Value'] = transport_n_yorkshire['Scaled_Value'].sum()
    transport_n_yorkshire['AREACD'] = 'E06000065'
    transport_n_yorkshire = transport_n_yorkshire.filter(items=['AREACD', 'Indicator', 'Value'])
    transport_n_yorkshire = transport_n_yorkshire.drop_duplicates()   
    
    #Cumbria: Cumberland and Westmorland and Furness
    
    transport_cumberland = metric[(metric['AREACD'] == 'E07000026') | (metric['AREACD'] == 'E07000028') | (metric['AREACD'] == 'E07000029')]
    transport_cumberland['Population'] = np.where(transport_cumberland['AREACD']=='E07000026', 96422,
                                                  np.where(transport_cumberland['AREACD']=='E07000028', 107524,
                                                           np.where(transport_cumberland['AREACD']=='E07000029',70603, 0)))
                                                               
    transport_cumberland['Prop_of_pop'] = transport_cumberland['Population'] / transport_cumberland['Population'].sum()
    transport_cumberland['Scaled_Value'] = transport_cumberland['Value'] * transport_cumberland['Prop_of_pop']
    transport_cumberland['Value'] = transport_cumberland['Scaled_Value'].sum()
    transport_cumberland['AREACD'] = 'E06000063'
    transport_cumberland = transport_cumberland.filter(items=['AREACD', 'Indicator', 'Value'])
    transport_cumberland = transport_cumberland.drop_duplicates()
                                                               
    transport_wf = metric[(metric['AREACD'] == 'E07000030') | (metric['AREACD'] == 'E07000031') | (metric['AREACD'] == 'E07000027')]
    transport_wf['Population'] = np.where(transport_wf['AREACD']=='E07000030', 52564,
                                                  np.where(transport_wf['AREACD']=='E07000031', 103658,
                                                           np.where(transport_wf['AREACD']=='E07000027',69087, 0)))
                                                               
    transport_wf['Prop_of_pop'] = transport_wf['Population'] / transport_wf['Population'].sum()
    transport_wf['Scaled_Value'] = transport_wf['Value'] * transport_wf['Prop_of_pop']
    transport_wf['Value'] = transport_wf['Scaled_Value'].sum()
    transport_wf['AREACD'] = 'E06000064'
    transport_wf = transport_wf.filter(items=['AREACD', 'Indicator', 'Value'])
    transport_wf = transport_wf.drop_duplicates()    
                                                

    #Merge 'new' boundaries into transport dataset and renaming older Scotland LAs
    transport_LAD21 = metric.merge(transport_bcp, how="outer")
    transport_LAD21 = transport_LAD21.merge(transport_dorset, how="outer")
    transport_LAD21 = transport_LAD21.merge(transport_buck, how="outer")
    transport_LAD21 = transport_LAD21.merge(transport_north_north, how="outer")
    transport_LAD21 = transport_LAD21.merge(transport_west_north, how="outer")
    transport_LAD21 = transport_LAD21.merge(transport_e_suffolk, how="outer")
    transport_LAD21 = transport_LAD21.merge(transport_w_suffolk, how="outer")
    transport_LAD21 = transport_LAD21.merge(transport_somerset, how="outer")
    transport_LAD21 = transport_LAD21.merge(transport_n_yorkshire, how="outer")
    transport_LAD21 = transport_LAD21.merge(transport_cumberland, how="outer")
    transport_LAD21 = transport_LAD21.merge(transport_wf, how="outer") 
    transport_LAD21['AREACD'] = np.where(transport_LAD21['AREACD']=='E06000048', 'E06000057', 
                                   np.where(transport_LAD21['AREACD']=='E07000100', 'E07000240',
                                            np.where(transport_LAD21['AREACD']=='E07000104', 'E07000241',
                                                     np.where(transport_LAD21['AREACD']=='E07000097', 'E07000242',
                                                              np.where(transport_LAD21['AREACD']=='E07000101', 'E07000243',
                                                                       np.where(transport_LAD21['AREACD']=='E08000020', 'E08000037', transport_LAD21['AREACD']))))))
    return(transport_LAD21) 

#SMOKING FUNCTION RECODE - function to convert smoking metric at 2019 boundaries to LAD21
def covert_smoking_LAD21(metric):
    import numpy as np

    from google.cloud import bigquery
    import pandas as pd
    client = bigquery.Client(location=" europe-west2")

    query = """
    SELECT *
    FROM `ons-luda-data-prod.ingest_luda.LAD_to_Country_Apr2021_UK` 
    
    """
    query_job = client.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="europe-west2",
    )  # API request - starts the query
    LAD21 = query_job.to_dataframe().filter(items=['LAD21CD', 'LAD21NM'])
    
    #Buckinghamshire -- might not need to include Buckinghamshire, data already has Buckinghamshire and E07000004, E07000005, E07000006, and E07000007 are NAs
    smoking_buck = metric[(metric['AREACD'] == 'E07000004') | (metric['AREACD'] == 'E07000005') | (metric['AREACD'] == 'E07000006') | (metric['AREACD'] == 'E07000007')]
    smoking_buck['Population'] = np.where(smoking_buck['AREACD']=='E07000004', 174137,
                                     np.where(smoking_buck['AREACD']=='E07000005', 92635,
                                             np.where(smoking_buck['AREACD']=='E07000006', 66867,
                                                     np.where(smoking_buck['AREACD']=='E07000007', 171644, 0))))
    smoking_buck['Prop_of_pop'] = smoking_buck['Population'] / smoking_buck['Population'].sum()
    smoking_buck['Scaled_Value'] = smoking_buck['Value'] * smoking_buck['Prop_of_pop']
    smoking_buck['Value'] = smoking_buck['Scaled_Value'].sum()
    smoking_buck['AREACD'] = 'E06000060'
    smoking_buck = smoking_buck.filter(items=['AREACD', 'Indicator', 'Value'])
    smoking_buck = smoking_buck.drop_duplicates()
    
    #North Northamptonshire smoking subset
    smoking_north_north = metric[(metric['AREACD'] == 'E07000150') | (metric['AREACD'] == 'E07000152') | (metric['AREACD'] == 'E07000153') | (metric['AREACD'] == 'E07000156')]
    smoking_north_north['Population'] = np.where(smoking_north_north['AREACD']=='E07000150', 61255,
                                     np.where(smoking_north_north['AREACD']=='E07000152', 86765,
                                             np.where(smoking_north_north['AREACD']=='E07000153', 93475,
                                                     np.where(smoking_north_north['AREACD']=='E07000156', 75356, 0))))
    smoking_north_north['Prop_of_pop'] = smoking_north_north['Population'] / smoking_north_north['Population'].sum()
    smoking_north_north['Scaled_Value'] = smoking_north_north['Value'] * smoking_north_north['Prop_of_pop']
    smoking_north_north['Value'] = smoking_north_north['Scaled_Value'].sum()
    smoking_north_north['AREACD'] = 'E06000061'
    smoking_north_north = smoking_north_north.filter(items=['AREACD', 'Indicator', 'Value'])
    smoking_north_north = smoking_north_north.drop_duplicates()

    #West Northamptonshire smoking subset
    smoking_west_north = metric[(metric['AREACD'] == 'E07000151') | (metric['AREACD'] == 'E07000154') | (metric['AREACD'] == 'E07000155')]
    smoking_west_north['Population'] = np.where(smoking_west_north['AREACD']=='E07000151', 77843,
                                     np.where(smoking_west_north['AREACD']=='E07000154', 212069,
                                             np.where(smoking_west_north['AREACD']=='E07000155', 85189, 0)))
    smoking_west_north['Prop_of_pop'] = smoking_west_north['Population'] / smoking_west_north['Population'].sum()
    smoking_west_north['Scaled_Value'] = smoking_west_north['Value'] * smoking_west_north['Prop_of_pop']
    smoking_west_north['Value'] = smoking_west_north['Scaled_Value'].sum()
    smoking_west_north['AREACD'] = 'E06000062'
    smoking_west_north = smoking_west_north.filter(items=['AREACD', 'Indicator', 'Value'])
    smoking_west_north = smoking_west_north.drop_duplicates()
 
    #2023 boundaries
                                                                              
    #North Yorkshire
    smoking_n_yorkshire = metric[(metric['AREACD'] == 'E07000163') | (metric['AREACD'] == 'E07000164') | (metric['AREACD'] == 'E07000165')| (metric['AREACD'] == 'E07000166') | (metric['AREACD'] == 'E07000167')| (metric['AREACD'] == 'E07000168') | (metric['AREACD'] == 'E07000169')]
    smoking_n_yorkshire['Population'] = np.where(smoking_n_yorkshire['AREACD']=='E07000163', 55409,
                                                   np.where(smoking_n_yorkshire['AREACD']=='E07000164', 89140,
                                                            np.where(smoking_n_yorkshire['AREACD']=='E07000165', 157869,
                                                                     np.where(smoking_n_yorkshire['AREACD']=='E07000166', 51965,
                                                                              np.where(smoking_n_yorkshire['AREACD']=='E07000167', 51751, 
                                                                                       np.where(smoking_n_yorkshire['AREACD']=='E07000168', 108793, 
                                                                                                np.where(smoking_n_yorkshire['AREACD']=='E07000169',83449, 0)))))))
    smoking_n_yorkshire['Prop_of_pop'] = smoking_n_yorkshire['Population'] / smoking_n_yorkshire['Population'].sum()
    smoking_n_yorkshire['Scaled_Value'] = smoking_n_yorkshire['Value'] * smoking_n_yorkshire['Prop_of_pop']
    smoking_n_yorkshire['Value'] = smoking_n_yorkshire['Scaled_Value'].sum()
    smoking_n_yorkshire['AREACD'] = 'E06000065'
    smoking_n_yorkshire = smoking_n_yorkshire.filter(items=['AREACD', 'Indicator', 'Value'])
    smoking_n_yorkshire = smoking_n_yorkshire.drop_duplicates()
                                                                                                                                                     
                                        
    #Cumbria: Cumberland and Westmorland and Furness
    
    smoking_cumberland = metric[(metric['AREACD'] == 'E07000026') | (metric['AREACD'] == 'E07000028') | (metric['AREACD'] == 'E07000029')]
    smoking_cumberland['Population'] = np.where(smoking_cumberland['AREACD']=='E07000026', 96422,
                                                  np.where(smoking_cumberland['AREACD']=='E07000028', 107524,
                                                           np.where(smoking_cumberland['AREACD']=='E07000029',70603, 0)))
                                                               
    smoking_cumberland['Prop_of_pop'] = smoking_cumberland['Population'] / smoking_cumberland['Population'].sum()
    smoking_cumberland['Scaled_Value'] = smoking_cumberland['Value'] * smoking_cumberland['Prop_of_pop']
    smoking_cumberland['Value'] = smoking_cumberland['Scaled_Value'].sum()
    smoking_cumberland['AREACD'] = 'E06000063'
    smoking_cumberland = smoking_cumberland.filter(items=['AREACD', 'Indicator', 'Value'])
    smoking_cumberland = smoking_cumberland.drop_duplicates()
                                                               
    smoking_wf = metric[(metric['AREACD'] == 'E07000030') | (metric['AREACD'] == 'E07000031') | (metric['AREACD'] == 'E07000027')]
    smoking_wf['Population'] = np.where(smoking_wf['AREACD']=='E07000030', 52564,
                                                  np.where(smoking_wf['AREACD']=='E07000031', 103658,
                                                           np.where(smoking_wf['AREACD']=='E07000027',69087, 0)))
                                                               
    smoking_wf['Prop_of_pop'] = smoking_wf['Population'] / smoking_wf['Population'].sum()
    smoking_wf['Scaled_Value'] = smoking_wf['Value'] * smoking_wf['Prop_of_pop']
    smoking_wf['Value'] = smoking_wf['Scaled_Value'].sum()
    smoking_wf['AREACD'] = 'E06000064'
    smoking_wf = smoking_wf.filter(items=['AREACD', 'Indicator', 'Value'])
    smoking_wf = smoking_wf.drop_duplicates()   
        
    #Somerset (West Somerset and Taunton Deane 2011 census populations summed up)
                                                                              
    smoking_somerset = metric[(metric['AREACD'] == 'E07000187') | (metric['AREACD'] == 'E07000188') | (metric['AREACD'] == 'E07000189')| (metric['AREACD'] == 'E07000246')]
    smoking_somerset['Population'] = np.where(smoking_somerset['AREACD']=='E07000187', 109279,
                                                  np.where(smoking_somerset['AREACD']=='E07000188', 114588,
                                                           np.where(smoking_somerset['AREACD']=='E07000189',161243, 
                                                                    np.where(smoking_somerset['AREACD']=='E07000246',144862,0))))
                                                               
    smoking_somerset['Prop_of_pop'] = smoking_somerset['Population'] / smoking_somerset['Population'].sum()
    smoking_somerset['Scaled_Value'] = smoking_somerset['Value'] * smoking_somerset['Prop_of_pop']
    smoking_somerset['Value'] = smoking_somerset['Scaled_Value'].sum()
    smoking_somerset['AREACD'] = 'E06000066'
    smoking_somerset = smoking_somerset.filter(items=['AREACD', 'Indicator', 'Value'])
    smoking_somerset = smoking_somerset.drop_duplicates()                                                                        

    #Merge 'new' boundaries into smoking dataset and renaming older Scotland LAs
    smoking_LAD21 = smoking.merge(smoking_buck, how="outer")
    smoking_LAD21 = smoking_LAD21.merge(smoking_north_north, how="outer")
    smoking_LAD21 = smoking_LAD21.merge(smoking_west_north, how="outer")
    smoking_LAD21 = smoking_LAD21.merge(smoking_n_yorkshire, how="outer")
    smoking_LAD21 = smoking_LAD21.merge(smoking_cumberland, how="outer")
    smoking_LAD21 = smoking_LAD21.merge(smoking_wf, how="outer")
    smoking_LAD21 = smoking_LAD21.merge(smoking_somerset, how="outer")
    smoking_LAD21['AREACD'] = np.where(smoking_LAD21['AREACD']=='S12000044', 'S12000050', 
                                   np.where(smoking_LAD21['AREACD']=='S12000046', 'S12000049', smoking_LAD21['AREACD']))
                                                                              
    return(smoking_LAD21)

#Function to convert data (upper tier only) to LAD21 - buckinghamshire and northamptonshire UAs
def convert_ut_20(metric):
    import numpy as np

    from google.cloud import bigquery
    import pandas as pd
    client = bigquery.Client(location=" europe-west2")

    query = """
    SELECT *
    FROM `ons-luda-data-prod.ingest_luda.LAD_to_Country_Apr2021_UK` 
    
    """
    query_job = client.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="europe-west2",
    )  # API request - starts the query
    LAD21 = query_job.to_dataframe().filter(items=['LAD21CD', 'LAD21NM'])

    metric_LAD21 = metric.merge(LAD21, left_on = "AREACD", right_on = "LAD21CD", how="outer")
    metric_LAD21['AREACD'] = metric_LAD21['AREACD'].fillna(metric_LAD21.pop('LAD21CD'))
    metric_LAD21['Indicator'] = metric_LAD21['Indicator'].interpolate(method='pad')
    metric_LAD21 = metric_LAD21.filter(items=['AREACD', 'Indicator', 'Value'])

    metric_LAD21_index = metric_LAD21.set_index('AREACD')
    metric_E10000002 = metric_LAD21_index.at['E10000002', 'Value']
    metric_E10000021 = metric_LAD21_index.at['E10000021', 'Value']
    metric_LAD21['E10000002'] = metric_E10000002
    metric_LAD21['E10000021'] = metric_E10000021

    metric_LAD21['Value'] = np.where(metric_LAD21['AREACD']=='E06000060', metric_LAD21['E10000002'], metric_LAD21['Value'])
    metric_LAD21['Value'] = np.where(metric_LAD21['AREACD']=='E06000061', metric_LAD21['E10000021'], metric_LAD21['Value'])
    metric_LAD21['Value'] = np.where(metric_LAD21['AREACD']=='E06000062', metric_LAD21['E10000021'], metric_LAD21['Value'])
    metric_LAD21 = metric_LAD21.filter(items=['AREACD', 'Indicator', 'Value'])
    return(metric_LAD21)

#Function to convert data (upper tier only) to LAD21 - northamptonshire UA only
def convert_ut_20_northamptonshire(metric):
    import numpy as np

    from google.cloud import bigquery
    import pandas as pd
    client = bigquery.Client(location=" europe-west2")

    query = """
    SELECT *
    FROM `ons-luda-data-prod.ingest_luda.LAD_to_Country_Apr2021_UK` 
    
    """
    query_job = client.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="europe-west2",
    )  # API request - starts the query
    LAD21 = query_job.to_dataframe().filter(items=['LAD21CD', 'LAD21NM'])

    metric_LAD21 = metric.merge(LAD21, left_on = "AREACD", right_on = "LAD21CD", how="outer")
    metric_LAD21['AREACD'] = metric_LAD21['AREACD'].fillna(metric_LAD21.pop('LAD21CD'))
    metric_LAD21['Indicator'] = metric_LAD21['Indicator'].interpolate(method='pad')
    metric_LAD21 = metric_LAD21.filter(items=['AREACD', 'Indicator', 'Value'])

    metric_LAD21_index = metric_LAD21.set_index('AREACD')
    metric_E10000021 = metric_LAD21_index.at['E10000021', 'Value']
    metric_LAD21['E10000021'] = metric_E10000021

    metric_LAD21['Value'] = np.where(metric_LAD21['AREACD']=='E06000061', metric_LAD21['E10000021'], metric_LAD21['Value'])
    metric_LAD21['Value'] = np.where(metric_LAD21['AREACD']=='E06000062', metric_LAD21['E10000021'], metric_LAD21['Value'])
    metric_LAD21 = metric_LAD21.filter(items=['AREACD', 'Indicator', 'Value'])
    return(metric_LAD21)
 
                                              
 #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                             


#Function to all metrics using 2022 boundaries to 2023 boundaries - uses Census 2021 population
                                              
                                              
def convert_LAD22_to_LAD23(metric):
    import numpy as np

    from google.cloud import bigquery
    import pandas as pd
    client = bigquery.Client(location=" europe-west2")

    query = """
    SELECT *
    FROM `ons-luda-data-prod.ingest_luda.LAD_to_Country_Apr2021_UK` 
    
    """
    query_job = client.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="europe-west2",
    )  # API request - starts the query
    LAD21 = query_job.to_dataframe().filter(items=['LAD21CD', 'LAD21NM'])
    
                                                                              
    #North Yorkshire
    LAD23_n_yorkshire = metric[(metric['AREACD'] == 'E07000163') | (metric['AREACD'] == 'E07000164') | (metric['AREACD'] == 'E07000165')| (metric['AREACD'] == 'E07000166') | (metric['AREACD'] == 'E07000167')| (metric['AREACD'] == 'E07000168') | (metric['AREACD'] == 'E07000169')]
    LAD23_n_yorkshire['Population'] = np.where(LAD23_n_yorkshire['AREACD']=='E07000163', 56927,
                                                   np.where(LAD23_n_yorkshire['AREACD']=='E07000164', 90690,
                                                            np.where(LAD23_n_yorkshire['AREACD']=='E07000165', 162666,
                                                                     np.where(LAD23_n_yorkshire['AREACD']=='E07000166', 49776,
                                                                              np.where(LAD23_n_yorkshire['AREACD']=='E07000167', 54707, 
                                                                                       np.where(LAD23_n_yorkshire['AREACD']=='E07000168', 108736, 
                                                                                                np.where(LAD23_n_yorkshire['AREACD']=='E07000169',91988, 0)))))))
    LAD23_n_yorkshire['Prop_of_pop'] = LAD23_n_yorkshire['Population'] / LAD23_n_yorkshire['Population'].sum()
    LAD23_n_yorkshire['Scaled_Value'] = LAD23_n_yorkshire['Value'] * LAD23_n_yorkshire['Prop_of_pop']
    LAD23_n_yorkshire['Value'] = LAD23_n_yorkshire['Scaled_Value'].sum()
    LAD23_n_yorkshire['AREACD'] = 'E06000065'
    LAD23_n_yorkshire = LAD23_n_yorkshire.filter(items=['AREACD', 'Indicator', 'Value'])
    LAD23_n_yorkshire = LAD23_n_yorkshire.drop_duplicates()
                                                                                                                                                     
                                        
    #Cumbria: Cumberland and Westmorland and Furness
    
    LAD23_cumberland = metric[(metric['AREACD'] == 'E07000026') | (metric['AREACD'] == 'E07000028') | (metric['AREACD'] == 'E07000029')]
    LAD23_cumberland['Population'] = np.where(LAD23_cumberland['AREACD']=='E07000026', 96157,
                                                  np.where(LAD23_cumberland['AREACD']=='E07000028', 110024,
                                                           np.where(LAD23_cumberland['AREACD']=='E07000029',67076, 0)))
                                                               
    LAD23_cumberland['Prop_of_pop'] = LAD23_cumberland['Population'] / LAD23_cumberland['Population'].sum()
    LAD23_cumberland['Scaled_Value'] = LAD23_cumberland['Value'] * LAD23_cumberland['Prop_of_pop']
    LAD23_cumberland['Value'] = LAD23_cumberland['Scaled_Value'].sum()
    LAD23_cumberland['AREACD'] = 'E06000063'
    LAD23_cumberland = LAD23_cumberland.filter(items=['AREACD', 'Indicator', 'Value'])
    LAD23_cumberland = LAD23_cumberland.drop_duplicates()
                                                               
    LAD23_wf = metric[(metric['AREACD'] == 'E07000030') | (metric['AREACD'] == 'E07000031') | (metric['AREACD'] == 'E07000027')]
    LAD23_wf['Population'] = np.where(LAD23_wf['AREACD']=='E07000030', 54735,
                                                  np.where(LAD23_wf['AREACD']=='E07000031', 104450,
                                                           np.where(LAD23_wf['AREACD']=='E07000027',67407, 0)))
                                                               
    LAD23_wf['Prop_of_pop'] = LAD23_wf['Population'] / LAD23_wf['Population'].sum()
    LAD23_wf['Scaled_Value'] = LAD23_wf['Value'] * LAD23_wf['Prop_of_pop']
    LAD23_wf['Value'] = LAD23_wf['Scaled_Value'].sum()
    LAD23_wf['AREACD'] = 'E06000064'
    LAD23_wf = LAD23_wf.filter(items=['AREACD', 'Indicator', 'Value'])
    LAD23_wf = LAD23_wf.drop_duplicates()   
        
    #Somerset
                                                                              
    LAD23_somerset = metric[(metric['AREACD'] == 'E07000187') | (metric['AREACD'] == 'E07000188') | (metric['AREACD'] == 'E07000189')| (metric['AREACD'] == 'E07000246')]
    LAD23_somerset['Population'] = np.where(LAD23_somerset['AREACD']=='E07000187', 116089,
                                                  np.where(LAD23_somerset['AREACD']=='E07000188', 125343,
                                                           np.where(LAD23_somerset['AREACD']=='E07000189',172671, 
                                                                    np.where(LAD23_somerset['AREACD']=='E07000246',157445,0))))
                                                               
    LAD23_somerset['Prop_of_pop'] = LAD23_somerset['Population'] / LAD23_somerset['Population'].sum()
    LAD23_somerset['Scaled_Value'] = LAD23_somerset['Value'] * LAD23_somerset['Prop_of_pop']
    LAD23_somerset['Value'] = LAD23_somerset['Scaled_Value'].sum()
    LAD23_somerset['AREACD'] = 'E06000066'
    LAD23_somerset = LAD23_somerset.filter(items=['AREACD', 'Indicator', 'Value'])
    LAD23_somerset = LAD23_somerset.drop_duplicates()                                                                        

    #Merge 'new' boundaries into smoking dataset 
    LAD23 = metric.merge(LAD23_n_yorkshire, how="outer")
    LAD23 = LAD23.merge(LAD23_cumberland, how="outer")
    LAD23 = LAD23.merge(LAD23_wf, how="outer")
    LAD23 = LAD23.merge(LAD23_somerset, how="outer")
                                                                         
    return(LAD23) 
                                           
#Function to convert data (upper tier only) to LAD23 - North Yorkshire, Somerset, Cumberland and Westmorland and Furness UAs
def convert_ut_23(metric):
    import numpy as np

    from google.cloud import bigquery
    import pandas as pd
    client = bigquery.Client(location=" europe-west2")

    query = """
    SELECT *
    FROM `ons-luda-data-prod.ingest_luda.LAD_to_Country_Apr2021_UK` 
    
    """
    query_job = client.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="europe-west2",
    )  # API request - starts the query
    LAD23 = query_job.to_dataframe().filter(items=['LAD21CD', 'LAD21NM'])

    metric_LAD23 = metric.merge(LAD23, left_on = "AREACD", right_on = "LAD21CD", how="outer")
    metric_LAD23['AREACD'] = metric_LAD23['AREACD'].fillna(metric_LAD23.pop('LAD21CD'))
    metric_LAD23['Indicator'] = metric_LAD23['Indicator'].interpolate(method='pad')
    metric_LAD23 = metric_LAD23.filter(items=['AREACD', 'Indicator', 'Value'])

    metric_LAD23_index = metric_LAD23.set_index('AREACD')
    metric_E10000006 = metric_LAD23_index.at['E10000006', 'Value'] #cumbria
    metric_E10000023 = metric_LAD23_index.at['E10000023', 'Value'] #north yorkshire
    metric_E10000027 = metric_LAD23_index.at['E10000027', 'Value'] #somerset
    metric_LAD23['E10000006'] = metric_E10000006 
    metric_LAD23['E10000023'] = metric_E10000023
    metric_LAD23['E10000027'] = metric_E10000027                                       

    metric_LAD23['Value'] = np.where(metric_LAD23['AREACD']=='E06000063', metric_LAD23['E10000006'], metric_LAD23['Value'])
    metric_LAD23['Value'] = np.where(metric_LAD23['AREACD']=='E06000064', metric_LAD23['E10000006'], metric_LAD23['Value'])
    metric_LAD23['Value'] = np.where(metric_LAD23['AREACD']=='E06000065', metric_LAD23['E10000023'], metric_LAD23['Value'])
    metric_LAD23['Value'] = np.where(metric_LAD23['AREACD']=='E06000066', metric_LAD23['E10000027'], metric_LAD23['Value'])
    metric_LAD23 = metric_LAD23.filter(items=['AREACD', 'Indicator', 'Value'])
    return(metric_LAD23)

#Creating new metrics at LAD21 and LAD23 level using above functions for use in analysis
#2021
travel_public_LAD21 = covert_transport_LAD21(travel_public)
travel_car_LAD21 = covert_transport_LAD21(travel_car)
travel_bike_LAD21 = covert_transport_LAD21(travel_bike)
absenses_LAD21 = convert_ut_20(absenses)
absenses_cla_LAD21 = convert_ut_20_northamptonshire(absenses_cla)
smoking_LAD21 = covert_smoking_LAD21(smoking)

#List of metrics using 2022 boundaries at Lower Tier geographies - might be able to remove OHID datasets from list if we ingest them with 2023 boundaries
                                              
to_convert_lt = ["gva", "weekly_pay", "employ_rate", "gdi", "broadband", "mobile4g", "ks2", "maths_5", "lit_5", "comm_5", "gcses", "good_schools", "absenses", "absenses_fsm", "absenses_cla", "fe_achievements", "app_start", "app_completion", "nvq", "fe_participation", "female_hle", "male_hle", "adult_obesity", "child_obesity", "year6_obesity", "cancer", "cardio", "satisfaction", "anxiety", "happiness", "worthwhile", "new_houses"]


#Loop to convert all datasets using 2022 boundaries to 2023 - Lower Tier 
                                           
#d = {}
#for luda_metric in to_convert_lt: 
#    d[luda_metric] = convert_LAD22_to_LAD23(to_convert_lt)


gva_LAD23 = convert_LAD22_to_LAD23(gva)
weekly_pay_LAD23 = convert_LAD22_to_LAD23(weekly_pay)                                        
employ_rate_LAD23 = convert_LAD22_to_LAD23(employ_rate)                                        
gdi_LAD23 = convert_LAD22_to_LAD23(gdi) 
broadband_LAD23 = convert_LAD22_to_LAD23(broadband)
mobile4g_LAD23 = convert_LAD22_to_LAD23(mobile4g)
good_schools_LAD23 = convert_LAD22_to_LAD23(good_schools)
app_start_LAD23 = convert_LAD22_to_LAD23(app_start) #nulls for DAs
app_completion_LAD23 = convert_LAD22_to_LAD23(app_completion) ##nulls for DAs
nvq_LAD23 = convert_LAD22_to_LAD23(nvq) 
fe_participation_LAD23 = convert_LAD22_to_LAD23(fe_participation) ##nulls for DAs
cancer_LAD23 = convert_LAD22_to_LAD23(cancer) #nulls for the DAs
cardio_LAD23 = convert_LAD22_to_LAD23(cardio) #nulls for the DAs
satisfaction_LAD23 = convert_LAD22_to_LAD23(satisfaction)
anxiety_LAD23 = convert_LAD22_to_LAD23(anxiety)
happiness_LAD23 = convert_LAD22_to_LAD23(happiness)
worthwhile_LAD23 = convert_LAD22_to_LAD23(worthwhile)
new_houses_LAD23 = convert_LAD22_to_LAD23(new_houses) #nulls for the DAs

#2023 boundaries conversion - Upper Tier          
ks2_LAD23 = convert_ut_23(ks2) #nulls for DAs and 07s
good_schools_LAD23 = convert_ut_23(good_schools_LAD23)
absenses_LAD23 = convert_ut_23(absenses)
absenses_cla_LAD23 = convert_ut_23(absenses_cla)
fe_achievements_LAD23 = convert_ut_23(fe_achievements)
app_start_LAD23 = convert_ut_23(app_start_LAD23)
app_completion_LAD23 = convert_ut_23(app_completion_LAD23)
fe_participation_LAD23 = convert_ut_23(fe_participation_LAD23)
female_hle_LAD23 = convert_ut_23(female_hle)
male_hle_LAD23 = convert_ut_23(male_hle)
cancer_LAD23 = convert_ut_23(cancer_LAD23)
cardio_LAD23 = convert_ut_23(cardio_LAD23)
satisfaction_LAD23 = convert_ut_23(satisfaction_LAD23)
anxiety_LAD23 = convert_ut_23(anxiety_LAD23)
happiness_LAD23 = convert_ut_23(happiness_LAD23)
worthwhile_LAD23 = convert_ut_23(worthwhile_LAD23)
new_houses_LAD23 = convert_ut_23(new_houses_LAD23)

#transport metrics UTLA 2023
travel_public_LAD21 = convert_ut_23(travel_public_LAD21)
travel_car_LAD21 = convert_ut_23(travel_car_LAD21)
travel_bike_LAD21 = convert_ut_23(travel_bike_LAD21)