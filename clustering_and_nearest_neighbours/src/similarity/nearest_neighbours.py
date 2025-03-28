from typing import Dict
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from src.utils.utils import get_table_from_path
from scipy.spatial.distance import cdist
from sklearn.neighbors import NearestNeighbors
import copy

def nearest_neighbours_filter_nesting(
        loaded_config: Dict,
        df: pd.DataFrame,
        cluster_df: pd.DataFrame,
        number: int,
        geography_col: str ="AREACD",
        distance_metric: str = "euclid",
):
    """
    

    Parameters
    ----------
    data : pd.DataFrame
        Processed data for all nearest neighbour areas.
    distance_metric : str
        Type of distance used
    number : int
        Number of neighbours required
    geography_col : str, optional
        Name of the geography column to index. The default is "".

    Returns
    -------
    None.

    """
    #Set index, replace NAs and standardise data
    geographies = list(df[geography_col])
    data = copy.deepcopy(df)
    data.set_index(geography_col, inplace=True)
    data.fillna(data.median(numeric_only= True), inplace= True)
    
    scaler = StandardScaler()
    data_scaled = scaler.fit_transform(data)
    
    #Create upper-tier 2 way dict to replace nesting geographies
    
    ut_to_lt_lookup = get_table_from_path(
        table_name=loaded_config["upper_tier_to_lower_tier_lookup"],
        path=loaded_config["inputs_file_path"],
        create_geodataframe=False,
        cols_to_select=[loaded_config["upper_tier_code_column_name"], loaded_config["lower_tier_code_column_name"]],
    )
    
    ut_to_lt_lookup = ut_to_lt_lookup.rename(columns={loaded_config["upper_tier_code_column_name"]: 'AREACD1',
                                    loaded_config["lower_tier_code_column_name"]: 'AREACD2'})
    ut_to_lt_lookup2=copy.deepcopy(ut_to_lt_lookup)
    
    ut_to_lt_lookup2 = ut_to_lt_lookup.rename(columns={'AREACD2': 'AREACD1',
                                    'AREACD1': 'AREACD2'})
    
    ut_to_lt_lookup = pd.concat([ut_to_lt_lookup, ut_to_lt_lookup2], ignore_index=True)
    ut_to_lt_lookup = ut_to_lt_lookup.rename(columns={'AREACD2': 'AREACD1',
                                    'AREACD1': 'AREACD'})
    
    ut_to_lt_lists = ut_to_lt_lookup.groupby('AREACD')['AREACD1'].apply(list).reset_index()
    ut_to_lt_lists = ut_to_lt_lists.rename(columns={'AREACD1': 'nesting_geogs'}) 

    #Generate distance matrix
    distances = cdist(data_scaled, data_scaled, distance_metric)
    distances_table = copy.deepcopy(distances)
    distances_table = pd.DataFrame(distances_table)
    distances = pd.DataFrame(distances, columns=geographies, index=geographies)
    
    #Create dataframe of closest points
    distances = distances.apply(lambda row: pd.Series([col for _, col in sorted(zip(row.values, distances.columns))], index=row.index), axis=1)
    
    distances = distances.rename(columns={x:y for x,y in zip(distances.columns,range(0,len(distances.columns)))})
    
    distances.reset_index(inplace=True)
    
    #Use UTLA dict to replace any nesting geographies
    distances = distances.rename(columns={'index': 'AREACD'}) 
    distances = pd.merge(distances,ut_to_lt_lists, on='AREACD', how='left')
    
    #Apply the dictionary to replace all nezting geographies with NAs
    def replace_not_in_list(row):
        for col in distances.columns:
            if isinstance(col,int):  # Check if column name is an integer
                if row[col] in row['nesting_geogs']:
                    row[col] = np.nan
        return row            
    distances = distances.apply(replace_not_in_list, axis=1)
    
    #Shift all NAs to the end of the dataset
    def shift_na_left(row):
        non_na = row.dropna().tolist()
        na_count = row.isna().sum()
        return pd.Series(non_na + [np.nan] * na_count)

    distances = distances.drop(0, axis = 1) 
    
    # Apply the function to each row
    distances = distances.apply(shift_na_left, axis=1)
    
    # Trim the dataset to the desired number of neighbours
    distances = distances.iloc[:, :number+1]
    
    return distances, distances_table

def nearest_neighbours(
        loaded_config: Dict,
        df: pd.DataFrame,
        cluster_df: pd.DataFrame,
        number: int,
        geography_col: str ="AREACD",
        distance_metric: str = "euclid",
):
    """
    

    Parameters
    ----------
    data : pd.DataFrame
        Processed data for all nearest neighbour areas.
    distance_metric : str
        Type of distance used
    number : int
        Number of neighbours required
    geography_col : str, optional
        Name of the geography column to index. The default is "".

    Returns
    -------
    None.

    """
    #Set index, replace NAs and standardise data
    geographies = list(df[geography_col])
    data = copy.deepcopy(df)
    data.set_index(geography_col, inplace=True)
    data.fillna(data.median(numeric_only= True), inplace= True)
    scaler = StandardScaler()
    data_scaled = scaler.fit_transform(data)

    #Generate distance matrix
    distances = cdist(data_scaled, data_scaled, distance_metric)
    distances_table = copy.deepcopy(distances)
    distances_table = pd.DataFrame(distances_table)
    distances = pd.DataFrame(distances, columns=geographies, index=geographies)
    
    #Create dataframe of closest points
    distances = distances.apply(lambda row: pd.Series([col for _, col in sorted(zip(row.values, distances.columns))], index=row.index), axis=1)
    distances = distances.rename(columns={x:y for x,y in zip(distances.columns,range(0,len(distances.columns)))})
    distances.reset_index(inplace=True)
    
    #Trim dataframe to desired number
    distances = distances.iloc[:, :number+1]
    
    return distances, distances_table


    
def find_optimal_neighbors(
        loaded_config: Dict,
        df:pd.DataFrame,
        Area_code: str="AREACD",
        max_neighbours:float=30):
    
        path=loaded_config["outputs_file_path"]
        df = df.set_index(Area_code)  
        df.fillna(0, inplace=True)
        scaler = StandardScaler()
        df_scaled = scaler.fit_transform(df)
        nbrs = NearestNeighbors(n_neighbors=max_neighbours).fit(df_scaled)
        distances, indices = nbrs.kneighbors(df_scaled)
        
        sorted_distances = np.sort(distances[:, -1])
        plt.figure(figsize=(10, 6))
        plt.plot(sorted_distances, marker='o', color='blue')
        plt.title("Elbow Method for Optimal k (Nearest Neighbors)")
        plt.xlabel("Data Points")
        plt.ylabel("Distance to k-th Neighbor")
        plt.grid()
        plt.savefig(f"{path}/Elbow Method for Optimal k (Nearest Neighbors).jpeg")
        return sorted_distances