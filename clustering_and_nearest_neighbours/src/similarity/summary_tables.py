from typing import Dict
import pandas as pd
from src.utils.utils import get_table_from_path


def clusters_summary_stats(
        table_metrics: pd.DataFrame, 
        clusters_table: pd.DataFrame, 
        stats: str
) -> pd.DataFrame:
    """Creates summary stats for each cluster.

    Parameters
    ----------
    table_metrics
        DataFrame containing metrics.
    clusters_table
        DataFrame containing clusters.
    stats
        String or list of strings with names of methods. Options are:
        'mean' - calculates the mean of each cluster
        'median' - calculates the median of each cluster

    Returns
    -------
    clusters_head_avg: pd.dataframe
    DataFrame containing groups aggregated according to specified stats.
    """
    table_metrics_rescale = table_metrics.rename_axis(None).reset_index(level=0)
    
    #subset clusters table and merge to metrics
    clusters_table = clusters_table[["AREACD", "Cluster"]]
    clusters_head_figures = table_metrics_rescale.merge(clusters_table, on="AREACD", how="left")
    clusters_head_figures = clusters_head_figures.drop("index", axis=1)
    clusters_head_figures = clusters_head_figures.drop("AREACD", axis=1)
    
    #Calculate overall average
    clusters_overall_avg = clusters_head_figures.agg(stats)
    clusters_overall_avg = pd.DataFrame(clusters_overall_avg)
    clusters_overall_avg = clusters_overall_avg.transpose()
    clusters_overall_avg["Average of all areas"] = "Average of all areas"
    clusters_overall_avg = clusters_overall_avg.set_index("Average of all areas")
    
    #Calculate cluster averages
    clusters_avg = clusters_head_figures.groupby(["Cluster"]).agg(stats)
    clusters_avg = pd.DataFrame(clusters_avg)
    
    #Create final table
    frames = [clusters_avg, clusters_overall_avg]
    clusters_head_avg = pd.concat(frames)
    clusters_head_avg = clusters_head_avg.drop("Cluster", axis=1)
    return clusters_head_avg


def ITL1_summary(
        loaded_config: Dict, 
        clusters_table: pd.DataFrame
) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    loaded_config : Dict
        Saved config file including parameters.
    clusters_table : pd.DataFrame
        A table including the cluster, area code and name.

    Returns
    -------
    ITL1_table: pd.dataframe
        A dataframe with the ITL1 composition of each cluster   

    """
    #Load in ITL1 lookup
    ITL1_lookup = get_table_from_path(table_name=(loaded_config["Geog_mapper"]),
      path=(loaded_config["inputs_file_path"]),
      create_geodataframe=False,
      cols_to_select=[loaded_config["ITL1_col"], loaded_config["Area_col"]])
    
    #Merge ITL1 lookup to cluster table and create ITL1 table
    ITL1_cluster_table = clusters_table.merge(ITL1_lookup, right_on=(loaded_config["Area_col"]), left_on="AREACD", how="left")
    ITL1_table = pd.crosstab(index=(ITL1_cluster_table["Cluster"]), columns=(ITL1_cluster_table[loaded_config["ITL1_col"]]))
    return ITL1_table
