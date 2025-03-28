from typing import Dict
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from src.utils.utils import get_table_from_path


def relabel_clusters(
        clusters: pd.DataFrame, 
        model, 
        output: str='clusters'
) -> pd.DataFrame:
    """Re-labels clusters based on performance.

    Parameters
    ----------
    clusters
        DataFrame containing clusters.
    model
        Instance of the clustering model.
    output
        Specifies what to output. Options are 'clusters' and 'ranks'.

    Returns
    -------
    clusters: pd.Dataframe
    DataFrame containing re-labelled clusters, or ranks if specified.
    """
    centres = model.cluster_centers_
    performance = list(pd.DataFrame(centres)[0])
    indices = list(range(len(performance)))
    indices.sort(key=(lambda x: performance[x]))
    ranking = [0] * len(indices)
    for i, x in enumerate(indices):
        ranking[x] = i
    else:
        if output == "ranks":
            return indices
        clusters["Cluster"] = clusters["Cluster"].apply(lambda x: ranking[x])
        return clusters


def cluster_table(
        loaded_config: Dict, 
        clusters_table: pd.DataFrame
) -> pd.DataFrame:
    """Gets local authority area codes and area names.

    Parameters
    ----------
    loaded_config
        Contains the loaded config.
    cols_to_select
        Columns that should be selected from the table.
    table_name
        Name of the table containing the data.
    rename_mapper
        Mapping of old_column: new_column for any columns that must be renamed.

    Returns
    -------
    cluster_table: pd.Dataframe
    DataFrame with selected columns.
    """
    Area_names = get_table_from_path(table_name=(loaded_config["Geog_mapper"]),
      path=(loaded_config["inputs_file_path"]),
      create_geodataframe=False,
      cols_to_select=[loaded_config["Area_col"], loaded_config["Area_name_col"]])
    cluster_table = clusters_table.merge(Area_names, right_on=(loaded_config["Area_col"]), left_on="AREACD", how="left")
    cluster_table = cluster_table[["AREACD", loaded_config["Area_name_col"], "Cluster"]]
    return cluster_table



def make_clustering_model(
    metrics: pd.DataFrame, 
    loaded_config: Dict, 
    seed: int=19042022, 
    n_init: int=10, 
    min_k: int=4, 
    max_k: int=15,
):
    """
    

    Parameters
    ----------
    metrics : pd.DataFrame
        Winsorized and processed data.
    loaded_config : Dict
        loaded config name.
    seed : int, optional
        Seed. The default is 19042022.
    n_init : int, optional
        number of random seed initialisations. The default is 10.
    min_k : int, optional
        Minimum number of clusters. The default is 4.
    max_k : int, optional
        Maximum number of clusters. The default is 15.

    Returns
    -------
    best_clusters: pd.dataframe
        geodataframe including cluster allocation and mapping infromation    
    cluster_centers: np.array
        array of cluster centres used for the radar plot
    sil_data_df: pd.dataframe.
        dataframe including the silhouette score, supplementary model information.
    """
    np.random.seed(seed=seed)
    best_k = 0
    best_sil = 0
    min_k = min_k
    max_k = max_k
    metrics_indexed = metrics.set_index("AREACD")
    
    #optimises the number of clusters over the specified range
    for k in range(min_k, max_k):
        np.random.seed(seed=seed)
        model = KMeans(n_clusters=k, n_init=n_init, max_iter=300)
        no_na_metrics = metrics_indexed[metrics_indexed.notna().all(axis=1)]
        scaler = StandardScaler()
        metrics_scaled = scaler.fit_transform(no_na_metrics)
        model.fit(metrics_scaled)
        clusters = pd.DataFrame(no_na_metrics.reset_index("AREACD"))
        clusters["Cluster"] = model.labels_
        labels = model.fit_predict(no_na_metrics)
        sil = silhouette_score(no_na_metrics, labels)
        if sil > best_sil:
            best_sil = sil
            best_k = k
    np.random.seed(seed=seed)
    
    #specifies the best model using optomised k
    best_model = KMeans(n_clusters=best_k, n_init=n_init, max_iter=300)
    no_na_metrics_best = metrics_indexed[metrics_indexed.notna().all(axis=1)]
    scaler = StandardScaler()
    metrics_scaled = scaler.fit_transform(no_na_metrics_best)
    best_model.fit(metrics_scaled)
    best_clusters = pd.DataFrame(no_na_metrics_best.reset_index()["AREACD"])
    best_clusters["Cluster"] = best_model.labels_
    
    #loads in the shapefile and stitches to cluster table
    la_geo = get_table_from_path(
            table_name=(loaded_config["shapefile"]),
            path=(loaded_config["inputs_file_path"]),
            cols_to_select=[loaded_config["shapefile_area_col"], "geometry", "BNG_E", "BNG_N"],
            create_geodataframe=True)
    best_clusters = la_geo.merge(best_clusters, right_on="AREACD", left_on=(loaded_config["shapefile_area_col"]), how="right")
    best_clusters = best_clusters.drop((loaded_config["shapefile_area_col"]), axis=1)
    
    #obtains the cluster centres
    best_clusters = relabel_clusters(best_clusters, best_model)
    cluster_centers = best_model.cluster_centers_
    labels = best_model.fit_predict(no_na_metrics_best)
    
    #gets the silhouette score and makes cluster table
    sil_score = silhouette_score(no_na_metrics_best, labels)
    sil_data = ["silhouette score", sil_score]
    sil_data_df = pd.DataFrame([sil_data], columns=["Measure", "Value"])
    return (best_clusters, cluster_centers, sil_data_df)


