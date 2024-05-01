from typing import Dict, Mapping, Sequence, Union
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import pandas as pd, numpy as np, seaborn as sns
from matplotlib.colors import ListedColormap
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_samples, silhouette_score
from utils import get_table_from_path

def metrics_to_table(
        metrics: Sequence[pd.DataFrame]
) -> pd.DataFrame:
    """Puts metrics into one pivot table.

    Parameters
    ----------
    metrics
        List of DataFrames, each containing a metric.

    Returns
    -------
    metrics: pd.dataframe
    DataFrame containing all metrics.
    """
    if len(metrics) > 0:
        metrics = pd.concat(metrics)
        cols = metrics["Indicator"].drop_duplicates()
        metrics = pd.pivot_table(metrics, values="Value", columns="Indicator", index="AREACD")[cols].reindex()
    return metrics


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
      run_locally=(loaded_config["run_locally"]),
      path=(loaded_config["local_file_path"]),
      create_geodataframe=False,
      cols_to_select=[loaded_config["Area_col"], loaded_config["Area_name_col"]],
      project_name=(loaded_config["gcp_project_name"]),
      dataset_name=(loaded_config["Geog_mapper"]),
      project_location=(loaded_config["gcp_project_location"]))
    cluster_table = clusters_table.merge(Area_names, right_on=(loaded_config["Area_col"]), left_on="AREACD", how="left")
    cluster_table = cluster_table[["AREACD", loaded_config["Area_name_col"], "Cluster"]]
    return cluster_table


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
    clusters_table = clusters_table[["AREACD", "Cluster"]]
    clusters_head_figures = clusters_table.merge(table_metrics_rescale, on="AREACD", how="left")
    clusters_head_figures = clusters_head_figures.drop("index", axis=1)
    clusters_head_figures = clusters_head_figures.drop("AREACD", axis=1)
    clusters_overall_avg = clusters_head_figures.agg(stats)
    clusters_overall_avg = pd.DataFrame(clusters_overall_avg)
    clusters_overall_avg = clusters_overall_avg.transpose()
    clusters_overall_avg["Average of all areas"] = "Average of all areas"
    clusters_overall_avg = clusters_overall_avg.set_index("Average of all areas")
    clusters_avg = clusters_head_figures.groupby(["Cluster"]).agg(stats)
    clusters_avg = pd.DataFrame(clusters_avg)
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
    ITL1_lookup = get_table_from_path(table_name=(loaded_config["Geog_mapper"]),
      run_locally=(loaded_config["run_locally"]),
      path=(loaded_config["local_file_path"]),
      create_geodataframe=False,
      cols_to_select=[loaded_config["ITL1_col"], loaded_config["Area_col"]],
      project_name=(loaded_config["gcp_project_name"]),
      dataset_name=(loaded_config["Geog_mapper"]),
      project_location=(loaded_config["gcp_project_location"]))
    ITL1_cluster_table = clusters_table.merge(ITL1_lookup, right_on=(loaded_config["Area_col"]), left_on="AREACD", how="left")
    ITL1_table = pd.crosstab(index=(ITL1_cluster_table["Cluster"]), columns=(ITL1_cluster_table[loaded_config["ITL1_col"]]))
    return ITL1_table


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
            run_locally=(loaded_config["run_locally"]),
            path=(loaded_config["local_file_path"]),
            cols_to_select=[loaded_config["shapefile_area_col"], "geometry", "BNG_E", "BNG_N"],
            create_geodataframe=True,
            project_name=(loaded_config["gcp_project_name"]),
            dataset_name="ingest_geography",
            project_location=(loaded_config["gcp_project_location"]))
    best_clusters = la_geo.merge(best_clusters, right_on="AREACD", left_on=(loaded_config["shapefile_area_col"]), how="right")
    best_clusters = best_clusters.drop((loaded_config["shapefile_area_col"]), axis=1)
    #obtains the cluster centres
    best_clusters = relabel_clusters(best_clusters, best_model)
    cluster_centers = best_model.cluster_centers_
    labels = best_model.fit_predict(no_na_metrics_best)
    #gets the silhouette score and makes df
    sil_score = silhouette_score(no_na_metrics_best, labels)
    sil_data = ["silhouette score", sil_score]
    sil_data_df = pd.DataFrame([sil_data], columns=["Measure", "Value"])
    return (best_clusters, cluster_centers, sil_data_df)


def cluster_map(
        clusters: pd.DataFrame,
        cmap: str='tab10'
):
    """
    

    Parameters
    ----------
    clusters : pd.DataFrame
        A geodataframe including the cluster number of each area
    cmap : str, optional
        Ability to specify colour scheme The default is 'tab10'.

    Returns
    -------
    str
        map of clusters, which is saved into outputs folder

    """
    clusters["Cluster"].apply(lambda x: int(x))
    n = len(pd.unique(clusters["Cluster"]))
    plot = clusters.plot(marker="-", column="Cluster", vmin=0,
      vmax=n,
      categorical=True,
      cmap=cmap,
      markersize=100,
      legend=True,
      figsize=(15, 15))
    plt.axis("off")
    plt.title("Cluster map", size=20, y=1.05)
    plt.savefig("Outputs/cluster_map.jpg")
    return "map saved"


def radar_plot(
        loaded_config: Dict,
        metrics: pd.DataFrame, 
        clusters: pd.DataFrame, 
        centres: np.array, 
        cmap: str='tab10'
):
    """
    

    Parameters
    ----------
    loaded_config : Dict
        Saved config file including parameters.
    metrics : pd.DataFrame
       The data table used for clustering.
    clusters : pd.DataFrame
       A table including the cluster of each area.
    centres : np.array
        cluster centres, output in the clustering function.
    cmap : str, optional
        desired cmap for the visualisation. The default is 'tab10'.

    Returns
    -------
    map saved into output folder.

    """
    analysis = clusters.merge(metrics, on="AREACD")
    performance = list(pd.DataFrame(centres)[0])
    indices = list(range(len(performance)))
    indices.sort(key=(lambda x: performance[x]))
    ranking = [0] * len(indices)
    for i, x in enumerate(indices):
        ranking[x] = i
    else:
        metrics = metrics.set_index("AREACD")
        categories = metrics.columns
        categories = [*categories, categories[0]]
        cluster_centres = []
        for i in range(len(centres)):
            cluster_i = centres[i].tolist()
            cluster_i = [*cluster_i, cluster_i[0]]
            cluster_centres.append(cluster_i)
        else:
            ranks = indices
            cluster_centres = [cluster_centres[rank] for rank in ranks]
            label_loc = np.linspace(start=0, stop=(2 * np.pi), num=(len(categories)))
            plt.figure(figsize=(14, 14))
            plt.subplot(polar=True)
            for i in range(len(centres)):
                plt.plot(label_loc, (cluster_centres[i]), label=("Cluster" + str(i)))
            else:
                plt.title("Radar plot", size=20, y=1.05)
                lines, labels = plt.thetagrids((np.degrees(label_loc)), labels=categories)
                plt.legend()
                n = len(centres)
                path = loaded_config["local_file_path"]
                plt.savefig("Outputs/radar_plot.jpeg")
    return "radar plot saved"


def export_to_xlsx(
        frames, 
        file_path, 
        file_name, 
        include_maps=True
 ):
    """
    

    Parameters
    ----------
    frames : list
        A list of items to export and corresponding sheet names.
    file_path : str
       The file path to where you want to export the data.
    file_name : str
        Desired file name.
    include_maps : bool, optional
        A boolean operand marking whether the maps should be exported in the 
        excel. The default is True.

    Returns
    -------
    Exported table.

    """
    writer = pd.ExcelWriter(f"{file_path}/{file_name}.xlsx", engine="xlsxwriter")
    for sheet, frame in frames.items():
        frame.to_excel(writer, sheet_name=sheet)
    else:
        if include_maps:
            workbook = writer.book
            worksheet = workbook.add_worksheet("Cluster_map")
            worksheet.insert_image("A1", "Outputs/cluster_map.jpg")
            worksheet = workbook.add_worksheet("Radar_plot")
            worksheet.insert_image("A1", "Outputs/radar_plot.jpeg")
        writer.close()
    return "table exported"



