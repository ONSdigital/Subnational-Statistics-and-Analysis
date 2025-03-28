from typing import Dict
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def cluster_map(
        loaded_config: Dict,
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
    path=loaded_config["outputs_file_path"]
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
    plt.savefig(f"{path}/Cluster_map.jpeg")
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
                path=loaded_config["outputs_file_path"]
                plt.savefig(f"{path}/radar_plot.jpeg")
    return "radar plot saved"
