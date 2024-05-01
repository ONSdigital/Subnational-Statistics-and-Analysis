from typing import Dict
import yaml
import os

from data_prep.subnat_data_clean import *
from data_prep.subnat_data_import import *
from cluster_functions import *
from yaml.loader import SafeLoader


def pipeline(
    loaded_config: Dict,
) -> None:
    # Load data and split into individual metrics
    datasets = import_data(
        loaded_config=loaded_config,
        cols_to_select=["AREACD", "Indicator", "Value"],
        table_name=loaded_config["subnational_indicators_table_name"],
    )

    # Clean metrics grouped by mission
    for key, value in datasets.items():
        value = clean_groups(loaded_config, value)

    # Convert metrics into pivoted tables
    tables = {}
    for key, value in datasets.items():
        tables[key] = metrics_to_table(value)

    clusters_and_plots(
        loaded_config=loaded_config,
        metrics=tables["mission_six"],
        model=make_clustering_model(),
    )

    return
