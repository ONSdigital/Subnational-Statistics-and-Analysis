from typing import Dict, Sequence
import pandas as pd
import os
from src.utils.utils import get_table_from_path
from src.data.transform_data import convert_LAD22_to_LAD23


def get_custom_metrics(
    df: pd.DataFrame,
    custom_metrics: Sequence[str],
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for custom group of metrics specified in config.
    
    Parameters
    ----------
    df
        Contains the loaded data.
        
    Returns
    -------
    List containing a DataFrame for each metric.
    """
    metrics = []
    for metric in custom_metrics:
        metrics.append(extract_single_metric(df, metric))
    
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"custom_metrics": metrics}
    
    return metrics_dict

def extract_single_metric(
    df: pd.DataFrame,
    metric_name: str,
    indicator_column: str = "Indicator",
) -> pd.DataFrame:
    """Gets a single metric from long-format ESS-style data.

    Parameters
    ----------
    df
        Contains the long-format data.
    metric_name
        Name of the indicator that should be filtered for.
    indicator_column
        Column name containing the indicator.

    Returns
    -------
    DataFrame with the filtered metric.
    """
    if metric_name in df[indicator_column].unique():
        extracted_metric = df.loc[df[indicator_column] == metric_name]
        return extracted_metric
    else:
        print(f"{metric_name} not found in {indicator_column} column")


def get_most_recent(
    df: pd.DataFrame,
    year_col: str = "YEAR",
) -> pd.DataFrame:
    """Gets the data from the most recent year.

    df
        Contains the loaded data.
    year_col
        Name of the column that should be filtered by.

    Returns
    -------
    DataFrame containing only the most recent year of data.
    """
    most_recent_year = df.loc[df[year_col] == df[year_col].max()]
    return most_recent_year


def reverse_direction(
    df: pd.DataFrame,
    value_col: str = "Value",
) -> pd.Series:
    """Reverses the magnitude of the specified value column.

    Parameters
    ----------
    df
        Contains the loaded data.
    value_col
        Name of the value column to be reversed.

    Returns
    -------
    Series containing reversed value column.
    """
    reverse_metric = 0 - df[value_col]
    return reverse_metric


def combine_datasets(
    loaded_config: Dict,
    cols_to_select: Sequence[str],
) -> pd.DataFrame:
    """Loads and concatentates all data files in a folder.
    
    Parameters
    ----------
    loaded_config
        Contains the loaded config.
    cols_to_select
        Columns that should be selected from the table.

    Returns
    -------
    DataFrame containing all data in location.
    """
    filenames_to_load = os.listdir(loaded_config['inputs_file_path'])
    filenames_to_load = [filename[:-4] for filename in filenames_to_load if filename.endswith(".csv")]
    
    df = pd.DataFrame()
    df = get_table_from_path(
        table_name=loaded_config["subnational_indicators_table_name"],
        path=loaded_config["inputs_file_path"],
        create_geodataframe=False,
        cols_to_select = None,
)

    return df


def import_data(
    loaded_config: Dict,
    cols_to_select: Sequence[str],
    table_name: str,
) -> Sequence[pd.DataFrame]:
    """Gets data from table and splits each metric into separate DataFrames.

    Parameters
    ----------
    loaded_config
        Contains the loaded config.
    cols_to_select
        Columns that should be selected from the table.
    table_name
        Name of the table containing the data.

    Returns
    -------
    List containing each metric as a DataFrame.
    """

    df = combine_datasets(loaded_config, cols_to_select)

    df.loc[:, 'Value'] = pd.to_numeric(df['Value'], errors='coerce')

    metric_dfs = get_custom_metrics(df, loaded_config["custom_metrics_to_run"])

    #If metric boundaries need updating get necessary parameters from the configs
    for key in metric_dfs:
        for data in range(len(metric_dfs[key])):
            conversion_params = {}
            Indicator = metric_dfs[key][data]["Indicator"].unique()[0]
            result = any(Indicator in d.values() for d in loaded_config["metric_to_update_boundaries"].values())
            if result:
                for subkey in loaded_config["metric_to_update_boundaries"].keys():
                    current_indicator = loaded_config["metric_to_update_boundaries"][subkey]["indicator"]
                    if current_indicator == Indicator:
                        conversion_params = loaded_config["metric_to_update_boundaries"][subkey]
                        if conversion_params.get("end_col"):
                            metric_dfs[key][data] = convert_LAD22_to_LAD23(
                                loaded_config, 
                                metric=metric_dfs[key][data], 
                                start_col=conversion_params["start_col"],
                                end_col = conversion_params["end_col"],
                                denominator = conversion_params["denominator"]
                            )
                        else:
                            metric_dfs[key][data] = convert_LAD22_to_LAD23(
                                loaded_config, 
                                metric=metric_dfs[key][data], 
                                start_col=conversion_params["start_col"],
                                denominator = conversion_params["denominator"]
                            )
    return metric_dfs

def export_to_xlsx(
        loaded_config: Dict,
        frames, 
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
    path=loaded_config["outputs_file_path"]
    writer = pd.ExcelWriter(f"{path}/{file_name}.xlsx", engine="xlsxwriter")
    for sheet, frame in frames.items():
        frame.to_excel(writer, sheet_name=sheet)
    else:
        if include_maps:
            path=loaded_config["outputs_file_path"]
            workbook = writer.book
            worksheet = workbook.add_worksheet("Cluster_map")
            worksheet.insert_image("A1", f"{path}/Cluster_map.jpeg")
            worksheet = workbook.add_worksheet("Radar_plot")
            worksheet.insert_image("A1", f"{path}/radar_plot.jpeg")
        writer.close()
    return "table exported"