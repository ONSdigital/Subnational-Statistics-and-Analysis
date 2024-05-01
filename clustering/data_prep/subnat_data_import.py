from typing import Dict, Sequence

from google.cloud import bigquery
import pandas as pd
import os

from utils import get_table_from_path
from data_prep.LAD23_boundaries_external import convert_LAD22_to_LAD23 


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


def reverse_wellbeing_metric(
    df: pd.DataFrame,
    value_col: str = "Value",
) -> pd.DataFrame:
    """Reverses the magnitude of wellbeing metrics.

    Parameters
    ----------
    df
        Contains the loaded data.
    value_col
        Name of the value column.

    Returns
    -------
    DataFrame with values reversed.
    """
    df.loc[:, value_col] = 10 - df[value_col]
    return df


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
    filenames_to_load = os.listdir(loaded_config['local_file_path'])
    filenames_to_load = [filename[:-4] for filename in filenames_to_load if filename.endswith(".csv")]
    
    df = pd.DataFrame()
    df = get_table_from_path(
        table_name=loaded_config["subnational_indicators_table_name"],
        run_locally=loaded_config["run_locally"],
        path=loaded_config["local_file_path"],
        create_geodataframe=False,
        cols_to_select = None,
        project_name=loaded_config["gcp_project_name"],
        project_location=loaded_config["gcp_project_location"],
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
    if loaded_config["run_locally"]:
        df = combine_datasets(loaded_config, cols_to_select)
    else:
        df = get_table_from_path(
            table_name=table_name,
            run_locally=loaded_config["run_locally"],
            path=loaded_config["local_file_path"],
            create_geodataframe=False,
            cols_to_select=cols_to_select,
            project_name=loaded_config["gcp_project_name"],
            dataset_name="ingest_luda",
            project_location=loaded_config["gcp_project_location"],
        )
    df.loc[:, 'Value'] = pd.to_numeric(df['Value'], errors='coerce')

    metric_dfs = get_mission_one_metrics(df)
    metric_dfs = {**metric_dfs, **get_mission_two_metrics(df)}
    metric_dfs = {**metric_dfs, **get_mission_three_metrics(df)}
    metric_dfs = {**metric_dfs, **get_mission_four_metrics(df)}
    metric_dfs = {**metric_dfs, **get_mission_five_metrics(df)}
    metric_dfs = {**metric_dfs, **get_mission_six_metrics(df)}
    metric_dfs = {**metric_dfs, **get_mission_seven_metrics(df)}
    metric_dfs = {**metric_dfs, **get_mission_eight_metrics(df)}
    metric_dfs = {**metric_dfs, **get_mission_nine_metrics(df)}
    metric_dfs = {**metric_dfs, **get_mission_ten_metrics(df)}
    metric_dfs = {**metric_dfs, **get_mission_eleven_metrics(df)}
    metric_dfs = {**metric_dfs, **get_mission_twelve_metrics(df)}
    metric_dfs = {**metric_dfs, **get_custom_metrics(df, loaded_config["custom_metrics_to_run"])}

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


def get_mission_one_metrics(
    df: pd.DataFrame,
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for every mission one metric.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    List containing a DataFrame for each metric.
    """
    gva = extract_single_metric(df, "Gross value added per hour worked")
    weekly_pay = extract_single_metric(df, "Gross median weekly pay")
    employ_rate = extract_single_metric(df, "Employment rate for 16 to 64 year olds")
    gdi = extract_single_metric(df, "Gross disposable household income per head")
    exports = extract_single_metric(df, "Total value of UK exports")
    inward_fdi = extract_single_metric(df, "Inward foreign direct investment (FDI)")
    outward_fdi = extract_single_metric(df, "Outward foreign direct investment (FDI)")

    metrics = [gva, weekly_pay, employ_rate, gdi, exports, inward_fdi, outward_fdi]
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"mission_one": metrics}

    return metrics_dict


def get_mission_two_metrics(
    df: pd.DataFrame,
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for every mission two metric.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    List containing a DataFrame for each metric.
    """
    metrics = []
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"mission_two": metrics}

    return metrics_dict


def get_mission_three_metrics(
    df: pd.DataFrame,
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for every mission three metric.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    List containing a DataFrame for each metric.
    """
    travel_public = extract_single_metric(df, "Public transport or walk to employment centre with 500 to 4999 jobs")
    travel_car = extract_single_metric(df, "Drive to employment centre with 500 to 4999 jobs")
    travel_bike = extract_single_metric(df, "Cycle to employment centre with 500 to 4999 jobs")

    metrics = [travel_public, travel_car, travel_bike]
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"mission_three": metrics}

    return metrics_dict


def get_mission_four_metrics(
    df: pd.DataFrame,
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for every mission four metric.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    List containing a DataFrame for each metric.
    """
    broadband = extract_single_metric(df, "Gigabit capable broadband")
    mobile4g = extract_single_metric(df, "4G coverage")

    metrics = [broadband, mobile4g]
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"mission_four": metrics}

    return metrics_dict


def get_mission_five_metrics(
    df: pd.DataFrame,
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for every mission five metric.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    List containing a DataFrame for each metric.
    """
    ks2 = extract_single_metric(df, "Pupils at expected standards by end of primary school")
    gcses = extract_single_metric(df, "GCSEs (and equivalent) in English and maths by age 19")
    good_schools = extract_single_metric(df, "Schools and nursery schools rated good or outstanding")
    absenses = extract_single_metric(df, "Persistent absences for all pupils")
    absenses_fsm = extract_single_metric(df, "Persistent absences for pupils eligible for free school meals")
    absenses_cla = extract_single_metric(df, "Persistent absences for pupils looked after by local authorities")
    comm_5 = extract_single_metric(df, 
        "Children at expected standard for communication and language by end of early years foundation stage")
    lit_5 = extract_single_metric(df, "Children at expected standard for literacy by end of early years foundation stage")
    maths_5 = extract_single_metric(df, "Children at expected standard for maths by end of early years foundation stage")

    metrics = [ks2, gcses, good_schools, absenses, absenses_fsm, absenses_cla, comm_5, lit_5, maths_5]
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"mission_five": metrics}

    return metrics_dict


def get_mission_six_metrics(
    df: pd.DataFrame,
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for every mission six metric.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    List containing a DataFrame for each metric.
    """
    fe_achievements = extract_single_metric(df, "Aged 19 years and over further education and skills learner achievements")
    app_start = extract_single_metric(df, "Apprenticeships starts")
    app_completion = extract_single_metric(df, "Apprenticeships achievements")
    nvq = extract_single_metric(df, "Aged 16 to 64 years level 3 or above qualifications")
    fe_participation = extract_single_metric(df, "Aged 19 years and over further education and skills participation")

    metrics = [fe_achievements, app_start, app_completion, nvq, fe_participation]
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"mission_six": metrics}

    return metrics_dict


def get_mission_seven_metrics(
    df: pd.DataFrame,
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for every mission seven metric.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    List containing a DataFrame for each metric.
    """
    female_hle = extract_single_metric(df, "Female healthy life expectancy")
    male_hle = extract_single_metric(df, "Male healthy life expectancy")
    smoking = extract_single_metric(df, "Cigarette smokers")
    child_obesity = extract_single_metric(df, "Overweight children at reception age (aged four to five years)")
    year6_obesity = extract_single_metric(df, "Overweight children at Year 6 age (aged 10 to 11 years)")
    adult_obesity = extract_single_metric(df, "Overweight adults (aged 18 years and over)")
    cancer = extract_single_metric(df, "Cancer diagnosis at stage 1 and 2")
    cardio = extract_single_metric(df, "Cardiovascular mortality considered preventable in persons aged under 75")

    metrics = [female_hle, male_hle, smoking, child_obesity, year6_obesity, adult_obesity, cancer, cardio]
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"mission_seven": metrics}

    return metrics_dict


def get_mission_eight_metrics(
    df: pd.DataFrame,
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for every mission eight metric.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    List containing a DataFrame for each metric.
    """
    satisfaction = extract_single_metric(df, "Life satisfaction")
    worthwhile = extract_single_metric(df, "Feeling life is worthwhile")
    happiness = extract_single_metric(df, "Happiness")
    anxiety = extract_single_metric(df, "Anxiety")

    metrics = [satisfaction, worthwhile, happiness, anxiety]
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"mission_eight": metrics}

    return metrics_dict


def get_mission_nine_metrics(
    df: pd.DataFrame,
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for every mission nine metric.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    List containing a DataFrame for each metric.
    """
    metrics = []
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"mission_nine": metrics}

    return metrics_dict


def get_mission_ten_metrics(
    df: pd.DataFrame,
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for every mission ten metric.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    List containing a DataFrame for each metric.
    """
    new_houses = extract_single_metric(df, "Additions to the housing stock")
    
    metrics = [new_houses]
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"mission_ten": metrics}

    return metrics_dict


def get_mission_eleven_metrics(
    df: pd.DataFrame,
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for every mission eleven metric.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    List containing a DataFrame for each metric.
    """
    homicide = extract_single_metric(df, "Homicide Offences")

    metrics = [homicide]
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"mission_eleven": metrics}

    return metrics_dict


def get_mission_twelve_metrics(
    df: pd.DataFrame,
) -> Sequence[pd.DataFrame]:
    """Creates individual DataFrames for every mission twelve metric.

    Parameters
    ----------
    df
        Contains the loaded data.

    Returns
    -------
    List containing a DataFrame for each metric.
    """
    devo_deal = extract_single_metric(df, "Population under devolution deal in England")

    metrics = [devo_deal]
    metrics = [metric for metric in metrics if metric is not None]
    metrics_dict = {"mission_twelve": metrics}

    return metrics_dict

