import pandas as pd
from typing import Dict

def add_percentages(
        data: pd.DataFrame,
        loaded_config: Dict, 
) -> pd.DataFrame:
    """
    This function takes a numerator and denominator column and calculates a percentage, which is added to the data

    Parameters
    ----------
    data : pd.DataFrame
        aggregated dataset to desired gepgraphy.
    loaded_config: Dict
        config including field names and file paths

    Returns
    -------
    data : dataframe
        Data including a percentage column.

    """
    #extract information from the config
    numerator_col = loaded_config["numerator_column"]
    denominator_col = loaded_config["denominator_column"]
    #calculate the percentage from the columns
    data["percent"] = (data[numerator_col]/data[denominator_col])*100
    #round to appropriate level
    data["percent"] =  data["percent"].round(1)
    return data

def add_rates(
        data: pd.DataFrame,
        loaded_config: Dict, 
        out_of:float
) -> pd.DataFrame:
    """
    This function takes a numerator and denominator column and calculates a rate, which is added to the data

    Parameters
    ----------
    data : pd.DataFrame
        aggregated data to desired geography.
    loaded_config: Dict
        config including field names and file paths
    out_of : float, optional
        variable where you specify the per divider.
    Returns
    -------
    data : dataframe
        input data with additional rate column.

    """
    #extract information from the config
    value_col = loaded_config["numerator_column"]
    population_col = loaded_config["denominator_column"]
    #calculate the rate from the columns
    data["rate"] = data[value_col]/(data[population_col]/out_of)
    #round to appropriate level
    data["rate"] = data["rate"].round(1)
    return data