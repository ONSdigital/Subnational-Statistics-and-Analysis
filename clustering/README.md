# Clustering code

See the below for a guide to the different files within the clustering refactor repository. If you run the code as specified with suitable data all outputs and model information will automatically be generated.

## Notebooks/Run_clustering_script.ipynb
This notebook calls in the functions and information from the other script and runs the clustering model. As it is currently set up it will run a clustering model on the data before outputting the radar plot, maps and supplementary model information. There are notes in each cell which detail what the code is doing and provides information on where you can customize the parameters of models.

## Config.yaml
This, aside from the run_clustering_script, is the only file you need to alter to run the model. In this file you specify file paths, the names of columns in the lookup files and the metrics you want to include in the model. There are notes within the script to guide you through this and the file must be saved before the model is run.

## Cluster_code/Cluster_functions.py
This script contains the functions involved in running the clustering model, mapping and exporting the outputs. These functions are called in the “run_clustering_script” notebook. All functions have docstrings detailing the inputs and outputs. Some functions have multiple outputs which must be allocated, these are detailed in the notes within the “run_clustering_script” notebook.

## Data_prep/LAD23_boundaries_external.py
This script contains the functions used to take data produced with old Local Authority boundaries and impute values for the newer Local Authorities. All functions have docstrings detailing the inputs and outputs.

## Data_prep/subnat_data_clean.py
This script contains all of the functions to clean the data, these functions are called and applied to the data in the “run_clustering_script” notebook. All functions have docstrings detailing the inputs and outputs.

## Data_prep/subnat_data_import.py
This script contains all of the functions to import the data, these functions are called and applied to the data in the “run_clustering_script” notebook. All functions have docstrings detailing the inputs and outputs.

## Utils.py
This script contains the “get_table_from_path” function which loads in data in the correct format.

## Outputs
The maps and data tables are output to this folder when the code is run.

## Data
A folder where you paste your data/the existing dataset is stored. The data must be in the same format as the example dataset in the folder. If there is a missing local authority district in your data and you want the value to be imputed there must be a row in the data for that area with a blank value column

## Lookups
The files in this folder include:
-	Shapefiles for our mapping function.
-	A lower-tier to upper-tier local authority lookup, used in our data cleaning function to impute results where lower-tier statistics are missing but upper-tier are available.
-	Census population by age and geographical area datasets, used in our function to apportion old local authority results to the new local authority boundaries.
-	A table of lower-tier local authority names, codes and ITL1 region. This dataset is used in the functions to create summary tables of the clusters.

For more detail on the approach we have taken in this analysis, please see our [methodology article](https://www.ons.gov.uk/peoplepopulationandcommunity/wellbeing/methodologies/clusteringsimilarlocalauthoritiesintheukmethodology)

For any queries regarding this code please contact subnational@ons.gov.uk
