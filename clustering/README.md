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
A folder where you paste your data/the existing dataset is stored. The data must be in the same format as the [ESS machine readable]( https://www.ons.gov.uk/peoplepopulationandcommunity/wellbeing/articles/subnationalindicatorsexplorer/2022-01-06) which can be found by clicking “Download the data for the subnational indicators explorer” under the plots on the web page. This format should include all columns aside from the MAD column which isn’t necessary. If there is a missing local authority district in your data and you want the value to be imputed there must be a row in the data for that area with a blank value column. 

## Lookups
The code requires several lookup files, which should be saved in the lookups folder. You can specify the names and desired columns of the lookups within the config file.
The files required include:
-	Shapefiles for UK LADs used in our mapping function. These are available through the [ONS open geography portal](https://geoportal.statistics.gov.uk/datasets/79a4e87783be4b6bbb96ddad6dda52a3_0/explore)
-	A csv file including all current lower-tier local authority codes in one column and their corresponding upper tier local authority codes in another. This is used for imputing lower-tier data with upper-tier. [Available through the ONS open geoportal]( https://geoportal.statistics.gov.uk)
-	Census lower-tier local authority population by single year age table in csv form, this is used in the function to update local authority boundaries that have changed. The columns in this should read from left to right: area name, area code, total population and then single year population in ascending years. This can be found through the [NOMIS Census page]( https://www.nomisweb.co.uk/sources/census_2021)
- 	A csv table including the area of all lower-tier local authorities in hectares and the area codes. This table can be found through [this link](https://geoportal.statistics.gov.uk/datasets/235c70d40c494361bd6b0ddaebdf0bad/about)
-	A csv table including separate columns with lower-tier local authority names, codes and corresponding ITL1 regions. This dataset is used in the functions to create summary tables of the clusters. [Available through the ONS open geoportal]( https://geoportal.statistics.gov.uk)

If you are struggling to source these lookups, we can provide them if you get in touch using the below email, however this is a busy shared inbox so there may be a slight delay in our reply.

For more detail on the approach we have taken in this analysis, please see our [methodology article](https://www.ons.gov.uk/peoplepopulationandcommunity/wellbeing/methodologies/clusteringsimilarlocalauthoritiesintheukmethodology)

For any queries regarding this code please contact subnational@ons.gov.uk

