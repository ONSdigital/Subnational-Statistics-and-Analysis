# Clustering and Statistical Nearest Neighbours

This is the public version of the code used to create the ONS clustering and statistical nearest neighbours analysis, which groups UK local authorities with similar characteristics and outcomes. 
See the below for a guide to the different files within the clustering refactor repository. 
If you run the code as specified with suitable data, all outputs and model information will automatically be generated.

## Contact
This repository was developed and is maintained by the ONS Subnational Methods for Dissemination team.

> To contact us raise an issue on Github or via email at [subnational@ons.gov.uk](mailto:subnational@ons.gov.uk).
> See our methodology publication here: [Clustering similar local authorities and statistical nearest neighbours in the UK, methodology](https://www.ons.gov.uk/peoplepopulationandcommunity/wellbeing/methodologies/clusteringsimilarlocalauthoritiesandstatisticalnearestneighboursintheukmethodology).
> See our dataset here: [Clustering similar local authorities and statistical nearest neighbours in the UK, 2025](https://www.ons.gov.uk/peoplepopulationandcommunity/wellbeing/adhocs/2632clusteringsimilarlocalauthoritiesandstatisticalnearestneighboursintheuk2025).

## Setup

* This project was developed using Python 3.10.5
* Required Python libraries are listed in `requirements.txt`

## Getting started

### Notebooks/run_all_analysis.ipynb
This notebook calls in the functions and information from the other files and runs the clustering and nearest neighbours analysis. 
As it is currently set up it will run all similarity analysis, creating files for main results and variable selection alongside all relevant visualisations, which are saved in the specified output folder. 
There are notes in each cell which detail what the code is doing and provides information on where you can customise the parameters of models.

### Config.yaml
This, aside from the run_all_analysis script, is the only file you need to alter to run the model. In this file you specify file paths, the names of columns in the lookup files and the metrics you want to include in the model. There are notes within the script to guide you through this and the file must be saved before the model is run.

### clustering_refactor/src/data

1. `[import_export_data.py]` - This script contains the functions to import the data and export the results, these functions are called and applied in the “run_all_analysis” notebook. All functions have docstrings detailing the inputs and outputs.
2. `[clean_data.py]` - This script contains all of the functions to clean the data, these functions are called and applied to the data in the “run_all_analysis” notebook. All functions have docstrings detailing the inputs and outputs.
3. `[transform_data.py]` - This script contains the winsorization function, as well as functions used to take data produced with old Local Authority boundaries and impute values for the newer Local Authorities. All functions have docstrings detailing the inputs and outputs.
4. `[variable_selection.py]` - This script contains functions for variable selection process covering PCA, correlations, and variance analysis.

### clustering_refactor/src/similarity

1. `[kmeans_clustering.py]` - This script contains the functions involved in running the clustering model. These functions are called in the “run_all_analysis” notebook. All functions have docstrings detailing the inputs and outputs. Some functions have multiple outputs which must be allocated, these are detailed in the notes within the “run_all_analysis” notebook.
2. `[nearest_neighbours.py]` - This script contains the functions involved in implementing the euclidean distance calculation for the nearest neighbours as well as to find the optimum number of neighbours.
3. `[summary_tables.py]` - This script contains the functions to create the supplementary tables including cluster average and ITL1 distribution of cluster. These tables are then output in Excel form within the “run_all_analysis” notebook.

### clustering_refactor/src/visualisation
1. `[plot.py]` - this script is to output the charts from the analysis which are the cluster maps and radar plot.

### clustering_refactor/src/utils
1. `[utils.py]` - This script contains the “get_table_from_path” function which loads in data in the correct format.

### Data
To be compatible with the code, your data must include all variables stacked into one csv file. This data must include the following columns:
-	AREACD and AREANM: which denote the code and name of each area.
-	Indicator: which denotes the variable.
-	Period: which includes the reference period of the source data.
-	Value: which includes the data value for an area for a given variable.


### Lookups
The code requires several lookup files, which should be saved in the lookups folder. You can specify the names and desired columns of the lookups within the config file.
The files required include:
-	Shapefiles for UK LADs used in our mapping function. These are available through the [ONS open geography portal](https://geoportal.statistics.gov.uk/datasets/79a4e87783be4b6bbb96ddad6dda52a3_0/explore)
-	Census lower-tier local authority population by single year age table in csv form, this is used in the function to update local authority boundaries that have changed. The columns in this should read from left to right: area name, area code, total population and then single year population in ascending years. This can be found through the [NOMIS Census page]( https://www.nomisweb.co.uk/sources/census_2021)
- 	A csv table including the area of all lower-tier local authorities in hectares and the area codes. This table can be found through [this link](https://geoportal.statistics.gov.uk/datasets/235c70d40c494361bd6b0ddaebdf0bad/about)
-	A csv table including separate columns with lower-tier local authority names, codes and corresponding ITL1 regions. This dataset is used in the functions to create summary tables of the clusters. [Available through the ONS open geoportal]( https://geoportal.statistics.gov.uk)

If you are struggling to source these lookups, we can provide them if you get in touch using the below email, however this is a busy shared inbox so there may be a slight delay in our reply.

## Project structure

```text
| config.yaml             			  	 <- contains all the configuration/settigs needed to run.
| requirements.txt          		 	 <- python libraries required.
|
+---src                    			 	 <- This is a placeholder which the pipeline populates with data
|   +---data
|   |   |	clean_data.py            	 <- cleaning of the imported data
|   |   |	import_export_data.py    	 <- imports the required dataset
|   |   |	transform_data.py        	 <- performs all needed transformation to prepare the data
|   |   |	variable_selection.py 	 	 <- performs the variable slection process to select only the needed columns for analysis
|   +---similarity
|   |   |	kmeans_clustering.py     	 <- implements the KMeans algorithm
|   |   |	nearest_neighbours.py    	 <- performs the statitical nearest neighbours calculations 
|   |   |	summary_tables.py       	 <- output all needed supplementary data as Excel table
|   +---utils
|   |   |	utils.py             		 <- gets table from path
|   +---visualisation
|   |   |	plot.py             		 <- returns all the visualisations from the analysis
+--- notebook                         
|   |   run_all_analysis.py         	 <- runs the full analysis using all the modules above

```
