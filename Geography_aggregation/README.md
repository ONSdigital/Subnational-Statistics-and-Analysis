# Geography Aggregation

This is the public version of the code we use to aggregate data for Explore Local Statistics - when the data source does not already provide the geographies we need.

See the below for a guide to the different files within the geography aggregation code.

If you run the code as specified with suitable data, all outputs will be automatically output alongside a list of missing geographies for QA purposes.

## Contact
This repository was developed and is maintained by the ONS Subnational Methods for Dissemination team.

> To contact us raise an issue on Github or via email at [subnational@ons.gov.uk](mailto:subnational@ons.gov.uk).

## Setup

* This project was developed using Python 3.10.5
* Required Python libraries are listed in `requirements.txt`

## Getting started

### run_aggregation.ipynb
This notebook calls in the functions and information from the other files and runs the aggregation process based on the config file, and lookups saved in the inputs folder. 
As it is currently set up it will import the raw data and lookups specified in the config file, load functions from the function_scripts, update any out of date geographies and aggregate to all geographies listed in the config.
This file must be run in an environment that supports Jupyter notebooks.

There are notes in each cell which detail what the code is doing and provides information on where you can customise the code.

### run_aggregation_with_time_series.ipynb
This alternative notebook does everything that run_aggregation.ipynb does, except it uses the 'keep' version of key functions.  These allow a 'year' field to be passed through the process to support timeseries data.  Note- this version also could be used to pass through and present the data with other additional variables.

### config.yaml
This, aside from either the aggregation_script.py OR the aggregation_script_with_time_series.py script, is the only file you need to alter to run the geography aggregation. In this file you specify file paths, the names of columns in the lookup files and what you want to name the output file. There are notes within the script to guide you through this and the file must be saved before the model is run.

### function_scripts

1. `[aggregate_data.py]` - This script contains all of the functions to aggregate count data from a smaller 'base geography' e.g. Local Authority, to a larger 'target geography' e.g. Combined Authority. All functions have docstrings detailing the inputs and outputs.
2. `[export_and_qa.py]` - This script contains a qa function to check for missing areas in the raw data and a function to output the aggregations to excel. All functions have docstrings detailing the inputs and outputs.
3. `[metric_calculation.py]` - This script contains functions for calculating percentages and rates from your newly aggregated count data.
4. `[reshape_data.py]` - This script contains functions for manipulating and reshaping your data.
5. `[update_boundaries.py]` - This script contains functions for updating creating new "base geographies" from obsolete areas where areas have been merged (common in the unitarisation of LAs)

### Data
This aggregation code only works with count data. This means that, if the figure you wish to aggregate is a percentage or a rate, you will have to identify the count components that go into the variable, aggregate those counts, and recalculate the rate or percentage.
Your data should be in csv format and include the following columns:
- Area code
- Area name
- Value column(s)
- Keep column (if applicable)

No other columns are required for the code to run, it may be that the counts required to calculate your percentage or rate are not provided in the source data, in this case you can contact the data owner to see if you can obtain the counts and join them to the raw data.

### Lookups
The code requires 2 lookup files, which should be saved in the inputs folder. You can specify the names and desired columns of the lookups within the config file.

The lookups define 1. any changes in your base geography - to match your base data to that needed for the aggregation, and 2. the relationship between your (smaller) base geography and your desired (larger) output geography

The source for definitive information on the relationships between different UK geographies is the geoportal [ONS open geography portal](https://geoportal.statistics.gov.uk/).

Lookups represent the relationships between the boundaries of one geography and another showing how each area in a smaller base geography relates to a larger area.  For example the below excerpt from Local Authority District to Region (December 2024) Lookup shows the relationship between 2024 Local Authority Districts to Regions.  It shows that Blackpool is in the North West.

Your lookup should contain the geography you are aggregating to in the left most columnm , and a column for each of the larger geography codes it maps into to the right.

Whilst the names are helpful and meaningful in real life - the codes are more helpful for most operations as they not prone to differences in spelling.


|LAD24CD  |LAD24NM                      |RGN24CD  |RGN24NM                 |
|---------|-----------------------------|---------|------------------------|
|E06000001|Hartlepool                   |E12000001|North East              |
|E06000002|Middlesbrough                |E12000001|North East              |
|E06000003|Redcar and Cleveland         |E12000001|North East              |
|E06000004|Stockton-on-Tees             |E12000001|North East              |
|E06000005|Darlington                   |E12000001|North East              |
|E06000006|Halton                       |E12000002|North West              |
|E06000007|Warrington                   |E12000002|North West              |
|E06000008|Blackburn with Darwen        |E12000002|North West              |
|E06000009|Blackpool                    |E12000002|North West              |
|E06000010|"Kingston upon Hull, City of"|E12000003|Yorkshire and The Humber|

One of the key characteristics of UK geographies is change. Local Authorities often change year on year and in recent years this has been by merging 2 or more smaller areas into fewer larger authorities. The above lookup is based on 2024 Local Authorities, but older datasets may be to older Local Authority definitions.

Our code enables an older area boundary to be converted to a more recent definition.   This will work on any base geography which has changed- but only where the changes are merges.   Some other changes e.g. splits cannot be aggregated by conventional means.

The boundary changes lookup must include the old area code and name, and the new area code and name that these old areas map into, an example for lower-tier local authorities is show below:

|AREACD19  |AREANM19                      |AREACD23  |AREANM23                           |
|----------|------------------------------|----------|-----------------------------------|
|E06000028 |Bournemouth                   |E06000058 |Bournemouth, Christchurch and Poole|
|E06000029 |Poole                         |E06000058 |Bournemouth, Christchurch and Poole|
|E07000048 |Christchurch                  |E06000058 |Bournemouth, Christchurch and Poole|

This lookup will often have to be constructed manually as it is not produced by the ONS geoportal.

## Use cases
-   Update input datasets from different sources onto the latest local authority areas for input into a clustering model which requires consistent inputs.
-   Convert data which is on a lengthy timeseries across different years local authority definitions onto the latest local authority definition for timeseries analysis.
-   Aggregate all the data for a project onto a new set of analytical areas that you have created by combining areas from an existing geography.

## Limitations
-   These aggregation methods deal only with geographies that map directly into each other. Any changes relating to splits or more complex relationships cannot be aggregated.
-   Raw data needs to be available in count format for both the denominator and the numerator. In some cases it may be possible to unpick a rate or a percentage if one of these is available.
-   Rounding in the raw data will result in less accurate aggregations.
-   The code does not aggregate values with missing underlying data, if there is a lot of missingness in your source data this will transfer to your output.
-   For some datasets additional coding or additional input data may be required.


## Project structure
```text
| config.yaml                               <- contains all the configuration/settings needed to run (input file names/locations/lookups/field names).
| requirements.txt                          <- python libraries required.
| run_aggregation.ipynb                     <- Jupyter Notebook File to run a single year aggregation.
| run_aggregation_with_time_series.ipynb    <- Jupyter Notebook File to run a timeseries aggregation.
|
+---function_scripts                            
|   |   aggregate_data.py            	    <- contains functions for aggregating data to larger geographies
|   |   export_and_qa.py 	 	            <- contains the functions for QA and exporting.
|   |   metric_calculation.py     	        <- contains functions for rate and percent calculations.
|   |   reshape_data.py    	                <- contains functions to reshape the data. 
|   |   update_boundaries.py       	        <- contains functions for generating data for new geographies.
|  
+---outputs                             <- The code outputs are saved here. Created by the code if it doesn't already exist.
