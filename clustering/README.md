# Cloning the repository
To be able to run this code, you will need to clone this git repository. To do this, run the following command:
`git clone https://github.com/ONSdigital/Subnational-Statistics-and-Analysis.git`\
This code was developed in a Vertex AI workbench where it is executable with Python notebooks, however if you wish to run the
code another way it is likely that you will need to clone this repository into the cloud shell and deploy it to your chosen
service from there.

# Using this code
The code within the clustering folder should be executed in a Google Cloud environment as it uses functions to get data from
the BigQuery service. Throughout the code, there are instances of queries used to get the data that must be updated to match the
path to your data in BigQuery. For example `project.ingest_dataset_name.table_name` would need to be updated in the code to the name of
your project, followed by the name of the dataset and table that contains the data you will be running the clustering method on.

# Running the headline clustering model
To run the clustering model, you must have completed the above steps to copy the code to your notebook on LUDA. The code for running the model is held in the `mission_headline_support.ipynb`
notebook. The execution of the headline model produced by this file works as follows:
| Step          | Description |
|-------------  | ----------- |
|1              | Load subnational indicators explorer data from BigQuery. |
|2              | Assign loaded metric variables to lists for their respective missions e.g. `mission1 = [gva, weekly_pay, employ_rate, gdi]`. Each loaded metric is a dataframe. |
|3              | Clean each group of metrics: <ul><li>Convert upper tier local authorities (combination of counties and unitary authorities) to lower tier (singular unitary authorities or local authority districts).</li><li>Drop `index` and `YEAR` columns from data.</li><li>Rename `area` column to `AREACD` and put at the start of columns.</li><li>Convert all values to numeric.</li></ul> |
|4              | Create the chosen clustering model. The code currently supports the following models: <ul><li>k-means</li></ul> |
|5              | Combine group of metrics into one table. |
|6              | Calculate clusters using specified clustering model. |
|7              | Create radar plot, scatter plot and map of clusters using the `clusters_and_plots` or `clusters_and_plots_cb` (colourblind) functions. |
</ol>