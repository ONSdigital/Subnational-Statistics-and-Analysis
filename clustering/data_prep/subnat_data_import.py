#This script will read in all the data we are currently interested in from the subnational explorer.
import pandas as pd

#Helpful functions for moving metrics about.
def cut_single_metric(name):
    return subnat_explorer_metrics[subnat_explorer_metrics['Indicator']==name]

def query_to_dataframe(query):
    client = bigquery.Client(location="location")
    query_job = client.query(query, location="location",)  
    return(query_job.to_dataframe())

def get_most_recent(data, year_syntax='YEAR'):
    most_recent_year = data[data[year_syntax] == data[year_syntax].max()]
    return(most_recent_year)

#Get data -- first from published subnat explorer.
from google.cloud import bigquery
import pandas as pd
client = bigquery.Client(location="location")

query = """
    SELECT AREACD, Indicator, Value 
    FROM `project.ingest_dataset_name.ingest_table_name` 
    
"""
query_job = client.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="location",
)  # API request - starts the query

subnat_explorer_metrics = query_job.to_dataframe()

#PROCESS DATA TO GIVE A BUNCH OF SEPARATE DATAFRAMES TO PICK AND CHOOSE FROM.
#Process data to do with Productivity
gva = cut_single_metric("Gross Value Added per hour worked")
query = """SELECT GEOGRAPHY, YEAR, HIGH_GROWTH_BUSINESSES as Value FROM `project.ingest_dataset_name.growth_table_name`"""
high_growth = query_to_dataframe(query)
high_growth = get_most_recent(high_growth)
high_growth['Indicator']='High growth businesses'

#Pay
weekly_pay = cut_single_metric("Gross median weekly pay")

query="""SELECT LAD_CODE, YEAR, GDI_PER_HEAD as Value FROM `project.ingest_dataset_name.gdi_table_name`"""
gdi = get_most_recent(query_to_dataframe(query))
gdi['Indicator'] = 'GDI per head'

query="""SELECT ONS_GEOGRAPHY_CODE_9_DIGIT, YEAR, PROPORTION_NEET_OR_NOT_KNOWN as Value FROM `project.ingest_dataset_name.neet_table_name`"""
neet = get_most_recent(query_to_dataframe(query))
#For some reason, looks like this table has duplicated rows.
neet = neet.drop_duplicates()
neet['Indicator'] = 'Proportion NEET'

#Living standards

male_hle = cut_single_metric("Male healthy life expectancy")
female_hle = cut_single_metric("Female healthy life expectancy")
satisfaction = cut_single_metric("Average life satisfaction rating")

#Think we've questioned this metric for rural areas...
query="""SELECT LAD_CODE, AVERAGE_DISTANCE_TO_NEAREST_PARK_OR_PUBLIC_GARDEN_OR_PLAYING_FIELD_M as Value FROM `project.ingest_dataset_name.green_space_table_name`"""
green_space = query_to_dataframe(query)
green_space['Indicator'] = 'Avergae distance to park or public garden or playing field'

query="""SELECT CODE, YEAR, ACTIVITY, RATE_PERCENTAGE as Value FROM `project.ingest_dataset_name.physical_activity_table_name`"""
activity = get_most_recent(query_to_dataframe(query))
#Take inactive metric as proportion inactive
inactive = activity[activity["ACTIVITY"]=="Inactive"]
inactive.pop("ACTIVITY")
inactive['Indicator'] = 'Proportion inactive'

#Attainment/skills
gcses = cut_single_metric("Young people achieving GCSEs (and equivalent qualifications) in English and Maths by age 19")

maths_5 = cut_single_metric("5 year olds achieving 'expected level' on maths early learning goals")
lit_5 = cut_single_metric("5 year olds achieving 'expected level' on literacy early learning goals")
comm_5 = cut_single_metric("5 year olds achieving 'expected level' on communication early learning goals")

employ_rate = cut_single_metric("Employment rate for 16 to 64 year olds")

app_start = cut_single_metric("Number of completions on apprenticeships") #Note: need a rate?
app_completion = cut_single_metric("Number of starts on apprenticeships")

#Get some more for the indicators explorer
broadband = cut_single_metric("Premises with gigabit capable broadband")
mobile4g = cut_single_metric("4G coverage provided by at least one mobile network provider")
good_schools = cut_single_metric("Schools and nurseries rated good or outstanding by OFSTED")
smoking = cut_single_metric("Adults that currently smoke cigarettes")
adult_obesity = cut_single_metric("Adult (18+) overweight and obesity prevalance")
child_obesity = cut_single_metric("Children (4 to 5 years old) overweight and obesity prevalance")
anxiety = cut_single_metric("Average anxiety rating")
happiness = cut_single_metric("Average happiness rating")
worthwhile = cut_single_metric("Average feeling that things done in life are worthwhile rating")
travel_car = cut_single_metric("Average travel time to nearest large employment centre (500+ employees) by car")
travel_bike = cut_single_metric("Average travel time to nearest large employment centre (500+ employees) by bike")
travel_public = cut_single_metric("Average travel time to nearest large employment centre (500+ employees) by public transport or walking")

#Here is a handy list of metrics which this script has processed.
metrics = ["gva", "high_growth", "weekly_pay", "gdi", "neet", "employ_rate", 
           "male_hle", "female_hle", "gcses", "maths_5", "lit_5", "comm_5", "app_start", "app_completion",
          "satisfaction", "green_space", "inactive",
          "broadbank", "mobile4g", "good_schools", "smoking" "adult_obesity", "child_obesity", "anxiety",
          "happiness", "worthwhile", "travel_car", "travel_bike", "travel_walk"]