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

def reverse_direction(metric):
    reverse_metric = 0 - metric["Value"]
    return(reverse_metric)

def reverse_anxiety(metric):
    metric["Value"] = 10 - metric["Value"]
    return(metric)


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
subnat_explorer_metrics['Value'] = pd.to_numeric(subnat_explorer_metrics['Value'], errors='coerce')

#PROCESS DATA TO GIVE A BUNCH OF SEPARATE DATAFRAMES TO PICK AND CHOOSE FROM.
#Mission 1
gva = cut_single_metric("Gross Value Added per hour worked")
weekly_pay = cut_single_metric("Gross median weekly pay")
employ_rate = cut_single_metric("Employment rate for 16 to 64 year olds")
gdi = cut_single_metric("Gross disposable household income per head")
exports = cut_single_metric("Total value of UK exports")
inward_fdi = cut_single_metric("Inward foreign direct investment (FDI)")
outward_fdi = cut_single_metric("Outward foreign direct investment (FDI)")

#Mission 2

#Mission 3
travel_car = cut_single_metric("Drive to employment centre with 500 to 4999 jobs")
travel_bike = cut_single_metric("Cycle to employment centre with 500 to 4999 jobs")
travel_public = cut_single_metric("Public transport or walk to employment centre with 500 to 4999 jobs")

#Mission 4
broadband = cut_single_metric("Gigabit capable broadband")
mobile4g = cut_single_metric("4G coverage")

#Mission 5
ks2 = cut_single_metric("Pupils at expected standards by end of primary school")
gcses = cut_single_metric("GCSEs (and equivalent) in English and maths by age 19")
good_schools = cut_single_metric("Schools and nursery schools rated good or outstanding")
absenses = cut_single_metric("Persistent absences for all pupils")
absenses_fsm = cut_single_metric("Persistent absences for pupils eligible for free school meals")
absenses_cla = cut_single_metric("Persistent absences for pupils looked after by local authorities")
maths_5 = cut_single_metric("Children at expected standard for maths by end of early years foundation stage")
lit_5 = cut_single_metric("Children at expected standard for literacy by end of early years foundation stage")
comm_5 = cut_single_metric("Children at expected standard for communication and language by end of early years foundation stage")

#Mission 6
fe_achievements = cut_single_metric("Aged 19 years and over further education and skills learner achievements")
app_start = cut_single_metric("Apprenticeships starts")
app_completion = cut_single_metric("Apprenticeships achievements")
nvq = cut_single_metric("Aged 16 to 64 years level 3 or above qualifications")
fe_participation = cut_single_metric("Aged 19 years and over further education and skills participation")

#Mission 7
female_hle = cut_single_metric("Female healthy life expectancy")
male_hle = cut_single_metric("Male healthy life expectancy")
smoking = cut_single_metric("Cigarette smokers")
adult_obesity = cut_single_metric("Overweight adults (aged 18 years and over)")
child_obesity = cut_single_metric("Overweight children at reception age (aged four to five years)")
year6_obesity = cut_single_metric("Overweight children at Year 6 age (aged 10 to 11 years)")
cancer = cut_single_metric("Cancer diagnosis at stage 1 and 2")
cardio = cut_single_metric("Cardiovascular mortality considered preventable in persons aged under 75")

#Mission 8
satisfaction = cut_single_metric("Life satisfaction")
anxiety = cut_single_metric("Anxiety")
happiness = cut_single_metric("Happiness")
worthwhile = cut_single_metric("Feeling life is worthwhile")

#Mission 9

#Mission 10
new_houses = cut_single_metric("New houses")

#Mission 11
homicide = cut_single_metric("Homicide")

#Here is a handy list of metrics which this script has processed.
metrics = ["gva", "weekly_pay", "employ_rate", "gdi", "exports", "inward_fdi", "outward_fdi", "travel_car", "travel_bike", "travel_public", "broadband", "mobile4g", "ks2", "maths_5", "lit_5", "comm_5", "gcses", "good_schools", "absenses", "absenses_fsm", "absenses_cla", "fe_achievements", "app_start", "app_completion", "nvq", "fe_participation", "female_hle", "male_hle", "smoking", "adult_obesity", "child_obesity", "year6_obesity", "cancer", "cardio", "satisfaction", "anxiety", "happiness", "worthwhile", "new_houses", "homicide"]