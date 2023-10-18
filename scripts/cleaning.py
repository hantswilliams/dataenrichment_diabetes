import pandas as pd 
import os


####### CNT_TRANSIT #######
## combine together cnt_transit files into one file
# get all files in directory
files = os.listdir('data/cnt_transit')
# create empty dataframe
df = pd.DataFrame()
# loop through files
for file in files:
    # read in file
    temp = pd.read_csv('data/cnt_transit/' + file)
    # get the name of the file, everything before the first _ (e.g. 'Ohio')
    temp['state'] = file.split('_')[0]
    # append to df
    df = df.append(temp)
# sort by state
df = df.sort_values(by='state')
# keep only place, name, alltransit_performance_score, and state
df = df[['place', 'name', 'alltransit_performance_score', 'state']]
# rename place to fip_place
df = df.rename(columns={'place': 'fip_place'})
# write to csv
df.to_csv('data/cnt_transit.csv', index=False)





###### CDC data #######
# read in data
df = pd.read_csv('data/cdc/cdc_2019.csv')
# keep only data where category is health outcomes
df = df[df['Category'] == 'Health Outcomes']
# keep only diabetes data
df = df[df['MeasureId'] == 'DIABETES']
# keep only Data_Value_Type = 'Crude Prevalence'
df = df[df['Data_Value_Type'] == 'Crude prevalence']
# keep only where GeographicLevel is equal to Census Tract
df = df[df['GeographicLevel'] == 'Census Tract']
# drop rows where CityFIPS is NaN (this is the national prevalence that we are dropping)
df = df.dropna(subset=['CityFIPS'])
# keep only StateAbbr, StateDesc, CityName, Measure, Data_Value, CityFIPS, TractFIPS
df = df[['StateAbbr', 'StateDesc', 'CityName', 'Data_Value', 'CityFIPS', 'TractFIPS', 'Short_Question_Text']]
# save to csv
df.to_csv('data/cdc/diabetes.csv', index=False)





###### Census data #######
# read in data which is a .txt file that is comma separated, and the first 1 rows are header
df = pd.read_csv('data/census/cbp19co.txt', sep=',')
## keep where naics string contains 722513
fast_food = df[df['naics'].str.contains('722513')]
## keep where naics string contains 445110 (grocery stores)
grocery = df[df['naics'].str.contains('445110')]
## combine fast food and grocery
food = fast_food.append(grocery)
## keep only fipstate, fipscty, naics, EST, 
food = food[['fipstate', 'fipscty', 'naics', 'est']]
## create new column naics_type, where 722513 is fast food, 445110 is grocery
food['naics_type'] = food['naics'].apply(lambda x: 'fast_food' if x == '722513' else 'grocery') 
## re-roder columns: fipstate, fipscty, naics, naics_type, est
food = food[['fipstate', 'fipscty', 'naics', 'naics_type', 'est']]
### save to csv
food.to_csv('data/census/food.csv', index=False)