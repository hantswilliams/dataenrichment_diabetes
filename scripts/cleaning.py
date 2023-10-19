import pandas as pd 
import os


#############################################
## load in fips county data, keep 0's in front of fips code
fips_county = pd.read_csv('data/fips/fips_counties.txt', sep='|', dtype={'STATEFP': 'str', 'COUNTYFP': 'str'})
fips_county.drop(columns=['CLASSFP', 'FUNCSTAT'], inplace=True)
len(fips_county)

## load in fips places data (this is for CNT transit) - lowest level is place
fips_places = pd.read_csv('data/fips/fips_places.txt', sep='|', dtype={'STATEFP': 'str', 'PLACEFP': 'str'})
fips_places.drop(columns=['CLASSFP', 'FUNCSTAT'], inplace=True)
len(fips_places)

## merge fips to fips COUNTYNAME and STATEFP, and fips_places to fips_places COUNTIES and STATEFP
fips = fips_places.merge(fips_county, how='left', left_on=['STATEFP', 'COUNTIES'], right_on=['STATEFP', 'COUNTYNAME'])
len(fips)

## vars to keep: STATE_x, STATEFP, PLACEFP, COUNTYFP, PLACENS, COUNTYNS, COUNTYNAME, TYPE, PLACENAME, COUNTIES
fips = fips[['STATE_x', 'STATEFP', 'PLACEFP', 'COUNTYFP', 'COUNTYNAME', 'TYPE', 'PLACENAME', 'COUNTIES']]

## rename STATE_x to STATE
fips.rename(columns={'STATE_x': 'STATE'}, inplace=True)

fips['fips_state_county'] = fips['STATEFP'] + fips['COUNTYFP']

fips['fips_places'] = fips['STATEFP'] + fips['PLACEFP']

## lower case all columns
fips.columns = [col.lower() for col in fips.columns]

## save to csv
fips.to_csv('data_clean/fips.csv', index=False)
#############################################





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
# remove double quotes from fip_place and name values
df['fip_place'] = df['fip_place'].str.replace('"', '')
df['name'] = df['name'].str.replace('"', '')
fips_small = fips[['fips_places', 'fips_state_county']]
# merge df and fips_small on fips_places
df = df.merge(fips_small, how='left', left_on='fip_place', right_on='fips_places')
## sort by fips_state_county
df = df.sort_values(by='fips_state_county')
## group by fips_state_county and get a mean and meadian alltransit_performance_score
df_groupped = df.groupby(['fips_state_county'])['alltransit_performance_score'].agg(['mean', 'median', 'min', 'max']).reset_index()
# rename fips_state_county to fips_county, and add in prefix cnt_ to all columns
df_groupped.columns = ['fips_county', 'cnt_mean_alltransit_performance_score', 'cnt_median_alltransit_performance_score', 'cnt_min_alltransit_performance_score', 'cnt_max_alltransit_performance_score']
# write to csv
df_groupped.to_csv('data_clean/cnt_transit.csv', index=False)





###### CDC data #######
# read in data
df = pd.read_csv('data/cdc/cdc_2023_county.csv', dtype={'CountyFIPS': 'str', 'LocationID': 'str'})
# keep only data where category is health outcomes
df = df[df['Category'] == 'Health Outcomes']
# keep only diabetes data
df = df[df['MeasureId'] == 'DIABETES']
# keep only Data_Value_Type = 'Crude Prevalence'
df = df[df['Data_Value_Type'] == 'Crude prevalence']
df_small = df[['StateAbbr', 'StateDesc', 'LocationName', 'LocationID', 'Short_Question_Text', 'Data_Value']]
## sort by LocationID
df_small = df_small.sort_values(by='LocationID')
## drop if StateAbbr is US
df_small = df_small[df_small['StateAbbr'] != 'US']
df_small.to_csv('data_clean/cdc_diabetes.csv', index=False)





###### Census data #######
# read in data which is a .txt file that is comma separated, and the first 1 rows are header
df = pd.read_csv('data/census/cbp19co.txt', sep=',', dtype={'fipstate': 'str', 'fipscty': 'str'})
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
## make sure fipstate and fipscty are strings
food['fipstate'] = food['fipstate'].astype('str')
food['fipscty'] = food['fipscty'].astype('str')
## create fips_county 
food['fips_county'] = food['fipstate'] + food['fipscty']
## drop fipstate and fipscty
food.drop(columns=['fipstate', 'fipscty'], inplace=True)
## rename est to est_businesses
food.rename(columns={'est': 'est_businesses'}, inplace=True)

## transform long to wide, with indexes being fips_county; and columns being either fast_food or grocery, and values being est_businesses
test = food.pivot(index='fips_county', columns='naics_type', values='est_businesses').reset_index()

## save to csv, while keeping the 0s in front of fipstate and fipscty
test.to_csv('data_clean/census_foodstores.csv', index=False)




#### usda food atlas ####
## read in data
df = pd.read_csv('data_clean/usda_foodatlas.csv', dtype={'CensusTract': 'str'})

## create a new dataframe where we create a group by County, and we get the average PovertyRate and MedianFamilyIncome
pivot1 = df.groupby(['County'])[['PovertyRate', 'MedianFamilyIncome']].mean().reset_index()

## group by County and then sum the LILATracts_1And10 and divide by the total number of tracts in the county
pivot2 = df.groupby(['County'])['LILATracts_1And10'].sum().reset_index()
pivot2['LILATracts_1And10'] = pivot2['LILATracts_1And10'] / df.groupby(['County'])['LILATracts_1And10'].count().reset_index()['LILATracts_1And10']
pivot2['LILATracts_1And10'] = pivot2['LILATracts_1And10'] * 100

## add together pivot1 and pivot2
pivot = pivot1.merge(pivot2, how='left', left_on='County', right_on='County')

## rename PovertyRate to meanPoveryRate, MedianFamilyIncome to meanFamilyIncome, and LILATracts_1And10 to percentLowFoodAccess
pivot.rename(columns={'PovertyRate': 'meanPovertyRate', 'MedianFamilyIncome': 'meanFamilyIncome', 'LILATracts_1And10': 'percentLowFoodAccess'}, inplace=True)
## save to csv
pivot.to_csv('data_clean/usda_foodatlas.csv', index=False)