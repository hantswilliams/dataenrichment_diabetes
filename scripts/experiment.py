import pandas as pd 

## configure pandas so expoential notation is not used
pd.set_option('display.float_format', lambda x: '%.3f' % x)



#############################################
## load in fips data
fips = pd.read_csv('data_clean/fips.csv', dtype={'statefp': 'str', 
                                                 'placefp': 'str', 
                                                 'countyfp': 'str', 
                                                 'fips_places': 'str',
                                                 'fips_state_county': 'str'})
## keep only vars that needed: STATE, STATEFP, PLACEFP, COUNTYNAME,TYPE,PLACENAME
# fips = fips[['state', 'statefp', 'countyfp', 'fips_state_county', 'fips_places', 'placefp', 'countyname', 'type', 'placename']]
## rename placename to fips_placename, countyname to fips_countyname, state to fips_state
fips = fips.rename(columns={'placename': 'fips_placename', 'countyname': 'fips_countyname', 'state': 'fips_state'})
fips = fips[['fips_state_county', 'fips_places', 'fips_placename', 'fips_countyname', 'fips_state']]
#############################################





#############################################
#### COUNTY LEVEL 
## load in census_foodstores [COUNTYFP, STATEFP -> fips_county]
census_fs = pd.read_csv('data_clean/census_foodstores.csv', dtype={'fips_county': 'str'})
## add in prefix census_ to all columns
census_fs.columns = ['census_' + col for col in census_fs.columns]
## keep only core values: census_naics_type  census_est_businesses census_fips_county
len(census_fs)
#############################################


#############################################
## load in cnt_transit // [PLACENS -> fips_places]
cnt_transit = pd.read_csv('data_clean/cnt_transit.csv', dtype={'fips_county': 'str'})
len(cnt_transit)
#############################################


#############################################
#####CENSUS TRACT LEVEL 
## load in data from data_clean folder
## cdc // lowest level is TractFIPS 
cdc_dm = pd.read_csv('data_clean/cdc_diabetes.csv', dtype={'CountyFIPS': 'str', 'LocationID': 'str'})
## add in cdc_ prefix to all columns
cdc_dm.columns = ['cdc_' + col for col in cdc_dm.columns]
len(cdc_dm)
## keep only core values: cdc_CountyFIPS cdc_LocationID cdc_Short_Question_Text  cdc_Data_Value
cdc_dm = cdc_dm[['cdc_LocationID', 'cdc_Data_Value']]
## rename cdc_Data_Value to diabetes_prevalence
cdc_dm = cdc_dm.rename(columns={'cdc_Data_Value': 'diabetes_prevalence'})
## can match up CountyFIPS with PLACEFP in fips_places, or with STATEFP and COUNTYFP in fips_county
#############################################


#############################################
##### CENSUS TRACT LEVEL
## load in usda_foodatlas - can use CensusTract to match with LocationID in cdc 
usda_fa = pd.read_csv('data_clean/usda_foodatlas.csv', dtype={'CensusTract': 'str'})
## add in usda_ prefix to all columns
usda_fa.columns = ['usda_' + col for col in usda_fa.columns]
## keep only core values: usda_CensusTract usda_Urban  usda_LILATracts_1And10  usda_PovertyRate  usda_MedianFamilyIncome
usda_fa = usda_fa[['usda_County', 'usda_meanPovertyRate',  'usda_meanFamilyIncome',  'usda_percentLowFoodAccess']]
len(usda_fa)
#############################################





#### use fips as initial base for merging census_fs, cnt_transit
fips_data = fips.copy()
len(fips_data)
fips_data = pd.merge(fips_data, census_fs, how='left', left_on='fips_state_county', right_on='census_fips_county')
## remove census_fips_county
fips_data.drop(columns=['census_fips_county'], inplace=True)
## add in usda_fa by usda_County and fips_countyname
fips_data = pd.merge(fips_data, usda_fa, how='left', left_on='fips_countyname', right_on='usda_County')
## remove usda_County
fips_data.drop(columns=['usda_County'], inplace=True)
## add in cdc_dm by cdc_LocationID and fips_state_county
fips_data = pd.merge(fips_data, cdc_dm, how='left', left_on='fips_state_county', right_on='cdc_LocationID')
## remove cdc_LocationID
fips_data.drop(columns=['cdc_LocationID'], inplace=True)
## add in cnt_transit by fips_places
fips_data = pd.merge(fips_data, cnt_transit, how='left', left_on='fips_state_county', right_on='fips_county')
## remove fips_county
fips_data.drop(columns=['fips_county'], inplace=True)
## save to csv 
fips_data.to_csv('data_clean/complete_data.csv', index=False)
len(fips_data)











