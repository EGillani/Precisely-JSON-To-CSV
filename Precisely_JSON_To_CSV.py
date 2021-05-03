#Flatten JSONS outputs from Precisely demographic responses and converts them to csv files that could be stored in a sql database for further analysis 
#https://developer.precisely.com/apis/demographics
# April 2021
import pandas as pd
import requests
import os
import json
import glob
import re
import sys
import csv
import time 
import itertools
import pathlib

def make_final_df(df_boundary,df):
    #concatinating 
    df = pd.concat([df_boundary,df], axis=1)
    #populate the boundaryinfo columns
    df.loc[:,'boundaryId'] = df.iloc[0]['boundaryId']
    df.loc[:,'boundaryType'] = df.iloc[0]['boundaryType']
    df.loc[:,'boundaryRef'] = df.iloc[0]['boundaryRef']
    df.loc[:,'theme'] = df.iloc[0]['theme']
    return df 

def append_to_csv(state,df, varType):
    doesCSVexist = os.path.isfile('demographics_' + state + '_'+ varType +'.csv')
    if doesCSVexist is True: 
        #don't want headers to be repeated if appended 
        df.to_csv('demographics_' + state + '_' + varType + '.csv', mode='a',header=False, index=False)
    else:
        df.to_csv('demographics_' + state + '_' + varType + '.csv', index=False)


def json_parse_individual(json_response, theme):

    #get boundary group information
    df_boundary_info = pd.io.json.json_normalize(json_response[u'boundaries'][u'boundary'])
    df_boundary_info['theme'] = theme

    #create dataframe for individualValueVariable
    df_individualVal = pd.io.json.json_normalize(json_response[u'boundaryThemes'][0][theme],'individualValueVariable')
    #concatinate fields with boundary info
    df_individualVal_final =  make_final_df(df_boundary_info,df_individualVal)
    #check if csv exists, if it doesn't then add....

    return df_individualVal_final

def json_parse_range(json_response, theme):

    #get boundary group information
    df_boundary_info = pd.io.json.json_normalize(json_response[u'boundaries'][u'boundary'])
    df_boundary_info['theme'] = theme
    
    df_rangeVal = pd.io.json.json_normalize(json_response[u'boundaryThemes'][0][theme]['rangeVariable'],'field',['name', 'alias', 'description', 'baseVariable', 'year'], record_prefix='field_', errors='ignore')
    #re-ordering so field columns show up at the end and adding the prefix 
    df_rangeVal = df_rangeVal.reindex(columns=['name', 'alias', 'description', 'baseVariable', 'year', 'field_name', 'field_description', 'field_value'])
    #concatenate and make into csv
    df_rangeVal_final =  make_final_df(df_boundary_info,df_rangeVal)

    return df_rangeVal_final

#this should be run only after you have completed collecting all the json files from precisely and now need to flatten them.
def main():
    #choose state you were interested in 
    state = 'California'
    demographic_themes = ["populationTheme", "raceAndEthnicityTheme","healthTheme","educationTheme", "incomeTheme", "assetsAndWealthTheme", "householdsTheme", "housingTheme", "employmentTheme", "expenditureTheme", "supplyAndDemandTheme"]
    
    # create folder if doesn't exist 
    parent_dir = os.getcwd() + '/data_csv_format'
    the_path = os.path.join(parent_dir, state)
    file = pathlib.Path(the_path)

    if file.exists() is not True:
    #make directory 
        os.mkdir(the_path)

    BGID_List = []
    for name in glob.glob('*.json'):
        #remove path details
        name = os.path.basename(name)
        #add to list
        BGID_List.append(name)

    completed_json_list = []
    #this is for if script broke somehow and they are remaining boundarygroups that need to be appended to csv
    #have a listofBG excel file that checks with ones are completed 
    doesCSVexist = os.path.isfile('demographics_' + state + '_listofBG.csv')
    if doesCSVexist is True: 
        with open('demographics_' + state + '_listofBG.csv','r') as f:
            reader =  csv.reader(f)
            next(reader, None)  # skip the headers
            for row in reader:
                completed_json_list.append(row[0])
            #do not repeat BGID that have already been transferred 
            BGID_List = list(itertools.filterfalse(lambda x:x in completed_json_list,BGID_List))
    
    length = len(BGID_List)
    completed_BGID_List = []
    start = time.time()
    for i in range(length):
        #open the boundaryfile we need to move to csv 
        try:
            filePath = state +'/' + BGID_List[i]
            jsonData = json.load(open(filePath))
            if len(jsonData) is 0:
                continue
        except:
            print("Something wrong with opening the file: " + BGID_List[i])
            continue 

        list_of_df_individualVar = []
        list_of_df_rangeVar = []

        for theme in demographic_themes:
            try:
                #flatten each theme 
                df_temp_individualVar = json_parse_individual(jsonData, theme)
                #concat to list 
                list_of_df_individualVar.append(df_temp_individualVar)
                if theme is not "supplyAndDemandTheme":
                    df_temp_rangeVar = json_parse_range(jsonData, theme)
                    list_of_df_rangeVar.append(df_temp_rangeVar)
            except: 
                print(BGID_List[i] + " not flattened")
                break

        try:
            #convert of list of df into one large df
            if len(list_of_df_individualVar) < 11 or len(list_of_df_rangeVar) < 10:
                raise Exception(BGID_List[i] +  "not all themes were completed")

            df_individualVar = pd.concat(list_of_df_individualVar, ignore_index=True)
            df_rangeVar = pd.concat(list_of_df_rangeVar, ignore_index=True)
        except: 
            print(BGID_List[i] + "couldnt concat")
            continue 

                #add to csv
        try:
            append_to_csv(state,df_individualVar, 'individualValueVariable')
            append_to_csv(state,df_rangeVar, 'rangeVariable')
            #append to completed list and print
            completed_BGID_List.append(BGID_List[i])
            print("Finished " + BGID_List[i])
        except: 
            print(BGID_List[i] + "couldnt store to csv")
            #continue 

    
    print(f"took {time.time() - start}")
    df_completed = pd.DataFrame (completed_BGID_List,columns=["Completed_BGID"])
    append_to_csv(state,df_completed, 'listofBG')

    doesCSVexist = os.path.isfile('demographics_' + state + '_listofBG.csv')
    if doesCSVexist is True: 
        with open('demographics_' + state + '_listofBG.csv','r') as f:
            reader =  csv.reader(f)
            next(reader, None)  # skip the headers
            for row in reader:
                completed_json_list.append(row[0])
            #do not repeat BGID that have already been transferred 
            BGID_List = list(itertools.filterfalse(lambda x:x in completed_json_list,BGID_List))

    print("number of json not translated:" + str(len(BGID_List)))
        


if __name__ == "__main__":
    main()