from __future__ import print_function

import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import re
import pandas as pd
import numpy as np

#########################################################

debug=True

# gsl s2
debug_url = 'https://docs.google.com/spreadsheets/d/1kVVGd8-fCbhimea2kZSH15JRH7eJz092pS_nlIkBRfU/edit#gid=0'
debug_sheetrange='B:H'

# gsl s3
live_url = 'https://docs.google.com/spreadsheets/d/1_yDiuUkeW6vj0g-5jBEdaiNeSrTXy7yvl2vdozF168Y/edit#gid=0'
live_sheetrange='B:I'

rounds=['Ro24','Ro16','Ro8','Ro4','Final']

adffile='output/matches.csv'

#########################################################

scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# construct the google service
creds = None
# The file token.pickle stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', scopes)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)

service = build('sheets','v4',credentials=creds)

##############################################################

if debug:
    url=debug_url
    sheetrange=debug_sheetrange
else:
    url=live_url
    sheetrange=live_sheetrange

id = re.search('/spreadsheets/d/([a-zA-Z0-9-_]+)',url).group(1)
#print(id)

# the relevant range. for now assumed to be constant across sheets

# Call the Sheets API
sheet = service.spreadsheets()

title = sheet.get(spreadsheetId=id,ranges=None,includeGridData=False,
    fields='properties(title)').execute()['properties']['title']
print(title)

#round='ro8'
# these sheets will be checked

def crunch_sheet(round):
    bookrange='!'.join([round,sheetrange])

    result=sheet.values().get(spreadsheetId=id,range=bookrange).execute()
    values=result.get('values', [])

    # create the dataset
    df=pd.DataFrame(values[1:]).replace('',np.NaN,regex=False)
    df=df[~df[0].isna()].reset_index()

    # we need to handle scores if we're in a round where scores are predicted
    # for this we need to tag each row with match
    df['ismatch']=(~(df[0]=='score') & df[1].isna())
    df['match']=''

    for i in range(0,df.shape[0]):
        if df.loc[i,'ismatch']==True:
            match=df.loc[i,0]
        df.loc[i,'match']=match

    # unpivot main dataset 
    df=df[(~df['ismatch'])].drop(columns=[1,'ismatch','index'])
    df=pd.melt(df,id_vars=[0,'match']).rename(columns={0:'player'})

    sc=df[df['player']=='score']

    df=df[(df['player']!='score') & (df['value'].notnull())].drop(columns=['value'])

    # massage the scores
    if sc.shape[0]>0:
        wl=pd.DataFrame(sc['value'].str.split('-').values.tolist(),
            columns=['won','lost']).astype(int)
        sc=pd.concat([sc.reset_index(),wl],axis=1)[['match','variable','won','lost']]

        # connect the scores
        df=pd.merge(df,sc,on=['match','variable'],how='left')

    # massage the players dataset
    df['round']=round

    # # connect the names
    names=pd.DataFrame(map(str.lower,values[0]),columns=['predictor'])
    names['variable']=names.index

    df=pd.merge(df,names,on=['variable']).drop(columns=['variable'])

    return df

#sdf_test=list(map(crunch_sheet,rounds))
sdf=pd.concat(map(crunch_sheet,rounds))

##############################################################

# obtain the aligulac dataset csv
adf=pd.read_csv(adffile)
adf['loss']=(adf['won']<adf['lost']).astype(int)

# combine prediction with result. will be done in cases
# 1. the group rounds
r1=['Ro24','Ro16']
sdf1=sdf[sdf['round'].isin(r1)]
adf1=adf[adf['round'].isin(r1)]

# double elimination
adf1=adf1.groupby(['round','player'],as_index=False).sum()
adf1=adf1[adf1['loss']<2]

# combine match results and predictions
df1=pd.merge(
    sdf1,adf1[['round','player']],
    on=['round','player'],how='left',indicator=True)

# score, 2 right per group gives 3 points
df1['score']=(df1['_merge']=='both').astype(int)
df1=df1.groupby(['round','predictor','match'],as_index=False).sum()
df1.loc[df1['score']==2,'score']=3
df1=df1.groupby(['round','predictor'],as_index=False).sum()

# 2. playoffs
r2=['Ro8','Ro4','Final']
sdf2=sdf[sdf['round'].isin(r2)]
adf2=adf[(adf['round'].isin(r2)) & (adf['loss']==0)]

if sdf2.shape[0]>0:
    df2=pd.merge(
        sdf2,adf2[['round','player']],
        on=['round','player'],how='left',indicator='w_corr').merge(
        adf2[['round','player','won','lost']],
        on=['round','player','won','lost'],how='left',indicator='s_corr')

    df2['score']=(df2['w_corr']=='both').astype(int)*2
    df2.loc[df2['s_corr']=='both','score']+=1
    df2=df2.groupby(['round','predictor'],as_index=False).sum()

    # # combine 1. and 2
    df=pd.concat([df1,df2]).drop(columns=['won','lost'])
else:
    df=df1

agg=df.groupby(['predictor'],as_index=False).sum().sort_values(
    by=['score'],ascending=False)

agg.fillna(0).to_csv('predscores.csv', index=False, float_format='%g')