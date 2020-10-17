import requests
import pandas as pd
from io import StringIO
import datetime

keyfile = open('apikey','r')
baseurl = "http://aligulac.com/api/v1/"
authkey = {'apikey': keyfile.read().rstrip()}

# Request
# myrequest = "match/?eventobj__uplink__parent=110098&limit=100&order=-date" # GSL s2 2020
myrequest = "match/?eventobj__uplink__parent=107374&limit=100&order=-date" # GSL s3 2020

# get response from api
response = requests.get(baseurl + myrequest, params=authkey)
response.encoding = 'utf-8'
mystring = response.text.replace('null', 'None').replace('false', 'False').replace('true', 'True')

# eval string to dict, extract data from it
matches = eval(mystring)['objects']

data = []

for match in matches:    
    player1 = match['pla']['tag'].lower()
    player2 = match['plb']['tag'].lower()
    score1 = match['sca']
    score2 = match['scb']
    round = match['eventobj']['fullname']
    print(str(player1) + ', ' + str(score1) +', ' + str(score2) + ', ' + str(player2) + ', ' + str(round))
    data.append([player1,score1,score2,round])
    data.append([player2,score2,score1,round])
    
# data is now a table, make it a dataframe
df = pd.DataFrame(data)
df.columns = ['player','won','lost','matchdata']

last_match = df.iloc[1]['matchdata']

df = df[df['matchdata'].str.contains("Code S")]
df['round'] = df['matchdata'].apply(lambda x: x.split('Code S ')[1].split(' ')[0])
df = df.drop('matchdata', axis=1)
round_dict = {'Ro24': 1, 'Ro16': 2, 'Ro8': 3, 'Ro4': 4, 'Final': 5}
df['round_points'] = df['round'].apply(lambda x: 5 * round_dict[x])

# save match data to csv
df.to_csv('matches.csv', index=False)

# from the match dataframe, calculate won/lost and points
point_df = df.filter(items=['player','round_points']).groupby('player').max()
point_df['won_games'] = df.filter(items=['player','won']).groupby('player').sum()['won']
point_df['lost_games'] = df.filter(items=['player','lost']).groupby('player').sum()['lost']
point_df['points'] = point_df['round_points'] + point_df['won_games'] - point_df['lost_games']

# read teams 
# TODO: have as separate file?
teams = StringIO("""
NVP, MM Lolsters, SS telecom Z1, Varbergs Zergs, Grounzhog Day
rogue, ty, dark, maru, innovation
cure, dongraegu, parting, stats, solar
dream, trap, zest, soo, taeja
ragnarok, byun, sos, bunny, hurricane
""")

team_df = pd.read_csv(teams, sep=', ', header='infer')
team_df = team_df.melt(var_name='team', value_name='player')

# join teams with results to aggregate 
result_df = team_df.merge(point_df, on='player', how='outer').fillna('no team')
standing_df = result_df.query('team != "no team"')\
                        .filter(items=['team','points'])\
                        .groupby('team').sum().sort_values(by='points', ascending=False)

# write output
result_df.fillna(0).to_csv('results.csv', index=False, float_format='%g')
standing_df.fillna(0).to_csv('standings.csv', float_format='%g')

with open("latest_update", "w") as text_file:
    text_file.write("Updated: %s" % str(datetime.datetime.now()))
    text_file.write('<br><br>Latest round: %s' % str(last_match))

print(result_df)
