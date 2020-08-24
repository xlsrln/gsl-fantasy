import requests
import pandas as pd

keyfile = open('apikey','r')
baseurl = "http://aligulac.com/api/v1/"
authkey = {'apikey': keyfile.read()}

# Request
#myrequest = "match/?eventobj__uplink__parent=107374"
myrequest = "match/?eventobj__uplink__parent=110098&limit=100"

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
df = df[df['matchdata'].str.contains("Code S")]
df['round'] = df['matchdata'].apply(lambda x: x.split('Code S ')[1].split(' ')[0])
df = df.drop('matchdata', axis=1)
point_dict = {'Ro24': 1, 'Ro16': 2, 'Ro8': 3, 'Ro4': 4, 'Final': 5}
df['round_points'] = df['round'].apply(lambda x: point_dict[x])

print(df)

# from the match dataframe, calculate won/lost and points
point_df = df.filter(items=['player','round_points']).groupby('player').max('round_points')
point_df['won_games'] = df.filter(items=['player','won']).groupby('player').sum('won')['won']
point_df['lost_games'] = df.filter(items=['player','lost']).groupby('player').sum('lost')['lost']
point_df['points'] = point_df['round_points'] * 5 + point_df['won_games'] - point_df['lost_games']

print(point_df.sort_values('points', ascending=False))

print(point_df)


# read teams and join with results
from io import StringIO

teams = StringIO("""
NVP, MM Lolsters, SS telecom Z1, Varbergs Zergs, Grounzhog Day
rogue, ty, dark, maru, innovation
cure, dongraegu, parting, stats, solar
dream, trap, zest, soo, taeja
ragnarok, byun, sos, bunny, hurricane
""")

print(teams)

team_df = pd.read_csv(teams, sep=', ', header='infer')
team_df = team_df.melt(var_name='team', value_name='player')

result_df = team_df.merge(point_df, on='player', how='inner')
standing_df = result_df.filter(items=['team','points']).groupby('team').sum().sort_values(by='points', ascending=False)

print(result_df)
print(standing_df)

# write output
result_df.to_csv('results.csv', index=False)
standing_df.to_csv('standings.csv')
