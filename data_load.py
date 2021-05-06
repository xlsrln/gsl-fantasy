import requests
import pandas as pd
from io import StringIO
import datetime

keyfile = open('apikey','r')
baseurl = "http://aligulac.com/api/v1/"
authkey = {'apikey': keyfile.read().rstrip()}

# teams for s2 2021
teams_2021_s2 = StringIO("""
Grounzhog Day, MM Lolsters, NVP, SS telecom Z1, Varbergs Zergs
innovation, ragnarok, byun, dark, armani
solar, trap, dream, sos, bunny
cure, ty, rogue, zoun, maru
""")

team_df_2021_s2 = pd.read_csv(teams_2021_s2, sep=', ', header='infer', engine='python')
team_df_2021_s2 = team_df_2021_s2.melt(var_name='team', value_name='player')

# teams for s3 2020
teams_2020 = StringIO("""
NVP, MM Lolsters, SS telecom Z1, Varbergs Zergs, Grounzhog Day
rogue, ty, dark, maru, innovation
cure, dongraegu, parting, stats, solar
dream, trap, zest, soo, taeja
ragnarok, byun, sos, bunny, hurricane
""")

team_df_2020 = pd.read_csv(teams_2020, sep=', ', header='infer', engine='python')
team_df_2020 = team_df_2020.melt(var_name='team', value_name='player')

# teams for s1 2021
teams_2021_1 = StringIO("""
NVP, MM Lolsters, SS telecom Z1, Varbergs Zergs, Grounzhog Day
rogue, ty, dark, maru, innovation
cure, zoun, prince, stats, solar
dream, trap, zest, armani, dongraegu
parting, byun, sos, bunny, ragnarok
""")

team_df_2021_1 = pd.read_csv(teams_2021_1, sep=', ', header='infer', engine='python')
team_df_2021_1 = team_df_2021_1.melt(var_name='team', value_name='player')

# event keys for 2021 s1
# events = [117965, 119400]

# event keys for 2021 s2
events = [121502]
team_df = team_df_2021_s2

def matches(event, print_bool=False):
    # Request
    myrequest = f"match/?eventobj__uplink__parent={event}&limit=200&order=-date"

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
        if print_bool:
            print(str(player1) + ', ' + str(score1) +', ' + str(score2) + ', ' + str(player2) + ', ' + str(round))
        data.append([player1,score1,score2,round])
        data.append([player2,score2,score1,round])

    # data is now a table, make it a dataframe
    df = pd.DataFrame(data)
    df.columns = ['player','won','lost','matchdata']

    last_match = df.iloc[1]['matchdata']
    
    with open("latest_update", "w") as text_file:
        text_file.write("Updated: %s" % str(datetime.datetime.now()))
        text_file.write('<br><br>Latest round: %s' % str(last_match))

    df = df[df['matchdata'].str.contains("Code S|Code A|Main Event")]
    
    df['ST'] = df['matchdata'].str.contains("Super Tournament")
    
    def round_finder(round_string):
        if 'Code S Playoffs' in round_string:
            return round_string.split('Code S ')[1].split(' ')[-1]
        if 'Code S Group Stage' in round_string:
            return 'Ro16'
        if 'Code S' in round_string:
            return round_string.split('Code S ')[1].split(' ')[0]
        if 'Super Tournament' in round_string:
            return round_string.split('Main Event ')[1].split(' ')[0]
        if 'Code A' in round_string:
            return 'Ro24'
    
    df['round'] = df['matchdata'].apply(round_finder)
    
    round_dict = {'Ro24': 1, 'Ro16': 2, 'Ro8': 3, 'Ro4': 4, 'Final': 5}
    
    df['round_points'] = df['round'].apply(lambda x: 5 * round_dict[x])
    df['round_points'] = df['round_points'] - 5 * df['ST']

    df = df.drop(['matchdata', 'ST'], axis=1)
    
    # save match data to csv
    df.to_csv('matches.csv', index=False)
    
    return df

def point_counter(match_df, teams, print_bool=False):

    # from the match dataframe, calculate won/lost and points
    point_df = match_df.filter(items=['player','round_points']).groupby('player').max()
    point_df['won_games'] = match_df.filter(items=['player','won']).groupby('player').sum()['won']
    point_df['lost_games'] = match_df.filter(items=['player','lost']).groupby('player').sum()['lost']
    point_df['points'] = point_df['round_points'] + point_df['won_games'] - point_df['lost_games']

    # join teams with results to aggregate
    result_df = teams.merge(point_df, on='player', how='outer').fillna(0)
    result_df['team'] = result_df['team'].replace(0, 'no team')

    # convert to integers from floats
    result_df = result_df.astype({"round_points":'int', "won_games":'int', "lost_games":'int', "points":'int'})

    # aggregate team standings
    standing_df = result_df.query('team != "no team"')\
                            .filter(items=['team','points'])\
                            .groupby('team').sum().sort_values(by='points', ascending=False)

    if print_bool:
        print(result_df)
        print(standing_df)

    return (result_df, standing_df)

## actually calculate stuff
results = []
standings = []

for event_key in events:
    df = matches(event_key)
    results.append(point_counter(df, team_df)[0])
    standings.append(point_counter(df, team_df)[1])

standings_df = pd.concat(standings).groupby(['team']).sum()
standings_df['rank'] = standings_df.rank(ascending=False)
standings_df = standings_df.astype({'rank': 'int'}).sort_values(by='points', ascending=False)

result_df = pd.concat(results)\
    .groupby(['player', 'team']).sum().reset_index()\
    .merge(standings_df, on='team', how='left', suffixes=('' , '_team'))\
    .sort_values(by=['rank', 'points'], ascending=[True, False])\
    .drop(['rank', 'points_team'], axis=1)

result_df = result_df.astype({'round_points': 'int',
                              'won_games':'int',
                              'lost_games':'int',
                              'points':'int'})

# write output
result_df.fillna(0).to_csv('output/results.csv', index=False, float_format='%g')
standings_df.fillna(0).to_csv('output/standings.csv', float_format='%g')
