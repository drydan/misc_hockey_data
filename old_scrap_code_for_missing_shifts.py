import requests
import json
import time
import re
import pandas as pd
import numpy as np


def scrape_single_game(gameId, folder, sleep_for=2):
    if 'HTML' in folder:
        str_gameId = str(gameId)
        reformatted_season_code = str_gameId[:4] + str(int(str_gameId[:4])+1)
    match folder:
        case 'JSON_Shifts':
            url = f'https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gameId}'
        case 'JSON_Events':
            url = f'https://api-web.nhle.com/v1/gamecenter/{gameId}/play-by-play'
        case 'HTML_Events':
            url = f'https://www.nhl.com/scores/htmlreports/{reformatted_season_code}/PL{str_gameId[4:]}.HTM'
        case 'HTML_Shifts_Away':
            url = f'https://www.nhl.com/scores/htmlreports/{reformatted_season_code}/TV{str_gameId[4:]}.HTM'
        case 'HTML_Shifts_Home':
            url = f'https://www.nhl.com/scores/htmlreports/{reformatted_season_code}/TH{str_gameId[4:]}.HTM'
        case 'HTML_Rosters':
            url = f'https://www.nhl.com/scores/htmlreports/{reformatted_season_code}/RO{str_gameId[4:]}.HTM'
        case _:
            print('no such folder')
            return None
    time.sleep(sleep_for)
    return requests.get(url)


def get_raw_html_shifts_df(game_id, home_shift_file, away_shift_file):
    data = []
    for team, shift_file in zip(['home','away'], [home_shift_file, away_shift_file]):
        players = re.split('class="playerHeading', shift_file)[1:]
        for player in players:
            rows = re.findall('<tr.*?>(.*?)</tr>',  player, re.DOTALL)
            row_data = [re.findall('>(.*?)<', row) for row in rows]
            shifts = [row for row in row_data if len(row)==6]
            player_data = re.search('>(.+?)<', player).groups()[0]
            player_data = [col.strip() for col in re.split('(\d+)|,', player_data) if col]
            if len(player_data) < 3:
                player_data.insert(0, '&nbsp;')
            shifts_data = [[game_id, team] + player_data + shift for shift in shifts]
            data.extend(shifts_data)
    return pd.DataFrame(
        data,
        columns = ['gameId', 'team', 'jerseyNumber', 'lastName', 'firstName', 'shiftNumber', 'period', 'startTime', 'endTime', 'duration', 'event'])


def get_cleaned_html_shifts_df(shift_df):
    shift_df.loc[shift_df['period'] == 'OT', 'period'] = '4'
    shift_df = shift_df[shift_df['period'] != 'SO'].copy().astype({'period':int}).drop('event', axis=1)
    shift_df['duration'] = shift_df['duration'].str.split(':').apply(lambda x: 60*int(x[0]) + int(x[1]))
    shift_df['startTime'] = shift_df['startTime'].str.split().str[0]
    shift_df['endTime'] = shift_df['endTime'].str.split().str[0]
    shift_df['startTimeSeconds'] = shift_df['startTime'].str.split(':').apply(lambda x: 60*int(x[0]) + int(x[1]))
    shift_df['endTimeSeconds'] = shift_df['endTime'].str.split(':').apply(lambda x: 60*int(x[0]) + int(x[1]) if len(x) == 2 else None)
    shift_df['endTimeSeconds'] = shift_df['endTimeSeconds'].fillna(shift_df['startTimeSeconds'] + shift_df['duration'])
    shift_df['startGameTimeSeconds'] = 1200*(shift_df['period']-1) + shift_df['startTimeSeconds']
    shift_df['endGameTimeSeconds'] = 1200*(shift_df['period']-1) + shift_df['endTimeSeconds']
    shift_df = shift_df.astype({'gameId':int, 'period':int})
    return shift_df


def match_team_rosters(html_rosters_df, json_rosters_df):

    def LevenshteinDistance(s, t):
        m = len(s)
        n = len(t)
        d = np.zeros((m+1, n+1))
        for i in range(0, m+1):
            d[i, 0] = i
        for j in range(0, n+1):
            d[0, j] = j
            for j in range(1, n+1):
                for i in range(1, m+1):
                    subCost = 0 if (s[i-1] == t[j-1]) else 1
                    d[i, j] = min([d[i-1, j] + 1, d[i, j-1] + 1, d[i-1, j-1] + subCost])
        return d[m, n]

    fix_names = {}
    # TRY PERFECT MATCHES...
    rosters_df = html_rosters_df.merge(json_rosters_df, how='outer', on=['gameId', 'team', 'fullName'])
    # SEPARATE LEFT OVERS...
    if rosters_df.playerId.isna().sum() > 0:
        fix_names = {}
        for homeAway in ['home', 'away']:
            unmatched_html = rosters_df[(rosters_df.playerId.isna()) & (rosters_df.team == homeAway)].fullName.tolist()
            unmatched_json = rosters_df[(rosters_df.html_idx.isna()) & (rosters_df.team == homeAway)].fullName.tolist()
            for s in unmatched_html:
                closest_match = sorted([((s, t),LevenshteinDistance(s, t)) for t in unmatched_json], key=lambda x: x[1])[0]
                fix_names[closest_match[0][1]] = closest_match[0][0]
        json_rosters_df['fullName'].replace(fix_names, inplace=True)
        rosters_df = html_rosters_df.merge(json_rosters_df, how='outer', on=['gameId', 'team', 'fullName'])
    return rosters_df[['gameId', 'team', 'jerseyNumber', 'lastName', 'firstName', 'playerId']], fix_names



name_check_dict = {}
list_of_dfs = []
SLEEP_TIME = 1.5
for game_id in range(2025020057, 2025020100+1):

    json_shifts = scrape_single_game(game_id, 'JSON_Shifts', sleep_for=SLEEP_TIME+np.random.random()).json()
    if json_shifts['total'] == 0:
        html_shifts_home = scrape_single_game(game_id, 'HTML_Shifts_Home', sleep_for=SLEEP_TIME+np.random.random())
        html_shifts_away = scrape_single_game(game_id, 'HTML_Shifts_Away', sleep_for=SLEEP_TIME+np.random.random())
        json_events = scrape_single_game(game_id, 'JSON_Events', sleep_for=SLEEP_TIME+np.random.random()).json()
        raw_html_shifts_df = get_raw_html_shifts_df(game_id, html_shifts_home.text, html_shifts_away.text)
        cleaned_html_shifts_df = get_cleaned_html_shifts_df(raw_html_shifts_df)

        html_rosters_df = (
            cleaned_html_shifts_df
            .assign(fullName = lambda x: x.firstName +'_'+ x.lastName+'_'+x.jerseyNumber)
            .loc[:,['gameId', 'team', 'firstName', 'lastName', 'jerseyNumber', 'fullName']]
            .drop_duplicates()
        ).rename_axis('html_idx').reset_index()

        json_rosters_df = (
            pd.json_normalize(json_events['rosterSpots'])
            .rename(columns = {'sweaterNumber':'jerseyNumber', 'firstName.default':'firstName', 'lastName.default':'lastName'})
            .astype({'jerseyNumber':str})
            .assign(
                team = lambda x: x.teamId.map({json_events['awayTeam']['id']:'away', json_events['homeTeam']['id']:'home'}),
                firstName = lambda x: x.firstName.str.upper(),
                lastName = lambda x: x.lastName.str.upper(),
                fullName = lambda x: x.firstName +'_'+ x.lastName+'_'+x.jerseyNumber,
                gameId = game_id
            ).loc[:, ['gameId', 'team', 'teamId', 'playerId', 'fullName']]
        )

        # can add manual intervention here to fix game specific errors before matching
        # i.e. if game_id == A: json_rosters_df.loc[json_rosters_df.fullName == B] = C
        rosters_df, fixed_names = match_team_rosters(html_rosters_df, json_rosters_df)
        list_of_dfs.append(
            cleaned_html_shifts_df.merge(rosters_df, how='left', on=['gameId', 'team', 'jerseyNumber', 'lastName', 'firstName'])
        )
        for i in fixed_names.items():
            name_check_dict.setdefault(i, []).append(game_id)

# FINAL DATAFRAME SAVE THIS SOMEWHERE...
shifts_df = pd.concat(list_of_dfs)

missing_ids = shifts_df.loc[shifts_df.playerId.isna(), ['gameId', 'team', 'firstName', 'lastName', 'jerseyNumber']].drop_duplicates()
if len(missing_ids) > 0:
    print(f'missing ids! {len(missing_ids)} instances')

# name_check_dict[(A, B)] = list of gameIds where non-perfect occured
print('check name_check_dict to catch any mistakes w/ non-perfect matches!')
for i in name_check_dict.keys():
    print(i)


shifts_df.gameId.unique()
