import pymysql
import sqlalchemy
import requests
import pandas as pd
import time
from sqlalchemy import create_engine

from get_match_info import match_info
from get_personal import personal
from get_team import team
from get_event import events


api_list = []
api_dict = {}
api_name = []


def request_url(url):
    while True:
        r = requests.get(url)

        if r.status_code == 200:
            return r.json()
        elif r.status_code == 403: # api 갱신 필요
            print('you need api renewal')
            print(url)
            return r.json()
        
        time.sleep(5)
        print('5 seconds sleep!')



def main():

    summoner_dict = {}
    
    for api in api_name:
        summoner_dict[api] = pd.read_csv('summoner_api/'+api+'.csv')
    
    summonername_df = summoner_dict[api]['summonerName']
    
    
    
    engine = create_engine('mysql+pymysql://%s:%s@%s:%d/%s' % (user, passwd, server, port, db),
                           echo=False)
    conn = engine.connect()
    
    match_already_in_tuple = engine.execute('SELECT gameId FROM match_id').fetchall()
    match_already_in = [gameId_tuple[0] for gameId_tuple in match_already_in_tuple]
    personal_already_in_tuple = engine.execute('SELECT gameId FROM personal_summary GROUP BY gameId').fetchall()
    personal_already_in = [gameId_tuple[0] for gameId_tuple in personal_already_in_tuple]
    team_already_in_tuple = engine.execute('SELECT gameId FROM team_summary GROUP BY gameId').fetchall()
    team_already_in = [gameId_tuple[0] for gameId_tuple in team_already_in_tuple]
    event_already_in_tuple = engine.execute('SELECT gameId FROM event_monster GROUP BY gameId').fetchall()
    event_already_in = [gameId_tuple[0] for gameId_tuple in event_already_in_tuple]
    
    l = 0
    for i in range(25, 1401):

        begin_index = 0

        while True:
            match_url = 'https://kr.api.riotgames.com/lol/match/v4/matchlists/by-account/' + summoner_dict[api_name[l]]['accountId'][i]
            match_url += '?queue=420&beginIndex=' + str(begin_index) + '&api_key=' + api_list[l]
            match_json = request_url(match_url)

            matchid_list = pd.DataFrame(match_json['matches'])
            will_break = 0
            if len(matchid_list) == 0:
                break
            elif matchid_list.iloc[-1]['gameId'] < 4071405708:
                matchid_list = matchid_list[matchid_list['gameId'] > 4071405708]
                will_break = 1

                        
            gameid_df = matchid_list['gameId']
            match_df = pd.DataFrame(columns=['gameId'])
            personal_df_list = [pd.DataFrame(columns=['gameId'])] * 3
            team_df = pd.DataFrame(columns=['gameId'])
            event_df_list = [pd.DataFrame(columns=['gameId'])] * 6

            for j in range(len(gameid_df)):
                now_matchid = gameid_df[j].item()

                already_in = [0, 0, 0, 0]
                if now_matchid in match_already_in:
                    already_in[0] = 1
                if now_matchid in personal_already_in:
                    already_in[1] = 1
                if now_matchid in event_already_in:
                    already_in[2] = 1
                if now_matchid in team_already_in:
                    already_in[3] = 1


                if sum(already_in) < 4:
                    data_url = 'https://kr.api.riotgames.com/lol/match/v4/matches/' + str(now_matchid) + '?api_key=' + api_list[l]
                    data_json = request_url(data_url)
                    match = pd.DataFrame(list(data_json.values()), index=list(data_json.keys())).T

                    time_url = 'https://kr.api.riotgames.com/lol/match/v4/timelines/by-match/' + str(now_matchid) + '?api_key=' + api_list[l]
                    time_json = request_url(time_url)
                    frame = pd.DataFrame(time_json['frames'])['participantFrames']
                    event = pd.DataFrame(time_json['frames'])['events']


                    if already_in[0] == 0:
                        summoner_ids = pd.DataFrame(dict(pd.DataFrame(match['participantIdentities'].iloc[0])['player'])).T['summonerId']
                        summoner_list = []
                        for k in range(10):
                            summoner_url = 'https://kr.api.riotgames.com/lol/league/v4/entries/by-summoner/' + summoner_ids.iloc[k] 
                            summoner_url += '?api_key=' + api_list[l]
                            summoner_list.append(pd.DataFrame(request_url(summoner_url)))

                        new_match_df = match_info(match, summoner_list)
                        match_df = match_df.append(new_match_df)

                    if already_in[1] == 0:
                        new_personal_df_list = personal(match, frame)       
                        for k in range(3):
                            personal_df_list[k] = personal_df_list[k].append(new_personal_df_list[k])

                    if already_in[2] == 0:
                        new_event_df_list = events(event)
                        for k in range(6):
                            new_event_df_list[k]['gameId'] = int(now_matchid)
                            event_df_list[k] = event_df_list[k].append(new_event_df_list[k])

                    if already_in[3] == 0:
                        new_team_df = team(match, new_event_df_list[5])
                        team_df = team_df.append(new_team_df)

                    print(i, now_matchid, 'data in!')

                else:
                    print(i, now_matchid, 'passed!')


                l += 1
                if l == 8:
                    l = 0    
            
            match_already_in_tuple = engine.execute('SELECT gameId FROM match_id').fetchall()
            match_already_in = [gameId_tuple[0] for gameId_tuple in match_already_in_tuple]
            match_df = match_df[~match_df['gameId'].isin(match_already_in)]
            match_df.to_sql('match_id', con=engine, index=False, if_exists='append')
            
            personal_already_in_tuple = engine.execute('SELECT gameId FROM personal_summary GROUP BY gameId').fetchall()
            personal_already_in = [gameId_tuple[0] for gameId_tuple in personal_already_in_tuple]
            for k in range(3):
                personal_df_list[k] = personal_df_list[k][~personal_df_list[k]['gameId'].isin(personal_already_in)]
            personal_df_list[0].to_sql('personal_summary', con=engine, index=False, if_exists='append')
            personal_df_list[1].to_sql('personal_rune', con=engine, index=False, if_exists='append')
            personal_df_list[2].to_sql('personal_detail', con=engine, index=False, if_exists='append') 
            
            event_already_in_tuple = engine.execute('SELECT gameId FROM event_monster GROUP BY gameId').fetchall()
            event_already_in = [gameId_tuple[0] for gameId_tuple in event_already_in_tuple]
            for k in range(6):
                event_df_list[k] = event_df_list[k][~event_df_list[k]['gameId'].isin(event_already_in)]
            event_df_list[0].to_sql('event_kill', con=engine, index=False, if_exists='append')
            event_df_list[1].to_sql('event_ward', con=engine, index=False, if_exists='append')
            event_df_list[2].to_sql('event_item', con=engine, index=False, if_exists='append')
            event_df_list[3].to_sql('event_skillup', con=engine, index=False, if_exists='append')
            event_df_list[4].to_sql('event_building', con=engine, index=False, if_exists='append')
            event_df_list[5].to_sql('event_monster', con=engine, index=False, if_exists='append')   
            
            team_already_in_tuple = engine.execute('SELECT gameId FROM team_summary GROUP BY gameId').fetchall()
            team_already_in = [gameId_tuple[0] for gameId_tuple in team_already_in_tuple]
            team_df = team_df[~team_df['gameId'].isin(team_already_in)]
            team_df.to_sql('team_summary', con=engine, index=False, if_exists='append')  
              
                               
            
            print(i, 'th database uploaded!\n')
            
            if will_break == 1:
                break        
            begin_index += 100

    
        print()
        print(i, 'th database upload completed!\n')
        
        
    conn.close()
    engine.dispose()
    
    
    
if __name__ == "__main__":
    main()
