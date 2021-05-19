import pymysql
import sqlalchemy
import requests
import pandas as pd
import time
import asyncio
import aiohttp
from sqlalchemy import create_engine

from get_match_info import match_info
from get_personal import personal
from get_team import team
from get_event import events


api_list = []
api_dict = {}
api_name = []
api_mine = ''


async def quest_df_async(url):
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession() as session:
        try:
            response = await session.get(url, timeout=timeout)
            while True:
                if response.status == 200:
                    text = await response.json()
                    return pd.DataFrame(text)
                elif response.status == 504:
                    await asyncio.sleep(0.2)
                    print('gateway timeout, request once more!', url)
                    response = await session.get(url)
                else:
                    await asyncio.sleep(30.0)
                    print('request limit.', url)
                    response = await session.get(url)
        except:
            text = request_url(url)
            return pd.DataFrame(text)
        
async def quest_json_async(url):
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession() as session:
        try:
            response = await session.get(url, timeout=timeout)
            while True:
                if response.status == 200:
                    text = await response.json()
                    return text
                elif response.status == 504:
                    await asyncio.sleep(0.2)
                    #print('gateway timeout, request once more!', url)
                    response = await session.get(url)
                else:
                    await asyncio.sleep(40.0)
                    #print('request limit.', url)
                    response = await session.get(url)
        except:
            text = request_url(url)
            return text


def request_url(url):
    while True:
        r = requests.get(url)

        if r.status_code == 200:
            return r.json()
        elif r.status_code == 403: # api 갱신 필요
            print('you need api renewal')
            print(url)
            return r.json()
        
        time.sleep(20)
        print(url)
        print('20 seconds sleep!')



async def main():

    summoner_dict = {}
    for i in range(26):
        summoner_dict[api_name[i]] = pd.read_csv('../summoner_api/'+api_name[i]+'.csv')
    
    summonername_df = summoner_dict[api_name[0]]['summonerName']
    
    
    engine = create_engine('mysql+pymysql://%s:%s@%s:%d/%s' % (user, passwd, host, port, db),
                           echo=False)
    conn = engine.connect()

    
    match_already_in_tuple = engine.execute('SELECT gameId FROM match_id').fetchall()
    match_already_in = [gameId_tuple[0] for gameId_tuple in match_already_in_tuple]
    personal_already_in_tuple = engine.execute('SELECT gameId FROM personal_summary GROUP BY gameId').fetchall()
    personal_already_in = [gameId_tuple[0] for gameId_tuple in personal_already_in_tuple]
    event_already_in_tuple = engine.execute('SELECT gameId FROM event_kill GROUP BY gameId').fetchall()
    event_already_in = [gameId_tuple[0] for gameId_tuple in event_already_in_tuple]
    team_already_in_tuple = engine.execute('SELECT gameId FROM team_summary GROUP BY gameId').fetchall()
    team_already_in = [gameId_tuple[0] for gameId_tuple in team_already_in_tuple]

    all_already_in = set(match_already_in).intersection(personal_already_in)
    all_already_in = all_already_in.intersection(event_already_in)
    all_already_in = all_already_in.intersection(team_already_in)
    
    
    data_url = 'https://kr.api.riotgames.com/lol/match/v4/matches/'
    time_url = 'https://kr.api.riotgames.com/lol/match/v4/timelines/by-match/'
    summoner_url = 'https://kr.api.riotgames.com/lol/league/v4/entries/by-summoner/'
    
    for i in range(1200, len(summoner_dict[api_name[0]])):
        
        matchid_df = pd.DataFrame()
        begin_index = 0

        while True:
            match_url = 'https://kr.api.riotgames.com/lol/match/v4/matchlists/by-account/' + summoner_dict[api_name[0]]['accountId'][i]
            match_url += '?queue=420&beginIndex=' + str(begin_index) + '&api_key=' + api_list[0]
            match_json = request_url(match_url)

            matchid_list = pd.DataFrame(match_json['matches'])
            will_break = 0
            if len(matchid_list) == 0:
                break
                
            matchid_df = pd.concat([matchid_df, matchid_list], ignore_index=True)
            
            if matchid_list.iloc[-1]['gameId'] < 4071405708:
                break
            
            begin_index += 100
       
        matchid_df = matchid_df[matchid_df['gameId'] > 4071405708]
        gameid_df = list(matchid_df['gameId'])
        total_game = len(gameid_df)

        gameid_df = [gameid for gameid in gameid_df if gameid not in all_already_in]
        uninserted_game = len(gameid_df)
        print('summoner', summonername_df[i], 'total', total_game, 'games played,', uninserted_game, 'games not yet inserted.')
        
        # only i,j,k = 45,47,18 has problem 
        # aiohttp.client_exceptions.ClientPayloadError: Response payload is not completed
        
        for j in range(uninserted_game//27+1):
            data_url_list, time_url_list, gameid_list = [], [], []
            for k in range(27):
                if (j*27+k) == uninserted_game:
                    break
                data_url_list.append(data_url+str(gameid_df[j*27+k])+'?api_key='+api_list[k+1])
                time_url_list.append(time_url+str(gameid_df[j*27+k])+'?api_key='+api_list[k+1])
                gameid_list.append(gameid_df[j*27+k])
        
            data_json_list = await asyncio.gather(*[quest_json_async(urls) for urls in data_url_list])
            data_df_list = [pd.DataFrame(list(data_json.values()), index=list(data_json.keys())).T for data_json in data_json_list]
            
            time_json_list = await asyncio.gather(*[quest_json_async(urls) for urls in time_url_list])
            frame_df_list = [pd.DataFrame(time_json['frames'])['participantFrames'] for time_json in time_json_list]
            event_df_list = [pd.DataFrame(time_json['frames'])['events'] for time_json in time_json_list]
            
            
            match_df = pd.DataFrame(columns=['gameId'])
            personal_df_list = [pd.DataFrame(columns=['gameId'])] * 3
            team_df = pd.DataFrame(columns=['gameId'])
            events_df_list = [pd.DataFrame(columns=['gameId'])] * 5
            
            for k in range(len(gameid_list)):
                now_matchid = gameid_list[k]
                match, frame, event = data_df_list[k], frame_df_list[k], event_df_list[k]
                already_in = [1, 1, 1, 1]
                
                if now_matchid not in match_already_in:
                    already_in[0] = 0
                    summoner_ids = pd.DataFrame(dict(pd.DataFrame(match['participantIdentities'].iloc[0])['player'])).T['summonerId']
                    summoner_url_list = [summoner_url+summonerId+'?api_key='+api_list[k+1] for summonerId in summoner_ids]
                    summoner_list = await asyncio.gather(*[quest_df_async(urls) for urls in summoner_url_list])
                    new_match_df = match_info(match, summoner_list)
                    match_df = match_df.append(new_match_df)
                    
                if now_matchid not in personal_already_in:
                    already_in[1] = 0
                    new_personal_df_list = personal(match, frame)       
                    for ii in range(3):
                        personal_df_list[ii] = personal_df_list[ii].append(new_personal_df_list[ii])

                if now_matchid not in event_already_in:
                    already_in[2] = 0
                    new_events_df_list = events(event)
                    for ii in range(5):
                        new_events_df_list[ii]['gameId'] = now_matchid
                        events_df_list[ii] = events_df_list[ii].append(new_events_df_list[ii])
                        
                if now_matchid not in team_already_in:
                    already_in[3] = 0
                    new_team_df = team(match, new_events_df_list[4])
                    team_df = team_df.append(new_team_df)
                    
                
                
                
            match_df.to_sql('match_id', con=engine, index=False, if_exists='append')

            personal_df_list[0].to_sql('personal_summary', con=engine, index=False, if_exists='append')
            personal_df_list[1].to_sql('personal_rune', con=engine, index=False, if_exists='append')
            personal_df_list[2].to_sql('personal_detail', con=engine, index=False, if_exists='append')

            events_df_list[0].to_sql('event_kill', con=engine, index=False, if_exists='append')
            #events_df_list[1].to_sql('event_ward', con=engine, index=False, if_exists='append')
            events_df_list[1].to_sql('event_item', con=engine, index=False, if_exists='append')
            events_df_list[2].to_sql('event_skillup', con=engine, index=False, if_exists='append')
            events_df_list[3].to_sql('event_building', con=engine, index=False, if_exists='append')
            events_df_list[4].to_sql('event_monster', con=engine, index=False, if_exists='append')   

            team_df.to_sql('team_summary', con=engine, index=False, if_exists='append')
            
            
            match_already_in += gameid_list
            personal_already_in += gameid_list
            event_already_in += gameid_list
            team_already_in += gameid_list


            print(i, 'th database', j, 'th part uploaded!') 
        
        
        match_already_in = list(set(match_already_in))
        personal_already_in = list(set(personal_already_in))
        event_already_in = list(set(event_already_in))
        team_already_in = list(set(team_already_in))
        all_already_in.update(gameid_df)
        
        print(i, 'th database upload completed!\n')
        
        
    conn.close()
    engine.dispose()
    
    
    
if __name__ == "__main__":
    asyncio.run(main())
