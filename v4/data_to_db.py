import sqlalchemy
import psycopg2
import requests
import pandas as pd
import time
import asyncio
import aiohttp
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import BigInteger, Integer, String, Text, TIMESTAMP
from d6tstack.utils import pd_to_psql

from make_tier import get_tier
from make_personal import get_ps, get_pr, get_pd
from make_team import get_ts
from make_minute import get_ms
from make_event import get_events


api_mine =  ''
api_list = []
api_name = []
api_dict = dict(zip(api_name, api_list))



async def quest_df_async(url):
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession() as session:
        try:
            response = await session.get(url, timeout=timeout)
            while True:
                if response.status == 200:
                    text = await response.json()
                    return pd.DataFrame(text)
                elif response.status == 504:
                    await asyncio.sleep(0.2)
                    #print('gateway timeout, request once more!', url)
                    response = await session.get(url)
                else:
                    await asyncio.sleep(30.0)
                    #print('request limit.', url.split('/')[-1])
                    response = await session.get(url)
        except:
            text = request_url(url)
            return pd.DataFrame(text)
        
async def quest_json_async(url):
    timeout = aiohttp.ClientTimeout(total=60)
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
                    await asyncio.sleep(30.0)
                    #print('request limit.', url.split('/')[-1])
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
        elif r.status_code == 504:
            time.sleep(0.2)
        else:
            time.sleep(20)
            print('20 seconds sleep!')



async def main():
    summoner_name = pd.read_csv('lol_csv/apikokoma.csv')['summonerName']
    summoner_series = pd.read_csv('lol_csv/apikokoma.csv')['accountId']
    
    
    already_in = []
    engine = {}
    uri1 = 'postgresql+psycopg2://%s:%s@%s:%d/%s' % (user, passwd, host1, port, db)
    uri2 = 'postgresql+psycopg2://%s:%s@%s:%d/%s' % (user, passwd, host2, port, db)
    
    for i in range(1, 26):
        uri = uri1 + '%02d'%i if i < 13 else uri2 + '%02d'%i
        engine[i] = create_engine(uri,echo=False)
        
        already_in_tuple = engine[i].execute('SELECT gameid FROM game_summary').fetchall()
        already_in += [gameId_tuple[0] for gameId_tuple in already_in_tuple]
     
    print('%d games already in!' % len(already_in))
    
    iter_size = len(api_name)
    
    data_url = 'https://kr.api.riotgames.com/lol/match/v4/matches/'
    time_url = 'https://kr.api.riotgames.com/lol/match/v4/timelines/by-match/'
    summoner_url = 'https://kr.api.riotgames.com/lol/league/v4/entries/by-summoner/'
    
    DB_name = ['game_summary', 'team_summary', 'minute_personal', 'minute_team', 'personal_summary', 'personal_rune', 'personal_detail',
               'event_kill', 'event_building', 'event_monster', 'event_item']
    #377
    for i in range(377,len(summoner_series)):
        matchid_df = pd.DataFrame()
        begin_index = 0
        
        while True:
            match_url = 'https://kr.api.riotgames.com/lol/match/v4/matchlists/by-account/' + summoner_series[i]
            match_url += '?queue=420&beginIndex=' + str(begin_index) + '&api_key=' + api_mine
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

        gameid_df = [gameid for gameid in gameid_df if gameid not in already_in]
        uninserted_game = len(gameid_df)
        announce = 'summoner %s total %d game played, %d games not yet inserted, %d chunks needed to be inserted.' \
                    % (summoner_name[i], total_game, uninserted_game, (uninserted_game-1)//iter_size+1)
        print(announce)
        
        
        toDB = [{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}]
        for j in range(11):
            for k in range(1,26):
                toDB[j][k] = pd.DataFrame()
                
        
        for j in range((uninserted_game-1)//iter_size+1):
        #for j in range(13,uninserted_game//iter_size+1):
            #start = time.time()
            url_list, gameid_list = [], []
            for k in range(iter_size):
                if (j*iter_size+k) >= uninserted_game:
                    break
                url_list.append(data_url+str(gameid_df[j*iter_size+k])+'?api_key='+api_list[k])
                url_list.append(time_url+str(gameid_df[j*iter_size+k])+'?api_key='+api_list[k])
                gameid_list.append(gameid_df[j*iter_size+k])
            json_list = await asyncio.gather(*[quest_json_async(urls) for urls in url_list])
            '''
            summoner_url_list, nickname_list = [], []
            for k in range(len(gameid_list)):
                identity = json_list[2*k]['participantIdentities']
                for ii in range(10):
                    summoner_url_list.append(summoner_url+identity[ii]['player']['summonerId']+'?api_key='+api_list[k])
                    nickname_list.append(identity[ii]['player']['summonerName'])
            summoner_list = await asyncio.gather(*[quest_df_async(urls) for urls in summoner_url_list])
            '''
            
            for k in range(len(gameid_list)):
            #for k in range(38,40):
                print(k)
                gameid = gameid_list[k]
                data = json_list[2*k]
                user, team = pd.DataFrame(data['participants']).sort_values('participantId'), pd.DataFrame(data['teams'])
                stat = pd.DataFrame(dict(user['stats'])).T
                user = user.drop(columns='stats')
                
                time_df = pd.DataFrame(json_list[2*k+1]['frames'])
                frame, event = time_df['participantFrames'], time_df['events']
                
                version = int(data['gameVersion'].split('.')[1])
                if version == 16:
                    if data['gameCreation'] > 1597881600000:
                        version = 17
                '''
                game_dict = {'gameid':gameid, 'season':int(data['gameVersion'].split('.')[0]), 'version':version,
                             'win':'BLUE' if team['win'][0] == 'Win' else 'RED',
                             'tier':get_tier(summoner_list[10*k:10*(k+1)]),
                             'creation':data['gameCreation']//1000, 'duration':data['gameDuration'] }
                new_gs = pd.DataFrame(game_dict, index=[0])
                
                new_ps = get_ps(user, stat, frame[:min(11,len(frame)-1)], nickname_list[10*k:10*(k+1)])
                new_ps['gameid'] = gameid
                '''
                for_rune = ['perkPrimaryStyle', 'perkSubStyle', 'perk0', 'perk1', 'perk2', 'perk3', 'perk4', 'perk5',
                           'statPerk0', 'statPerk1', 'statPerk2']
                for runeId in for_rune:
                    if runeId not in stat:
                        stat[runeId] = 0
                new_pr = get_pr(stat[for_rune].fillna(0))
                new_pr['gameid'] = gameid
                
                '''
                for_detail = ['totalDamageDealtToChampions','magicDamageDealtToChampions','physicalDamageDealtToChampions', 
                              'trueDamageDealtToChampions','damageDealtToObjectives','damageDealtToTurrets', 
                              'totalDamageTaken','magicalDamageTaken','physicalDamageTaken','trueDamageTaken','totalHeal','damageSelfMitigated',
                              'totalMinionsKilled','neutralMinionsKilled','neutralMinionsKilledTeamJungle','neutralMinionsKilledEnemyJungle',
                              'visionScore','visionWardsBoughtInGame','wardsPlaced','wardsKilled','firstBloodKill','firstTowerKill']
                if 'firstBloodKill' not in stat:
                    stat['firstBloodKill'] = 0
                if 'firstTowerKill' not in stat:
                    stat['firstTowerKill'] = 0
                new_pd = get_pd(stat[for_detail])
                new_pd['gameid'] = gameid
                '''
                #champ_list = new_ps['champion'].tolist()
                champ_list = user['championId'].tolist()
                monster_info = team[['dragonKills', 'baronKills', 'riftHeraldKills']]
                rune_list = stat[['perk0','perk1','perk2','perk3','perk4','perk5']].values.tolist()
                rune_item = [[1 if 8316 in user_rune else 0 for user_rune in rune_list]]       # minion
                rune_item.append([1 if 8313 in user_rune else 0 for user_rune in rune_list])   # stopwatch
                rune_item.append([1 if 8345 in user_rune else 0 for user_rune in rune_list])   # biscuit
                rune_item.append([1 if 8304 in user_rune else 0 for user_rune in rune_list])   # shoes
                
                new_event, kda, items, skilltree = get_events(event, champ_list, monster_info, rune_item)
                for ii in range(4):
                    new_event[ii]['gameid'] = gameid
                #new_ps['skilltree'] = skilltree    
                '''
                new_mp, new_mt = get_ms(frame, kda, items, new_event[1], new_event[2])
                new_mp['gameid'] = gameid
                new_mp.loc[new_mp.index[-1],'items'] = [new_ps['items'].tolist()]
                new_mt['gameid'] = gameid
                
                kills = new_ps['kills'].tolist()
                new_ts = get_ts(team, champ_list, kills, new_event[2])
                new_ts['gameid'] = gameid
                
                  
                toDB[0][version] = toDB[0][version].append(new_gs, ignore_index=True)
                toDB[1][version] = toDB[1][version].append(new_ts, ignore_index=True)
                toDB[2][version] = toDB[2][version].append(new_mp, ignore_index=True)
                toDB[3][version] = toDB[3][version].append(new_mt, ignore_index=True)
                toDB[4][version] = toDB[4][version].append(new_ps, ignore_index=True)
                toDB[5][version] = toDB[5][version].append(new_pr, ignore_index=True)
                toDB[6][version] = toDB[6][version].append(new_pd, ignore_index=True)
                toDB[7][version] = toDB[7][version].append(new_event[0], ignore_index=True)
                toDB[8][version] = toDB[8][version].append(new_event[1], ignore_index=True)
                toDB[9][version] = toDB[9][version].append(new_event[2], ignore_index=True)
                toDB[10][version] = toDB[10][version].append(new_event[3], ignore_index=True)
                '''
        return    
            #print(i, 'th summoner %2d th chunk updated. %5.2f seconds spent.' % (j+1, time.time() - start))
            #start = time.time()
        '''
        return
        if uninserted_game == 0:
            print()
            continue
            
        print(i, 'th summoner database upload started.')
        
        len_DB, time_DB = [0 for _ in range(25)], [0 for _ in range(25)]
        for j in range(1, 26):
            if len(toDB[0][j]) == 0:
                continue
            
            start=time.time()
            for ii in range(len(toDB)):
                DB_len = len(toDB[ii][j])
                for k in range((DB_len-1)//500+1):
                    end = min(500*(k+1), DB_len)
                    toDB[ii][j].iloc[500*k:end].to_sql(DB_name[ii], con=engine[j], index=False, if_exists='append', method='multi')
            len_DB[j-1] = len(toDB[0][j])
            time_DB[j-1] = round(time.time()-start,2)
        print(len_DB)
        print(time_DB)
        '''
        print(i, 'th summoner database upload finished!\n')

        
        already_in += gameid_df
        
        #if time.time()-start > 34000: return
        
    
    
if __name__ == "__main__":
    asyncio.run(main())
