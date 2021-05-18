import requests
import asyncio
import aiohttp
import time
import gc
import numpy as np


api_list = [
    '', '', '',
    '', '', '',
    '', '', '',
    '', '', '',
    '', '', '',
    '', '', '',
    '', '', '',
    '', '', '',
    '', '', '',
    '', '', '',
    '', '', '',
    '', '', '',
    '', '', ''
    ]


async def get_data_async(url):
    timeout = aiohttp.ClientTimeout(total=20)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            response = await session.get(url)
            while True:
                if response.status == 200:
                    text = await response.json()
                    await asyncio.sleep(0.02)
                    return text
                elif response.status == 504:
                    await asyncio.sleep(0.02)
                    #print('gateway timeout, request once more!', url)
                    response = await session.get(url)
                else:
                    await asyncio.sleep(5.0)
                    #print('request limit.', url.split('/')[-1])
                    response = await session.get(url)
    except:
        text = get_data(url)
        await asyncio.sleep(0.02)
        return text

        
def get_data(url):
    while True:
        r = requests.get(url)

        if r.status_code == 200:
            return r.json()
        elif r.status_code == 403: # api 갱신 필요
            print('you need api renewal')
            print(url)
            return -1
        else:
            #print(r.status_code, '20 seconds sleep!')
            time.sleep(1)        

    
    
def is_where(coordinate):
    where = 'JUNGLE'
    difference = coordinate['x'] - coordinate['y']
    if coordinate['x'] < 3000 and coordinate['y'] < 3000:     where = 'BLUE HOME'
    elif coordinate['x'] > 12000 and coordinate['y'] > 12000: where = 'RED HOME'
    elif coordinate['y'] > 12000 or coordinate['x'] < 3000:   where = 'TOP'
    elif coordinate['x'] > 12000 or coordinate['y'] < 3000:   where = 'BOTTOM'
    elif -1500 < difference < 1500:                           where = 'MID'
        
    return where


def position_check(user, frame, target):
    spells_list = [[user[i]['spell1Id'], user[i]['spell2Id']] for i in range(10)]    
    items_list = [[user[i]['stats']['item0'], user[i]['stats']['item1'], user[i]['stats']['item2'], user[i]['stats']['item3'], 
                   user[i]['stats']['item4'], user[i]['stats']['item5'], user[i]['stats']['item6']] for i in range(10)]
    
    position = [-1 for _ in range(10)]
    
    blue_is_smite = [row.count(11) for row in spells_list[:5]]
    red_is_smite = [row.count(11) for row in spells_list[5:]]
    if sum(blue_is_smite) != 1 or sum(red_is_smite) != 1:
        return -1
    position[1] = blue_is_smite.index(1)
    position[6] = red_is_smite.index(1)+5
    
    support_item = [3850, 3851, 3853, 3862, 3863, 3864, 3858, 3859, 3860, 3854, 3855, 3857]
    blue_is_support = [sum(item in support_item for item in items) for items in items_list[:5]]
    red_is_support = [sum(item in support_item for item in items) for items in items_list[5:]]
    if sum(blue_is_support) != 1 or sum(red_is_support) != 1:
        return -1
    position[4] = blue_is_support.index(1)
    position[9] = red_is_support.index(1)+5
    
    
    position_determined = [position[i] for i in range(10) if position[i] != -1]
    
    for j in range(2):
        position_left = ['TOP', 'MID', 'BOTTOM']
        position_index = {'TOP': j*5, 'MID': j*5+2, 'BOTTOM':j*5+3}
        for i in range(5):
            userid = frame[0]['participantFrames'][str(j*5+i+1)]['participantId'] - 1
            if userid in position_determined:
                continue
                
            where_list = [is_where(frame[minute]['participantFrames'][str(j*5+i+1)]['position']) for minute in range(1, 15)]
            where = max(set(where_list), key = where_list.count)
            if where not in position_left:
                return -1
            
            position[position_index[where]] = userid
            position_left.remove(where)
    
    
    f_to_u = [frame[0]['participantFrames'][str(i+1)]['participantId']-1 for i in range(10)]
    u_to_f = [str(f_to_u.index(i)+1) for i in range(10)]
    p_to_f = [u_to_f[position[i]] for i in range(10)]
    
    return position[target], p_to_f[target]


def data_to_check(data_dict, time_dict, db_index, team):
    target = team*5 + db_index
    
    gameid = (data_dict['gameId'] - 4900000000) * 2 + team
    user = data_dict['participants']
    try:
        u_target, f_target = position_check(user, time_dict, target)
    except:
        return -1
    
    minute_dict = [time_dict[minute]['participantFrames'][f_target] for minute in range(len(time_dict))]
    minute_list = [[gameid, minute, minute_dict[minute]['xp'], minute_dict[minute]['totalGold'], 
                    minute_dict[minute]['minionsKilled']+minute_dict[minute]['jungleMinionsKilled']] 
                   for minute in range(len(minute_dict))]
    
    death_list = []
    for minute in range(len(time_dict)):
        event_dict = time_dict[minute]['events']
        for event in event_dict:
            if event['type'] == 'CHAMPION_KILL':
                if event['victimId'] == u_target + 1:
                    death_list.append([gameid, event['timestamp']])
                
    return minute_list[2:], death_list
    

    
async def main():
    #db_list = ['top', 'jgl', 'mid', 'bot', 'sup']
    db_list = ['sup']
    step = len(api_list)
    data_url = 'https://kr.api.riotgames.com/lol/match/v4/matches/'
    time_url = 'https://kr.api.riotgames.com/lol/match/v4/timelines/by-match/'
    
    for db in db_list:
        low_gameid = np.load('low_gameid/%s_low_gameid.npy' % (db))
        db_index = db_list.index(db)
        
        minute, death = [], []
        total_step = len(low_gameid)//(step*5)
        for i in range(630, total_step):
            gameid_list = low_gameid[i*step*5:(i+1)*step*5, 0].tolist()
            team_list = low_gameid[i*step*5:(i+1)*step*5, 1].tolist()
            
            url_for_data = [data_url+str(gameid_list[i])+'?api_key='+api_list[i % step] for i in range(len(gameid_list))]
            url_for_time = [time_url+str(gameid_list[i])+'?api_key='+api_list[i % step] for i in range(len(gameid_list))]
            start_time = time.time()
            game_data_list = await asyncio.gather(*[get_data_async(urls) for urls in url_for_data])
            game_time_list = await asyncio.gather(*[get_data_async(urls) for urls in url_for_time])
            print(time.time() - start_time)
            
            start_time = time.time()
            for j in range(len(gameid_list)):
                try:
                    minute_list, death_list = data_to_check(game_data_list[j], game_time_list[j]['frames'], db_index, team_list[j])
                    minute += minute_list
                    death += death_list
                except:
                    continue
            print(time.time() - start_time)
            
            print('%d / %d steps done!' % (i+1, total_step))
            
            time.sleep(11)
            
            if (i+1) % (total_step//5) == 0:
                np.save('troll/%s_minute_%s' % (db, (i+1) // (total_step//5)), np.array(minute, dtype='uint32'))
                np.save('troll/%s_death_%s' % (db, (i+1) // (total_step//5)), np.array(death, dtype='uint32'))
                
                minute, death = [], []
                
                print(db, (i+1) // (total_step//5) + 1, 'th troll saved.')

            gc.collect()


    return


if __name__ == "__main__":
    asyncio.run(main())
    
    
"""            
문제 : 타라인에 가서 계속 있는 사람
    이거 구분이 되나
    지금 시스템으론 2탑이 되거나 딴 사람이 피하면 그 사람이 그 라인으로 판단됨
    라인 판별 시스템을 바꿔야하나 하기엔 정보가 부족. 분 단위의 위치 정보 뿐
    합의 하에 바꾼 가능성도 있는데 이를 위치 데이터로만 판단하기엔 힘들다
    
    갑자기 정글, 서폿이 라인을 먹는다 is not always 트롤. 라이너가 탈주했을 가능성
    2강타 역시 not always 트롤. 전략적 2강타가 가능하긴 함
    
    근데 RNN을 만들어보긴 하는게 좋을거같다.
    그러면 death log랑 xp/gold log를 일단 뽑아서 러닝을 해보자.
    
    time step이 변하는 rnn은 어케하나 이거 찾기
    
    db에 저장을 해야하나? 당연 -> 무엇을 저장해아하나
    death log
    죽은 시간, 죽은 상대, 죽은 위치
    이거의 문제 : time step가 일정하지 않다. 시계열이지않아.
    
    xp gold log
    분, 경험치, 골드
"""
