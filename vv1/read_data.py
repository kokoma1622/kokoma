import sqlalchemy
import pyodbc
import urllib
import requests
import asyncio
import aiohttp
import gc
import time
import pandas as pd
from sqlalchemy import create_engine
from copy import deepcopy

from get_match_data import get_match



api_list = []



async def check_gameid_valid_async(url):
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            response = await session.get(url)
            while True:
                if response.status == 200:
                    text = await response.json()
                    if text['queueId'] == 420:
                        if text['gameCreation'] < 1611162000000:
                            if text['gameDuration'] > 900:
                                await asyncio.sleep(0.02)
                                return [text['gameId'], text]
                    await asyncio.sleep(0.02)
                    return -1
                elif response.status == 404:
                    await asyncio.sleep(0.02)
                    return -1
                elif response.status == 504:
                    await asyncio.sleep(0.02)
                    #print('gateway timeout, request once more!', url)
                    response = await session.get(url)
                else:
                    #print(url, response.status)
                    await asyncio.sleep(30.0)
                    #print('request limit.', url.split('/')[-1])
                    response = await session.get(url)
        except:
            text = check_gameid_valid(url)
            await asyncio.sleep(0.01)
            return text
        

def check_gameid_valid(url):
    while True:
        r = requests.get(url)

        if r.status_code == 200:
            text = r.json()
            if text['queueId'] == 420:
                if text['gameCreation'] < 1611162000000:
                    if text['gameDuration'] > 900:
                        return [text['gameId'], text]
            return -1
        elif r.status_code == 403: # api 갱신 필요
            print('you need api renewal')
            print(url)
            return -1
        else:
            #print(url)
            #print(r.status_code, '20 seconds sleep!')
            time.sleep(0.2)


        
async def get_data_async(url):
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
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
                    await asyncio.sleep(30.0)
                    #print('request limit.', url.split('/')[-1])
                    response = await session.get(url)
        except:
            text = get_data(url)
            await asyncio.sleep(0.01)
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
            time.sleep(0.2)        


'''
def get_result(game_data_list, game_time_list, process_num, result_list):
    print(process_num)
    for i in range(process_num*len(api_list), (process_num+1)*len(api_list)):
        result = get_match(game_data_list[i], game_time_list[i]['frames'])
        if result != -1:
            for j in range(5):
                result_list[0][j] += result[1][j]
                result_list[1][j] += result[2][j]
                result_list[2][j] += result[3][j]
            
            result_list[3] += result[0]
    print(process_num)
    
    return
'''


            
async def main():
    #gameid_start = 4905010000
    gameid_start = 4934858849
    gameid_end = 4935000000
    
    #gameid_start = 4816000010
    #gameid_end = 4816000050
    
    game_in = 0
    
    step = len(api_list)
    data_url = 'https://kr.api.riotgames.com/lol/match/v4/matches/'
    time_url = 'https://kr.api.riotgames.com/lol/match/v4/timelines/by-match/'
    
    DB_name = ['_top', '_jgl', '_mid', '_bot', '_sup']
    table_name = ['result', 'detail', 'laning', 'minute']
    

    engine = []
    
    server = ""
    database = ""
    username = ""
    password = ""
    driver = ''

    for i in range(5):
        odbc_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database+DB_name[i] + ';PWD='+ password
        connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
        engine.append(create_engine(connect_str, echo=False, fast_executemany=True))
        
    odbc_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + ';PWD='+ password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
    engine.append(create_engine(connect_str, echo=False, fast_executemany=True))
    
    
    gameid_valid = []
    game_data_list = []

    
    try:
        gameid_valid_not_exist = 0
        gameid_checkpoint = gameid_start
        while gameid_checkpoint < gameid_end:
            url_for_check_valid = [data_url+str(gameid_checkpoint+i)+'?api_key='+api_list[i % step] for i in range(step * 5)]
            data_check_valid = await asyncio.gather(*[check_gameid_valid_async(urls) for urls in url_for_check_valid])

            gameid_valid_sub = [data[0] for data in data_check_valid if data != -1]
            if len(gameid_valid_sub) == 0:
                if gameid_valid_not_exist == 4:
                    gameid_passpoint = gameid_checkpoint + step * 25
                    while True:
                        url_for_check_pass = [data_url+str(gameid_passpoint+i)+'?api_key='+api_list[i % step] for i in range(step * 2)]
                        data_check_pass = await asyncio.gather(*[check_gameid_valid_async(urls) for urls in url_for_check_pass])
                        gameid_pass_valid = [data[0] for data in data_check_pass if data != -1]
                        print('pass check in', gameid_passpoint)

                        if len(gameid_pass_valid) > 0:
                            gameid_checkpoint = gameid_passpoint - step * 25
                            print('from', gameid_checkpoint, 'check valid restart.')
                            break

                        gameid_passpoint += step * 25

                    gameid_valid_not_exist = 0    
                    continue

                else:
                    gameid_valid_not_exist += 1

            gameid_valid += gameid_valid_sub
            game_data_list += [data[1] for data in data_check_valid if data != -1]

            if len(gameid_valid) > step * 6:
                url_for_time = [time_url+str(gameid_valid[i])+'?api_key='+api_list[i % step] for i in range(step * 6)]
                game_time_list = await asyncio.gather(*[get_data_async(urls) for urls in url_for_time])


                print('data making started')

                summary_list = []
                result_list = [[] for _ in range(10)]
                detail_list = [[] for _ in range(10)]
                laning_list = [[] for _ in range(10)]
                #minute_list = [[] for _ in range(10)]

                
                for i in range(step * 6):
                    result = get_match(game_data_list[i], game_time_list[i]['frames'])

                    if result != -1:
                        summary_list += result[0]
                        for j in range(5):
                            result_list[j] += result[1][j]
                            detail_list[j] += result[2][j]
                            laning_list[j] += result[3][j]
                            #minute_list[j] += result[4][j]


                to_sql_list = [result_list, detail_list, laning_list]
                #to_sql_list = [result_list, detail_list, laning_list, minute_list]

                print('to', gameid_valid[step * 6], 'insertion started.')

                for i in range(5):
                    for j in range(3):
                    #for j in range(4):
                        pd.DataFrame(to_sql_list[j][i]).to_sql(table_name[j], con=engine[i], index=False, if_exists='append')
                        '''
                        print(pd.DataFrame(list(result_list[0][0])).sort_values(by=['keyy']))
                        print(pd.DataFrame(list(result_list[0][1])).sort_values(by=['keyy']))
                        return
                        pd.DataFrame(list(result_list[j][i])).sort_values(by=['keyy']).to_sql(table_name[j], 
                                                                                        con=engine[i], index=False, if_exists='append')
                        '''

                #pd.DataFrame(result_list[3]).sort_values(by=['keyy']).to_sql('summary', con=engine[5], index=False, if_exists='append')
                pd.DataFrame(summary_list).to_sql('summary', con=engine[5], index=False, if_exists='append')

                print('to', gameid_valid[step * 6], 'insertion finished')

                game_in += int(len(summary_list)/2)
                gameid_valid = gameid_valid[step * 6:]
                game_data_list = game_data_list[step * 6:]
                
                gc.collect()


            gameid_checkpoint += step * 5
            print(gameid_checkpoint, game_in)
            
    
    except Exception as ex:
        print(gameid_valid)
        print(ex)
        
            
if __name__ == "__main__":
    asyncio.run(main())
