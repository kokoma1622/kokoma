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



async def sql_get(sql):
    skill_tuple_list = es_engine.execute(sql).fetchall()
    skillTree_list = ['' for _ in range(10)]

    for skill in skill_tuple_list:
        if skill[2] != 'NORMAL':
            continue
        skillTree_list[skill[0]-1] += slot_dict[skill[1]]
        
    return skillTree_list
     
    
async def sql_update(sql):
    ps_engine.execute(sql)



async def main():

    global ps_engine, es_engine, slot_dict

    ps_engine = create_engine('mysql+pymysql://%s:%s@%s:%d/%s' % (user, passwd, host, port, db),
                       echo=False)
    ps_conn = ps_engine.connect()
    
    es_engine = create_engine('mysql+pymysql://%s:%s@%s:%d/%s' % (user, passwd, host, port, db),
                           echo=False)
    es_conn = es_engine.connect()
    
    
    slot_dict = {1:'Q', 2:'W', 3:'E', 4:'R'}
    
    
    for j in range(1, 17):
        sql = '''select gameid from v10_%02d group by gameid''' % j
        gameid_tuple_list = es_engine.execute(sql).fetchall()
        gameid_list = [gameid_tuple[0] for gameid_tuple in gameid_tuple_list]
        iter_num = (len(gameid_list)-1)//10+1
        
        get_front = """select participantId, skillSlot, levelUpType from v10_%02d where gameid = """ % j
        update_front = """update v10_%02d set skillTree = '""" % j
        
        for i in range(iter_num):
            gameid_sublist = gameid_list[10*i:min(10*(i+1),len(gameid_list))]
            
            get_list = [get_front+str(gameid) for gameid in gameid_sublist]
            skillTree_list = await asyncio.gather(*[sql_get(sqls) for sqls in get_list])
            
            for k in range(10):
                update_back = """' where gameId = %d and participantId = """ % gameid_sublist[k]
                update_list = [update_front+skillTree_list[k][l]+update_back+str(l+1) for l in range(10)]
                await asyncio.gather(*[sql_update(sqls) for sqls in update_list])
            
            if i % 20 == 0:
                print(i)

    
if __name__ == "__main__":
    asyncio.run(main())   
