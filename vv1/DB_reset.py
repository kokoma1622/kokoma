import pandas as pd
import urllib
import pyodbc
from sqlalchemy import create_engine

DB_name = ['_top', '_jgl', '_mid', '_bot', '_sup']
table_name = ['result', 'detail', 'laning']

engine = []
server = "kokoma.database.windows.net"
database = "v11_01"
username = "kokoma"
password = "qkr741*963"
driver = '{ODBC Driver 17 for SQL Server}'

for i in range(5):
    odbc_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database+DB_name[i] + ';PWD='+ password
    connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
    engine.append(create_engine(connect_str, echo=False))

odbc_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + ';PWD='+ password
connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
engine.append(create_engine(connect_str, echo=False))


summary = """create table summary(
             keyy int primary key,
             gameid int, team smallint, win smallint, creation int, duration smallint, 
             top_p smallint, jgl_p smallint, mid_p smallint, bot_p smallint, sup_p smallint,
             top_b smallint, jgl_b smallint, mid_b smallint, bot_b smallint, sup_b smallint)"""
engine[5].execute(summary)

for i in range(5):
    result = """create table result(
                keyy int primary key,
                gameid int, team smallint, win smallint, duration smallint, champion smallint, spell1 smallint, spell2 smallint,
                rune_main smallint, rune_sub smallint, kills smallint, deaths smallint, assists smallint, level smallint, 
                cs smallint, gold int, item0 smallint, item1 smallint, item2 smallint, item3 smallint, item4 smallint, 
                item5 smallint, item6 smallint)"""
    detail = """create table detail(
                keyy int primary key,
                gameid int, team smallint, win smallint, duration smallint, champion smallint, deaths smallint, 
                dmg_dealt_champ int, dmg_dealt_object int, dmg_dealt_tower int, dmg_taken int, total_heal int, dmg_mitigated int, 
                cs_minion smallint, cs_jungle smallint, cs_jungle_team smallint, cs_jungle_enemy smallint, 
                vision_score smallint, ward_bought smallint, ward_placed smallint, ward_killed smallint)"""
    laning = """create table laning (
                keyy int primary key,
                gameid int, team smallint, win smallint, champion smallint, 
                kills smallint, deaths smallint, assists smallint, level smallint, xp smallint, gold smallint, 
                cs_minion smallint, cs_jungle smallint)"""
    
    sql_list = [result, detail, laning]
    
    for sql in sql_list:
        engine[i].execute(sql)
