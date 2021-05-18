import pandas as pd
import urllib
import pyodbc
from sqlalchemy import create_engine

server = "kokoma.database.windows.net"
database = "v11_01"
username = "kokoma"
password = "qkr741*963"
driver = '{ODBC Driver 17 for SQL Server}'

odbc_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database+'_top' + ';PWD='+ password
connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
engine = create_engine(connect_str, echo=False)

for db in db_list:
    for i in range(10):
        engine[db].execute('''create table interval%d_result (keyy int primary key, gameid int, team smallint, win smallint,
        duration smallint, champion smallint, spell1 smallint, spell2 smallint, rune_main smallint, rune_sub smallint, kills smallint, 
        deaths smallint, assists smallint, level smallint, cs smallint, gold int, item0 smallint, item1 smallint, item2 smallint, 
        item3 smallint, item4 smallint, item5 smallint, item6 smallint, kills_total smallint, deaths_total smallint, 
        assists_total smallint)''' % i)
        
        engine[db].execute('''create table interval%d_detail (keyy int primary key, gameid int, team smallint, win smallint, 
        duration smallint, champion smallint, deaths smallint, dmg_dealt_champ int, dmg_dealt_object int, dmg_dealt_tower int, 
        dmg_taken int, dmg_mitigated int, total_heal int, cs_minion smallint, cs_jungle smallint, cs_jungle_team smallint, 
        cs_jungle_enemy  smallint, vision_score smallint, ward_bought smallint, ward_placed smallint, ward_killed smallint)''' % i)
        
        engine[db].execute('''create table interval%d_laning (keyy int primary key, gameid int, team smallint, win smallint, 
        champion smallint, kills smallint, deaths smallint, assists smallint, level smallint, xp smallint, gold smallint, 
        cs_minion smallint, cs_jungle smallint)''' % i)
        
for db in db_list:
    for i in range(10):
        engine[db].execute('''drop table interval%d_laning''' % i)
        engine[db].execute('''create table interval%d_laning (keyy int primary key, gameid int, team smallint, win smallint, 
        duration smallint, champion smallint, kills smallint, deaths smallint, assists smallint, level smallint, xp smallint, 
        gold smallint, cs_minion smallint, cs_jungle smallint)''' % i)
        
for db in db_list:
    engine[db].execute('alter table laning add duration smallint')
    
engine.execute('update laning set duration = a.duration from (select keyy, duration from result) as a where laning.keyy = a.keyy')

interval = [0, 1204, 1333, 1464, 1581, 1685, 1788, 1899, 2033, 2223, 8000]

for i in range(10):
    engine.execute('''insert into interval%d_result select * from result where duration >= %d and duration < %d order by keyy'''
                   % (i, interval[i], interval[i+1]))
    engine.execute('''insert into interval%d_detail select * from detail where duration >= %d and duration < %d order by keyy'''
                   % (i, interval[i], interval[i+1]))
    engine.execute('''insert into interval%d_laning select * from laning where duration >= %d and duration < %d order by keyy'''
                   % (i, interval[i], interval[i+1]))
                   
engine.execute('drop table result')
engine.execute('drop table detail')
engine.execute('drop table laning')

for i in range(10):
    engine.execute('''create table interval%d_data_result_self (keyy int, win smallint, champion smallint, duration smallint,
        contribution real, death_ratio real, gold_pm real, cs_pm real)''' % i)
    engine.execute('''insert into interval%d_data_result_self select keyy, win, champion, duration, 
        (kills+assists)*1.0/(CASE WHEN kills_total > 1 THEN kills_total ELSE 1 END), 
        deaths*1.0/(CASE WHEN deaths_total > 1 THEN deaths_total ELSE 1 END), gold*60.0/duration, cs*60.0/duration
        from interval%d_result''' % (i, i))
    
    engine.execute('''create table interval%d_data_detail_self (keyy int, win smallint, champion smallint, duration smallint,
        dmg_dealt_champ_pm real, dmg_dealt_object_pm real, dmg_dealt_tower_pm real, dmg_taken_pm real, dmg_mitigated_pm real, 
        total_heal_pm real, cs_minion_pm real, cs_jungle_pm real, cs_jungle_team_pm real, cs_jungle_enemy_pm real,
        vision_score_pm real, ward_bought_pm real, ward_placed_pm real, ward_killed_pm real)''' % i)
    engine.execute('''insert into interval%d_data_detail_self select keyy, win, champion, duration,
        dmg_dealt_champ*60.0/duration, dmg_dealt_object*60.0/duration, dmg_dealt_tower*60.0/duration, dmg_taken*60.0/duration, 
        dmg_mitigated*60.0/duration, total_heal*60.0/duration, cs_minion*60.0/duration, cs_jungle*60.0/duration, 
        cs_jungle_team*60.0/duration, cs_jungle_enemy*60.0/duration, vision_score*60.0/duration, ward_bought*60.0/duration, 
        ward_placed*60.0/duration, ward_killed*60.0/duration from interval%d_detail''' % (i, i))
    
    engine.execute('''create table interval%d_data_laning_self (keyy int, win smallint, champion smallint, duration smallint,
        level_14min smallint, xp_14min smallint, gold_14min smallint, cs_minion_14min smallint, 
        cs_jungle_14min smallint, kills_14min smallint, deaths_14min smallint, assists_14min smallint)''' % i)
    engine.execute('''insert into interval%d_data_laning_self select keyy, win, champion, duration,
        level, xp, gold, cs_minion, cs_jungle, kills, deaths, assists from interval%d_laning''' % (i, i))
        
      
for i in range(10):
    if i != 0:
        engine.execute('''create table interval%d_data_result_self_ma (keyy int, win smallint, champion smallint, duration smallint,
            contribution real, death_ratio real, gold_pm real, cs_pm real)''' % i)
        stat = pd.read_sql('''select avg(contribution), stdev(contribution), avg(death_ratio), stdev(death_ratio), 
            avg(gold_pm), stdev(gold_pm), avg(cs_pm), stdev(cs_pm) from interval%d_data_result_self''' % i, con=engine).iloc[0].tolist()
        engine.execute('''insert into interval%d_data_result_self_ma select keyy, win, champion, duration,
            (contribution-%f)*1.0/%f, (death_ratio-%f)*1.0/%f, (gold_pm-%f)*1.0/%f, (cs_pm-%f)*1.0/%f 
            from interval%d_data_result_self''' % (i, stat[0], stat[1], stat[2], stat[3], stat[4], stat[5], stat[6], stat[7], i))

        engine.execute('''create table interval%d_data_detail_self_ma (keyy int, win smallint, champion smallint, duration smallint,
            dmg_dealt_champ_pm real, dmg_dealt_object_pm real, dmg_dealt_tower_pm real, dmg_taken_pm real, dmg_mitigated_pm real, 
            total_heal_pm real, cs_minion_pm real, cs_jungle_pm real, cs_jungle_team_pm real, cs_jungle_enemy_pm real,
            vision_score_pm real, ward_bought_pm real, ward_placed_pm real, ward_killed_pm real)''' % i)
        stat = pd.read_sql('''select avg(dmg_dealt_champ_pm), stdev(dmg_dealt_champ_pm), 
            avg(dmg_dealt_object_pm), stdev(dmg_dealt_object_pm), avg(dmg_dealt_tower_pm), stdev(dmg_dealt_tower_pm), 
            avg(dmg_taken_pm), stdev(dmg_taken_pm), avg(dmg_mitigated_pm), stdev(dmg_mitigated_pm), 
            avg(total_heal_pm), stdev(total_heal_pm), avg(cs_minion_pm), stdev(cs_minion_pm), avg(cs_jungle_pm), stdev(cs_jungle_pm), 
            avg(cs_jungle_team_pm), stdev(cs_jungle_team_pm), avg(cs_jungle_enemy_pm), stdev(cs_jungle_enemy_pm), 
            avg(vision_score_pm), stdev(vision_score_pm), avg(ward_bought_pm), stdev(ward_bought_pm), 
            avg(ward_placed_pm), stdev(ward_placed_pm), avg(ward_killed_pm), stdev(ward_killed_pm) from interval%d_data_detail_self''' 
            % i, con=engine).iloc[0].tolist()
        engine.execute('''insert into interval%d_data_detail_self_ma select keyy, win, champion, duration,
            (dmg_dealt_champ_pm-%f)/%f, (dmg_dealt_object_pm-%f)/%f, (dmg_dealt_tower_pm-%f)/%f, (dmg_taken_pm-%f)/%f, 
            (dmg_mitigated_pm-%f)/%f, (total_heal_pm-%f)/%f, (cs_minion_pm-%f)/%f, (cs_jungle_pm-%f)/%f, (cs_jungle_team_pm-%f)/%f, 
            (cs_jungle_enemy_pm-%f)/%f, (vision_score_pm-%f)/%f, (ward_bought_pm-%f)/%f, (ward_placed_pm-%f)/%f, (ward_killed_pm-%f)/%f
            from interval%d_data_detail_self''' % (i, stat[0], stat[1], stat[2], stat[3], stat[4], stat[5], stat[6], stat[7],
            stat[8], stat[9], stat[10], stat[11], stat[12], stat[13], stat[14], stat[15], stat[16], stat[17], stat[18], stat[19], 
            stat[20], stat[21], stat[2], stat[23], stat[24], stat[25], stat[26], stat[27], i))
    
        engine.execute('''create table interval%d_data_laning_self_ma (keyy int, win smallint, champion smallint, duration smallint,
            level_14min smallint, xp_14min smallint, gold_14min smallint, cs_minion_14min smallint, 
            cs_jungle_14min smallint, kills_14min smallint, deaths_14min smallint, assists_14min smallint)''' % i)
    stat = pd.read_sql('''select avg(cast(level_14min as real)), stdev(level_14min), avg(cast(xp_14min as real)), stdev(xp_14min), 
        avg(cast(gold_14min as real)), stdev(gold_14min), avg(cast(cs_minion_14min as real)), stdev(cs_minion_14min), 
        avg(cast(cs_jungle_14min as real)), stdev(cs_jungle_14min), avg(cast(kills_14min as real)), stdev(kills_14min), 
        avg(cast(deaths_14min as real)), stdev(deaths_14min), avg(cast(assists_14min as real)), stdev(assists_14min)
        from interval%d_data_laning_self''' % i, con=engine).iloc[0].tolist()
    engine.execute('''insert into interval%d_data_laning_self_ma select keyy, win, champion, duration,
        (level_14min-%f)/%f, (xp_14min-%f)/%f, (gold_14min-%f)/%f, (cs_minion_14min-%f)/%f, (cs_jungle_14min-%f)/%f, 
        (kills_14min-%f)/%f, (deaths_14min-%f)/%f, (assists_14min-%f)/%f from interval%d_data_laning_self''' %
        (i, stat[0], stat[1], stat[2], stat[3], stat[4], stat[5], stat[6], stat[7], stat[8], stat[9], stat[10], stat[11],
        stat[12], stat[13], stat[14], stat[15], i))
