import pymysql
from openpyxl import Workbook
from openpyxl import load_workbook
from sqlalchemy import create_engine
import sqlalchemy

import requests
import pandas as pd
import numpy as np
import time


test_db = pymysql.connect(
    user='kokoma', 
    passwd='qkr741963', 
    host='challenger-match-event.cq82nctrk585.ap-northeast-2.rds.amazonaws.com', 
    db='match_data'
)

curs = test_db.cursor(pymysql.cursors.DictCursor)


sql = '''CREATE TABLE match_id(
        gameId bigint NOT NULL,
        season int NOT NULL,
        version int NOT NULL,
        averageTier varchar(255) NOT NULL,
        PRIMARY KEY (gameId)
        )'''

curs.execute(sql)
test_db.commit()


sql = ''' CREATE TABLE personal_summary (
          iIndex bigint NOT NULL AUTO_INCREMENT,
          gameId bigint NOT NULL,
          participantId int NOT NULL,
          teamId varchar(255) NOT NULL,
          summonerName varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
          role varchar(255) NOT NULL,
          duo varchar(255) NOT NULL,
          win varchar(255) NOT NULL,
          champion char(20) NOT NULL,
          level int NOT NULL,
          kills int NOT NULL,
          deaths int NOT NULL,
          assists int NOT NULL,
          spell1 varchar(255) NOT NULL,
          spell2 varchar(255) NOT NULL,
          goldEarned int NOT NULL,
          goldSpent int NOT NULL,
          item0 int NOT NULL,
          item1 int NOT NULL,
          item2 int NOT NULL,
          item3 int NOT NULL,
          item4 int NOT NULL,
          item5 int NOT NULL,
          item6 int NOT NULL,
          PRIMARY KEY (iIndex)
          )'''

curs.execute(sql)
test_db.commit()

sql = ''' CREATE TABLE personal_rune (
          iIndex bigint NOT NULL AUTO_INCREMENT,
          gameId bigint NOT NULL,
          participantId int NOT NULL,
          primaryRuneId varchar(255) NOT NULL,
          primaryRune0 varchar(255) NOT NULL,
          primaryRune1 varchar(255) NOT NULL,
          primaryRune2 varchar(255) NOT NULL,
          primaryRune3 varchar(255) NOT NULL,
          subRuneId varchar(255) NOT NULL,
          subRune0 varchar(255) NOT NULL,
          subRune1 varchar(255) NOT NULL,
          statRune0 varchar(255) NOT NULL,
          statRune1 varchar(255) NOT NULL,
          statRune2 varchar(255) NOT NULL,
          PRIMARY KEY (iIndex)
          )'''

curs.execute(sql)
test_db.commit()

sql = ''' CREATE TABLE personal_detail (
          iIndex bigint NOT NULL AUTO_INCREMENT,
          gameId bigint NOT NULL,
          participantId int NOT NULL,
          sDamageDealt int NOT NULL,
          mDamageDealt int NOT NULL,
          pDamageDealt int NOT NULL,
          tDamageDealt int NOT NULL,
          sDamageTaken int NOT NULL,
          mDamageTaken int NOT NULL,
          pDamageTaken int NOT NULL,
          tDamageTaken int NOT NULL,
          totalHeal int NOT NULL,
          damageSelfMitigated int NOT NULL,
          objectDamage int NOT NULL,
          turretDamage int NOT NULL,
          minionCS int NOT NULL,
          monsterCS int NOT NULL,
          teamMonsterCS int NOT NULL,
          enemyMonsterCS int NOT NULL,
          visionScore int NOT NULL,
          visionWardsBought int NOT NULL,
          wardsPlaced int NOT NULL,
          wardsKilled int NOT NULL,
          turretKills int NOT NULL,
          inhibitorKills int NOT NULL,
          firstBloodKill varchar(255) NOT NULL,
          firstTowerKill varchar(255) NOT NULL,
          PRIMARY KEY (iIndex)
          )'''

curs.execute(sql)
test_db.commit()


sql = ''' CREATE TABLE team_summary (
          iIndex bigint NOT NULL AUTO_INCREMENT,
          gameId bigint NOT NULL,
          teamId varchar(255) NOT NULL,
          win varchar(255) NOT NULL,
          ban1 varchar(255),
          ban2 varchar(255),
          ban3 varchar(255),
          ban4 varchar(255),
          ban5 varchar(255),
          dragon1 varchar(255),
          dragon2 varchar(255),
          dragon3 varchar(255),
          dragon4 varchar(255),
          dragon5 varchar(255),
          dragon6 varchar(255),
          dragon7 varchar(255),
          firstBlood varchar(255),
          firstTower varchar(255),
          firstDragon varchar(255),
          firstBaron varchar(255),
          firstHerald varchar(255),
          firstInhibitor varchar(255),
          totalTower int,
          totalDragon int,
          totalBaron int,
          totalHerald int,
          totalInhibitor int,
          PRIMARY KEY (iIndex)          
          )'''

curs.execute(sql)
test_db.commit()


sql = ''' CREATE TABLE event_kill (
          iIndex bigint NOT NULL AUTO_INCREMENT,
          gameId bigint NOT NULL,
          timestamp int NOT NULL,
          positionX int NOT NULL,
          positionY int NOT NULL,
          killerId int NOT NULL,
          victimId int NOT NULL,
          assistIds varchar(255),
          PRIMARY KEY (iIndex)
          )'''

curs.execute(sql)
test_db.commit()

sql = ''' CREATE TABLE event_ward (
          iIndex bigint NOT NULL AUTO_INCREMENT,
          gameId bigint NOT NULL,
          timestamp int NOT NULL,
          eventType varchar(255) NOT NULL,
          wardType varchar(255) NOT NULL,
          participantId int NOT NULL,
          PRIMARY KEY (iIndex)
          )'''

curs.execute(sql)
test_db.commit()

sql = ''' CREATE TABLE event_item (
          iIndex bigint NOT NULL AUTO_INCREMENT,
          gameId bigint NOT NULL,
          timestamp int NOT NULL,
          eventType varchar(255) NOT NULL,
          participantId int NOT NULL,
          itemId int NOT NULL,
          fromItemId int,
          PRIMARY KEY (iIndex)
          )'''

curs.execute(sql)
test_db.commit()

sql = ''' CREATE TABLE event_skillup (
          iIndex bigint NOT NULL AUTO_INCREMENT,
          gameId bigint NOT NULL,
          timestamp int NOT NULL,
          participantId int NOT NULL,
          skillSlot int NOT NULL,
          levelupType varchar(255) NOT NULL,
          PRIMARY KEY (iIndex)
          )'''

curs.execute(sql)
test_db.commit()

sql = ''' CREATE TABLE event_building (
          iIndex bigint NOT NULL AUTO_INCREMENT,
          gameId bigint NOT NULL,
          timestamp int NOT NULL,
          teamId varchar(255) NOT NULL,
          killerId int NOT NULL,
          assistIds varchar(255),
          buildingType varchar(255) NOT NULL,
          laneType varchar(255) NOT NULL,
          towerType varchar(255) NOT NULL,
          PRIMARY KEY (iIndex)
          )'''

curs.execute(sql)
test_db.commit()

sql = ''' CREATE TABLE event_monster (
          iIndex int NOT NULL AUTO_INCREMENT,
          gameId varchar(255) NOT NULL,
          timestamp int NOT NULL,
          teamId varchar(255) NOT NULL,
          killerId int NOT NULL,
          monsterType varchar(255) NOT NULL,
          dragonSubType varchar(255),
          PRIMARY KEY (iIndex)
          )'''

curs.execute(sql)
test_db.commit()
