"""
This program extracts fifa_soccer data from the database database-1
The aim is to set up the Data Warehouse model Data Vault
It consists of 3 hubs, 1 link, and 6 satellites
The data are loded from the database into the DWH Data Vault.
The processes are executed on the AWS Server
Used programs are Python-3.6 and Posgres SQL
Input: database-1
Output: Data Vault filled with soccer data
Modules: fifa_dwh_lib.py
"""

import numpy as np
import pandas as pd
import datetime
from string import ascii_letters

import sqlalchemy
import psycopg2 # läuft im Hintergrund und ermöglicht den Datenverkehr
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, inspect
import hashlib
import sqlite3

import fifa_dwh_lib
import importlib # reload module with importlib.reload(fifa_dwh_lib)

# connect to sql server
sql_connect = 'postgres://postgres:topsecret00@database-1.clu9fkojue9l.eu-central-1.rds.amazonaws.com'
db = create_engine(sql_connect, echo=True)


# -----------------------------------------------------------------------------
# Extraction of date from the SQL Database
# -----------------------------------------------------------------------------

# path definition
directory = '/home/ec2-user/'
path = directory+'database01.sqlite'
# load database
df = sqlite3.connect(path)
# extract tables
country = pd.read_sql_query("SELECT * FROM country",df)
league = pd.read_sql_query("SELECT * FROM league",df)
match = pd.read_sql_query("SELECT * FROM match",df)
player = pd.read_sql_query("SELECT * FROM player",df)
player_attributes = pd.read_sql_query("SELECT * FROM player_attributes",df)
team = pd.read_sql_query("SELECT * FROM team",df)
team_attributes = pd.read_sql_query("SELECT * FROM team_attributes",df)


# -----------------------------------------------------------------------------
# Setting up the Data Vault - 3 hubs, 6 satellites, 1 link 
# -----------------------------------------------------------------------------

# drop tables if necessary
db.execute("drop table if exists hub_team cascade")
db.execute("drop table if exists sat_team cascade")
db.execute("drop table if exists sat_team_att cascade")
db.execute("drop table if exists hub_player cascade")
db.execute("drop table if exists sat_player cascade")
db.execute("drop table if exists sat_player_att cascade")
db.execute("drop table if exists hub_league cascade")
db.execute("drop table if exists sat_league cascade")
db.execute("drop table if exists link_match cascade")
db.execute("drop table if exists sat_match cascade")

# create 3 hubs: hub_league, hub_player, hub_match
db.execute(fifa_dwh_lib.create_hub('hub_league','hash_league'))
db.execute(fifa_dwh_lib.create_hub('hub_player','hash_player'))
db.execute(fifa_dwh_lib.create_hub('hub_team','hash_team'))

# create 1 link: link_match
db.execute(fifa_dwh_lib.create_link_match())

# create 6 satellites
db.execute(fifa_dwh_lib.create_sat_league())
db.execute(fifa_dwh_lib.create_sat_match())
db.execute(fifa_dwh_lib.create_sat_player())
db.execute(fifa_dwh_lib.create_sat_player_att())
db.execute(fifa_dwh_lib.create_sat_team())
db.execute(fifa_dwh_lib.create_sat_team_att())


# -----------------------------------------------------------------------------
# insert data sets into the hubs
# -----------------------------------------------------------------------------

# hub_player
for i in range(len(player)):
    datenow = datetime.datetime.now()
    datefmt = datenow.strftime("%Y-%m-%d %H:%M:%S")
    record = 'database01.sqlite'
    bk = player.player_api_id[i]
    hk = fifa_dwh_lib.get_hash(bk,record)
    db.execute(fifa_dwh_lib.insert_hub('hub_player', hk, bk, datefmt, record))

# an entry for missing information of home- and away-players in the table link_match
datenow = datetime.datetime.now()
datefmt = datenow.strftime("%Y-%m-%d %H:%M:%S")
record = 'database01.sqlite'
bk = -1
hk = fifa_dwh_lib.get_hash(bk,record)
db.execute(fifa_dwh_lib.insert_hub('hub_player', hk, bk, datefmt, record))

# hub_team
for i in range(len(team)):
    datenow = datetime.datetime.now()
    datefmt = datenow.strftime("%Y-%m-%d %H:%M:%S")
    record = 'database01.sqlite'
    bk = team.team_api_id[i]
    hk = fifa_dwh_lib.get_hash(bk,record)
    db.execute(fifa_dwh_lib.insert_hub('hub_team', hk, bk, datefmt, record))

# hub_league
for i in range(len(league)):
    datenow = datetime.datetime.now()
    datefmt = datenow.strftime("%Y-%m-%d %H:%M:%S")
    record = 'database01.sqlite'
    bk = league.country_id[i]
    hk = fifa_dwh_lib.get_hash(bk,record)
    db.execute(fifa_dwh_lib.insert_hub('hub_league', hk, bk, datefmt, record))


# -----------------------------------------------------------------------------
# insert data into the satellites of the hub hub_team
# ------------------------------------------------------------------------------

# # filling data into table sat_team with attributes team_long_name, team_short_name, and team_fifa_api_id
for i in range(len(team)):
    bk = team.team_api_id[i]     
    datenow = datetime.datetime.now()
    datefmt = datenow.strftime("%Y-%m-%d %H:%M:%S")
    dateend = '9999-12-31 23:59:59'
    record = 'database01.sqlite'
    a = team.team_fifa_api_id[i]
    b = np.str(team.team_long_name[i]).replace("'"," ")
    c = team.team_short_name[i]
    hk = fifa_dwh_lib.get_hash(bk,record)
    hd = fifa_dwh_lib.get_hash(a,b,c)
    fmt="insert into sat_team values ("+7*"'{:}',"+"'{:}')"
    db.execute(fmt.format(hk, datefmt, dateend, record, hd, a, b, c))

# data cleaning: filling missing Values with -1 
ta = team_attributes.drop(columns=['id','team_api_id']).copy()
ta.buildUpPlayDribbling.fillna(-1,inplace=True)
# filling data into table sat_team_att
for i in range(len(team_attributes)):
    bk = team_attributes.team_api_id[i]
    datenow = datetime.datetime.now()
    datefmt = datenow.strftime("%Y-%m-%d %H:%M:%S.%f")
    dateend = '9999-12-31 23:59:59'
    record = 'database01.sqlite'
    hk = fifa_dwh_lib.get_hash(bk,record)
    hd = fifa_dwh_lib.get_hash(ta.iloc[i,:])
    fmt = "insert into sat_team_att values ("+27*"'{:}',"+"'{:}')"
    db.execute(fmt.format( hk, datefmt, dateend, record, hd, \
       ta.team_fifa_api_id[i], ta.date[i], ta.buildUpPlaySpeed[i], ta.buildUpPlaySpeedClass[i], \
       ta.buildUpPlayDribbling[i], ta.buildUpPlayDribblingClass[i], ta.buildUpPlayPassing[i], \
       ta.buildUpPlayPassingClass[i], ta.buildUpPlayPositioningClass[i], ta.chanceCreationPassing[i], \
       ta.chanceCreationPassingClass[i], ta.chanceCreationCrossing[i], ta.chanceCreationCrossingClass[i], \
       ta.chanceCreationShooting[i], ta.chanceCreationShootingClass[i], ta.chanceCreationPositioningClass[i], \
       ta.defencePressure[i], ta.defencePressureClass[i], ta.defenceAggression[i], \
       ta.defenceAggressionClass[i], ta.defenceTeamWidth[i], ta.defenceTeamWidthClass[i], \
       ta.defenceDefenderLineClass[i] ))

# -----------------------------------------------------------------------------
# insert data into the satellites of the hub hub_league
# ------------------------------------------------------------------------------

# filling data into table sat_league with attributes country_id, country_name, and league_name
for i in range(len(league)):
    bk = league.id[i]     
    datenow = datetime.datetime.now()
    datefmt = datenow.strftime("%Y-%m-%d %H:%M:%S")
    dateend = '9999-12-31 23:59:59'
    record = 'database01.sqlite'
    a = league.country_id[i]
    b = country.name.iloc[i] 
    c = league.name.iloc[i]
    hk = fifa_dwh_lib.get_hash(bk,record)
    hd = fifa_dwh_lib.get_hash(a,b,c)
    fmt="insert into sat_league values ("+7*"'{:}',"+"'{:}')"
    db.execute(fmt.format(hk, datefmt, dateend, record, hd, a, b, c))

# -----------------------------------------------------------------------------
# insert data into the satellites of the hub hub_player
# ------------------------------------------------------------------------------

# filling data into table sat_player with attributes player_name, player_fifa_api_id, birthday, weight, and height 
for i in range(len(player)):
    bk = player.player_api_id
    datenow = datetime.datetime.now()
    datefmt = datenow.strftime("%Y-%m-%d %H:%M:%S")
    dateend = '9999-12-31 23:59:59'
    record = 'database01.sqlite'
    a = str(player.player_name[i]).replace("'"," ")
    b = player.player_fifa_api_id[i]
    c = player.birthday[i]
    d = player.height[i]
    e = player.weight[i]
    bk = player.player_api_id[i]
    hk = fifa_dwh_lib.get_hash(bk,record)
    hd = fifa_dwh_lib.get_hash(a,b,c,d,e)
    fmt="insert into sat_player values ("+9*"'{:}',"+"'{:}')"
    db.execute(fmt.format(hk, datefmt, dateend, record, hd, a, b, c, d, e))


# player aattributes - data cleaning

# devide sat_player_att into a numeric and a non-numeric part
pan = player_attributes._get_numeric_data().copy()
pan.drop(columns=['id','player_api_id'],inplace=True)
pao = player_attributes.select_dtypes('object').copy()

# filling missing values for numeric and non-numeric columns
pan.fillna(-1,inplace=True) # numeric missing values are set to -1
pao[pao.isna()] = 'NO VALUE' # missing values of objects are set to 'NO VALUE'

# filling data into table sat_player att
for i in range(len(player_attributes)):
    bk = player_attributes.player_api_id[i]
    datenow = datetime.datetime.now()
    datefmt = datenow.strftime("%Y-%m-%d %H:%M:%S.%f")
    dateend = '9999-12-31 23:59:59'
    record = 'database01.sqlite'
    hk = fifa_dwh_lib.get_hash(bk,record)
    hd = fifa_dwh_lib.get_hash(pao.iloc[i,:],pan.iloc[i,:])
    fmt = "insert into sat_player_att values ("+43*"'{:}',"+"'{:}')"
    db.execute(fmt.format( hk, datefmt, dateend, record, hd, \
       pan.player_fifa_api_id[i], pao.date[i], pan.overall_rating[i], pan.potential[i], \
       pao.preferred_foot[i], pao.attacking_work_rate[i], pao.defensive_work_rate[i] , \
       pan.crossing[i], pan.finishing[i], pan.heading_accuracy[i],pan.short_passing[i], \
       pan.volleys[i],pan.dribbling[i], pan.curve[i], pan.free_kick_accuracy[i], pan.long_passing[i], \
       pan.ball_control[i], pan.acceleration[i], pan.sprint_speed[i], pan.agility[i], \
       pan.reactions[i], pan.balance[i], pan.shot_power[i], pan.jumping[i], pan.stamina[i], \
       pan.strength[i], pan.long_shots[i], pan.aggression[i], pan.interceptions[i], pan.positioning[i], \
       pan.vision[i], pan.penalties[i], pan.marking[i], pan.standing_tackle[i], pan.sliding_tackle[i], \
       pan.gk_diving[i], pan.gk_handling[i], pan.gk_kicking[i], pan.gk_positioning[i], pan.gk_reflexes[i] ))


# -----------------------------------------------------------------------------
# insert values into the link link_match and its satellite sat_match
# ------------------------------------------------------------------------------

# select columns that are nedded for filling the next tables
match2 = match.iloc[:,:85].copy() # quotes are not needed

# devide match_player_2 into a numeric and a non-numeric part
matchn = match2._get_numeric_data().copy()
matcho = match2.select_dtypes('object').copy()

# filling missing values for numeric and non-numeric columns
matchn.fillna(-1,inplace=True) # numeric attributes
matcho[matcho.isna()] = 'NO VALUE' # non-numeric attributes

# link_match
for i in range(len(matchn)):
    datenow = datetime.datetime.now()
    datefmt = datenow.strftime("%Y-%m-%d %H:%M:%S")
    record = 'database01.sqlite'
    hk   = fifa_dwh_lib.get_hash(matchn.match_api_id.iloc[i],record)
    lg   = fifa_dwh_lib.get_hash(matchn.league_id.iloc[i],record)
    ht   = fifa_dwh_lib.get_hash(matchn.home_team_api_id.iloc[i],record)
    at   = fifa_dwh_lib.get_hash(matchn.away_team_api_id.iloc[i],record)
    hp1  = fifa_dwh_lib.get_hash(int(matchn.home_player_1.iloc[i]),record)
    hp2  = fifa_dwh_lib.get_hash(int(matchn.home_player_2.iloc[i]),record)
    hp3  = fifa_dwh_lib.get_hash(int(matchn.home_player_3.iloc[i]),record)
    hp4  = fifa_dwh_lib.get_hash(int(matchn.home_player_4.iloc[i]),record)
    hp5  = fifa_dwh_lib.get_hash(int(matchn.home_player_5.iloc[i]),record)
    hp6  = fifa_dwh_lib.get_hash(int(matchn.home_player_6.iloc[i]),record)
    hp7  = fifa_dwh_lib.get_hash(int(matchn.home_player_7.iloc[i]),record)
    hp8  = fifa_dwh_lib.get_hash(int(matchn.home_player_8.iloc[i]),record)
    hp9  = fifa_dwh_lib.get_hash(int(matchn.home_player_9.iloc[i]),record)
    hp10 = fifa_dwh_lib.get_hash(int(matchn.home_player_10.iloc[i]),record)
    hp11 = fifa_dwh_lib.get_hash(int(matchn.home_player_11.iloc[i]),record)
    ap1  = fifa_dwh_lib.get_hash(int(matchn.away_player_1.iloc[i]),record)
    ap2  = fifa_dwh_lib.get_hash(int(matchn.away_player_2.iloc[i]),record)
    ap3  = fifa_dwh_lib.get_hash(int(matchn.away_player_3.iloc[i]),record)
    ap4  = fifa_dwh_lib.get_hash(int(matchn.away_player_4.iloc[i]),record)
    ap5  = fifa_dwh_lib.get_hash(int(matchn.away_player_5.iloc[i]),record)
    ap6  = fifa_dwh_lib.get_hash(int(matchn.away_player_6.iloc[i]),record)
    ap7  = fifa_dwh_lib.get_hash(int(matchn.away_player_7.iloc[i]),record)
    ap8  = fifa_dwh_lib.get_hash(int(matchn.away_player_8.iloc[i]),record)
    ap9  = fifa_dwh_lib.get_hash(int(matchn.away_player_9.iloc[i]),record)
    ap10 = fifa_dwh_lib.get_hash(int(matchn.away_player_10.iloc[i]),record)
    ap11 = fifa_dwh_lib.get_hash(int(matchn.away_player_11.iloc[i]),record)
    fmt = "insert into link_match values ("+27*"'{:}',"+"'{:}')"
    db.execute(fmt.format( hk, lg ,ht, at, hp1, hp2, hp3, hp4, hp5, hp6, hp7, hp8, hp9, hp10, \
    hp11, ap1, ap2, ap3, ap4, ap5, ap6, ap7, ap8, ap9, ap10, ap11, datefmt, record ))

# sat_match
for i in range(len(match2)):
    bk = match2.match_api_id.iloc[i]
    datenow = datetime.datetime.now()
    datefmt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dateend = '9999-12-31 23:59:59'
    record = 'database01.sqlite'
    hk = fifa_dwh_lib.get_hash(bk,record)
    hd = fifa_dwh_lib.get_hash(matcho.iloc[i,:],matchn.iloc[i,:])
    fmt = "insert into sat_match values ("+61*"'{:}',"+"'{:}')"
    db.execute(fmt.format(hk, datefmt, dateend, record, hd, \
    matcho.season.iloc[i], matchn.stage.iloc[i], matcho.date.iloc[i], \
    matchn.home_team_goal.iloc[i], matchn.away_team_goal.iloc[i], \
    int(matchn.home_player_X1.iloc[i]),  int(matchn.home_player_X2.iloc[i]),  \
    int(matchn.home_player_X3.iloc[i]),  int(matchn.home_player_X4.iloc[i]),  \
    int(matchn.home_player_X5.iloc[i]),  int(matchn.home_player_X6.iloc[i]),  \
    int(matchn.home_player_X7.iloc[i]),  int(matchn.home_player_X8.iloc[i]),  \
    int(matchn.home_player_X9.iloc[i]),  int(matchn.home_player_X10.iloc[i]), \
    int(matchn.home_player_X11.iloc[i]), int(matchn.away_player_X1.iloc[i]),  \
    int(matchn.away_player_X2.iloc[i]),  int(matchn.away_player_X3.iloc[i]),  \
    int(matchn.away_player_X4.iloc[i]),  int(matchn.away_player_X5.iloc[i]),  \
    int(matchn.away_player_X6.iloc[i]),  int(matchn.away_player_X7.iloc[i]),  \
    int(matchn.away_player_X8.iloc[i]),  int(matchn.away_player_X9.iloc[i]),  \
    int(matchn.away_player_X10.iloc[i]), int(matchn.away_player_X11.iloc[i]), \
    int(matchn.home_player_Y1.iloc[i]),  int(matchn.home_player_Y2.iloc[i]),  \
    int(matchn.home_player_Y3.iloc[i]),  int(matchn.home_player_Y4.iloc[i]),  \
    int(matchn.home_player_Y5.iloc[i]),  int(matchn.home_player_Y6.iloc[i]),  \
    int(matchn.home_player_Y7.iloc[i]),  int(matchn.home_player_Y8.iloc[i]),  \
    int(matchn.home_player_Y9.iloc[i]),  int(matchn.home_player_Y10.iloc[i]), \
    int(matchn.home_player_Y11.iloc[i]), int(matchn.away_player_Y1.iloc[i]),  \
    int(matchn.away_player_Y2.iloc[i]),  int(matchn.away_player_Y3.iloc[i]),  \
    int(matchn.away_player_Y4.iloc[i]),  int(matchn.away_player_Y5.iloc[i]),  \
    int(matchn.away_player_Y6.iloc[i]),  int(matchn.away_player_Y7.iloc[i]),  \
    int(matchn.away_player_Y8.iloc[i]),  int(matchn.away_player_Y9.iloc[i]),  \
    int(matchn.away_player_Y10.iloc[i]), int(matchn.away_player_Y11.iloc[i]), \
    matcho.goal.iloc[i], matcho.shoton.iloc[i], matcho.shotoff.iloc[i], matcho.foulcommit.iloc[i], \
    matcho.card.iloc[i], matcho.cross.iloc[i], matcho.corner.iloc[i], matcho.possession.iloc[i] ))



