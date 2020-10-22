"""
fifa_dwh_lib.py is a modules. It connects to the AWS Postgres SQL Serverand 
and stores following functions.
output_table: selects all attributes and n datasets of a given table
create_hashkey: creates a hash_key in 32-bit Unicode Transformation Format
get_hash: creates a hash_key in 32-bit Unicode Transformation Format with extended cleaning functionalities
create_hab: creates a hub with a given name.
insert_hub: inserts one dataset into a hub of a given name
create_sat_team: creates satellite sat_team for hub_team
create_sat_team_att: creates satellite sat_team_att for table hub_team
create_link_match: creates table link_match
create_sat_match: creates satellite sat_match for table link_match 			
create_sat_player: creates satellite sat_player for hub_player
create_sat_player_att: creates satellite sat_player_att for table hub_player_att
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


# connect to sql server
sql_connect = 'postgres://postgres:topsecret00@database-1.clu9fkojue9l.eu-central-1.rds.amazonaws.com'
db = create_engine(sql_connect, echo=True)

# ouput of n datasets from a table
def output_table(table_name,n=1000):
  out = db.execute("select * from {:} limit {:.0f} ".format(str(table_name),n))
  for o in out:
    print(o)
  return

# create hashkey from a single text input
def create_hashkey(text):
    x = np.str(text)
    y = x.replace(' ','').lower().encode('utf-8')
    hashkey = hashlib.md5(y).hexdigest()
    return hashkey

# create hashkey from a series of inputs
def get_hash(*fields):
    fields2 = []
    for field in fields:
        n = np.size(field)
        if n == 1:
            fields2.append(field)
        else:
            for f in field:
                fields2.append(f)
    
    clean_str = ""
    first = False
    for field in fields2:
        string = str(field).strip().lower()
        if first:
            clean_str += ";"
        if (string != "NULL"):
            clean_str += (
                string.replace(' ','')
                .replace('\n','')
                .replace('\t','')
                .replace('\v','')
                .replace('\r','')
                .replace('\\','\\\\')
                .replace(';','\\;'))
            first = True
    		
    hashkey = hashlib.md5(clean_str.encode("UTF-8")).hexdigest()
    
    return hashkey

# create a hub
def create_hub(hub_name,hash_name):
    command="""
    create table if not exists {:}(
    {:}                  varchar(32) primary key,
    business_key         integer,
    load_datetime_stamp  timestamp,
    record_source        varchar(20))
    """.format(str(hub_name),str(hash_name))
    return command

# insert values into a hub
def insert_hub(hub_name, hk, bk, datefmt, record):
    command="""
    insert into {:} values ('{:}', '{:}', '{:}', '{:}')
    """.format(str(hub_name), hk, bk, datefmt, record)
    return command

# satellite table sat_team with a reference to hub_team
def create_sat_team():
    command="""
    create table if not exists sat_team(
    hash_team            varchar(32) references hub_team(hash_team),
    load_datetime_stamp  timestamp,
    end_datetime_stamp   timestamp,
    record_source        varchar(20),
    hash_diff            varchar(32),
    team_fifa_api_id     float,
    long_name            varchar(30),
    short_name           varchar(3),
    primary key(hash_team,load_datetime_stamp))
    """
    return command

# satellite table sat_team_att with a reference to table hub_team
def create_sat_team_att():
    command="""
    create table if not exists sat_team_att(
    hash_team                   varchar(32) references hub_team(hash_team),
    load_datetime_stamp         timestamp,
    end_datetime_stamp          timestamp,
    record_source               varchar(20),
    hash_diff                   varchar(32),
    team_fifa_api_id            integer,
    date                        timestamp,
    buildUpPlaySpeed            integer,
    buildUpPlaySpeedClass       text,
    buildUpPlayDribbling        float,
    buildUpPlayDribblingClass   text,
    buildUpPlayPassing          integer,
    buildUpPlayPassingClass     text,
    buildUpPlayPositioningClass text,
    chanceCreationPassing       integer,
    chanceCreationPassingClass  text,
    chanceCreationCrossing      integer,
    chanceCreationCrossingClass text,
    chanceCreationShooting      integer,
    chanceCreationShootingClass text,
    chanceCreationPositioningClass text, 
    defencePressure             integer,
    defencePressureClass        text,
    defenceAggression           integer,
    defenceAggressionClass      text,
    defenceTeamWidth            integer,
    defenceTeamWidthClass       text,
    defenceDefenderLineClass    text,	
    primary key(hash_team,load_datetime_stamp))
    """
    return command

# satellite sat_league with a reference to table hub_league
def create_sat_league():
    command="""
    create table if not exists sat_league(
    hash_league          varchar(32) references hub_league(hash_league),
    load_datetime_stamp  timestamp,
    end_datetime_stamp   timestamp,
    record_source        varchar(20),
    hash_diff            varchar(32),
    country_id           integer,
    league_name          varchar(32),
    country_name         varchar(32),
    primary key(hash_league,load_datetime_stamp))
    """
    return command

# table link_match with references to hub_team, hub_player, and hub_league
def create_link_match():
    command="""
    create table if not exists link_match(
    hash_match_api      varchar(32) primary key,
    hash_league         varchar(32) references hub_league(hash_league),
    hash_home_team_api  varchar(32) references hub_team(hash_team),
    hash_away_team_api  varchar(32) references hub_team(hash_team),
    hash_home_player_1  varchar(32) references hub_player(hash_player),
    hash_home_player_2  varchar(32) references hub_player(hash_player),
    hash_home_player_3  varchar(32) references hub_player(hash_player),
    hash_home_player_4  varchar(32) references hub_player(hash_player),
    hash_home_player_5  varchar(32) references hub_player(hash_player),
    hash_home_player_6  varchar(32) references hub_player(hash_player),
    hash_home_player_7  varchar(32) references hub_player(hash_player),
    hash_home_player_8  varchar(32) references hub_player(hash_player),
    hash_home_player_9  varchar(32) references hub_player(hash_player),
    hash_home_player_10 varchar(32) references hub_player(hash_player),
    hash_home_player_11 varchar(32) references hub_player(hash_player),
    hash_away_player_1  varchar(32) references hub_player(hash_player),
    hash_away_player_2  varchar(32) references hub_player(hash_player),
    hash_away_player_3  varchar(32) references hub_player(hash_player),
    hash_away_player_4  varchar(32) references hub_player(hash_player),
    hash_away_player_5  varchar(32) references hub_player(hash_player),
    hash_away_player_6  varchar(32) references hub_player(hash_player),
    hash_away_player_7  varchar(32) references hub_player(hash_player),
    hash_away_player_8  varchar(32) references hub_player(hash_player),
    hash_away_player_9  varchar(32) references hub_player(hash_player),
    hash_away_player_10 varchar(32) references hub_player(hash_player),
    hash_away_player_11 varchar(32) references hub_player(hash_player),
    load_datetime_stamp   timestamp,
    record_source         varchar(20))
    """
    return command

# satellite sat_match with a reference to table link_match
def create_sat_match():
    command="""
    create table if not exists sat_match(
    hash_match_api       varchar(32) references link_match(hash_match_api),
    load_datetime_stamp  timestamp,
    end_datetime_stamp   timestamp,
    record_source        varchar(20),
    hash_diff            varchar(32),
    season               varchar(15),
    stage                integer,
    date                 date,
    home_team_goal       integer,
    away_team_goal       integer,
    home_player_X1       integer,
    home_player_X2       integer,
    home_player_X3       integer,
    home_player_X4       integer,
    home_player_X5       integer,
    home_player_X6       integer,
    home_player_X7       integer,
    home_player_X8       integer,
    home_player_X9       integer,
    home_player_X10      integer,
    home_player_X11      integer,
    away_player_X1       integer,
    away_player_X2       integer,
    away_player_X3       integer,
    away_player_X4       integer,
    away_player_X5       integer,
    away_player_X6       integer,
    away_player_X7       integer,
    away_player_X8       integer,
    away_player_X9       integer,
    away_player_X10      integer,
    away_player_X11      integer,
    home_player_Y1       integer,
    home_player_Y2       integer,
    home_player_Y3       integer,
    home_player_Y4       integer,
    home_player_Y5       integer,
    home_player_Y6       integer,
    home_player_Y7       integer,
    home_player_Y8       integer,
    home_player_Y9       integer,
    home_player_Y10      integer,
    home_player_Y11      integer,
    away_player_Y1       integer,
    away_player_Y2       integer,
    away_player_Y3       integer,
    away_player_Y4       integer,
    away_player_Y5       integer,
    away_player_Y6       integer,
    away_player_Y7       integer,
    away_player_Y8       integer,
    away_player_Y9       integer,
    away_player_Y10      integer,
    away_player_Y11      integer,
    goal                 text,
    shoton               text,
    shotoff              text,
    foulcommit           text,
    cardtext             text,
    crosstext            text,
    corner               text,
    possession           text,
    primary key(hash_match_api,load_datetime_stamp))
    """
    return command

# satellite sat_player with a reference to table hub_player
def create_sat_player():
    command="""
    create table if not exists sat_player(
    hash_player          varchar(32) references hub_player(hash_player),
    load_datetime_stamp  timestamp,
    end_datetime_stamp   timestamp,
    record_source        varchar(20),
    hash_diff            varchar(32),
    player_name          varchar(50),
    player_fifa_api_id   integer, 
    birthday             date,
    height               float,
    weight               integer,
    primary key(hash_player,load_datetime_stamp))
    """
    return command

# satellite sat_player_att with a reference to table hub_player
def create_sat_player_att():
    command="""
    create table if not exists sat_player_att(
    hash_player_att      varchar(32) references hub_player(hash_player),
    load_datetime_stamp  timestamp,
    end_datetime_stamp   timestamp,
    record_source        varchar(20),
    hash_diff            varchar(32),
    player_fifa_api_id   integer,
    date                 date,
    overall_rating       float,
    potential            float,
    preferred_foot       text,
    attacking_work_rate  text,
    defensive_work_rate  text,
    crossing             float,
    finishing            float,
    heading_accuracy     float,
    short_passing        float,
    volleys              float,
    dribbling            float,
    curve                float,
    free_kick_accuracy   float,
    long_passing         float,
    ball_control         float,
    acceleration         float,
    sprint_speed         float,
    agility              float,
    reactions            float,
    balance              float,
    shot_power           float,
    jumping              float, 
    stamina              float,
    strength             float,
    long_shots           float,
    aggression           float,
    interceptions        float,
    positioning          float,
    vision               float,
    penalties            float,
    marking              float,
    standing_tackle      float,
    sliding_tackle       float,
    gk_diving            float,
    gk_handling          float,
    gk_kicking           float,
    gk_positioning       float,
    gk_reflexes          float,
    primary key(hash_player_att,load_datetime_stamp))
    """
    return command

