# Soccer matches in Europe

<b> Analysis of the European Soccer Database with over 28000 Matches in 11 Leagues between season 2008/2009 and 2015/2016. </b>

In this repository I want to do several projects. Theese are one data engineering and two data science projects. <br>
First, build up of a Data Warehouse on AWS server.
Secondary, analysis of game results, home-team advantage, and performance of Hamburger SV.
Thirdly, analysis of the player attributes of Hamburger SV, detection of weak points within the team. Suggesting helpful soccer players.

<b> Getting database from Kaggle </b> 

The database is available on Kaggle. Here is a short overwiev of commands, which are needed for searching and downloading. <br>

 - Seaching for European Soccer Database based on title <br>
   kaggle datasets list -s 'European Soccer Database' <br>
 - Searching for Databases based on reference <br>
   kaggle datasets files hugomathien/soccer <br>
 - Download European Soccer Database from reference hugomathien/soccer <br>
   kaggle datasets download hugomathien/soccer -f database.sqlite -p 'Z:/IT-Projekte/FIFA soccer analysis/' <br>

 <br> 

<b> Initialization of a data warehouse </b> 

The chosen DWH is the data vault. It is built with two python scripts on my AWS account. <br>
Data are extracted from the sql-database and loaded into the data vault. <br>

<i> fifa_dwh_lib.py  </i>  <br>
 - About: module for fifa_data_vault.py. It contains subroutines for creating hubs, satelites, and links, inserting into existing hubs and creating hashkeys. <br>
 - Code: Python 3.6 <br>
 - Dependencies: Postgres SQL and connection to AWS server <br>
 - Packages: numpy, pandas, datetime, string, sqlalchemy, psycopg2, hashlib, sqlite3 <br>
 
<i> fifa_data_vault.py </i> <br>
 - About: This program extracts  european soccer data from the database database.sqlite. The aim is to set up the DWH of kind Data Vault on AWS.
 - Code: Python 3.6 <br>
 - Dependencies: Postgres SQL and connection to AWS server <br>
 - Data: database.sqlite <br>
 - Pakete: numpy, pandas, datetime, string, sqlalchemy, psycopg2, hashlib, sqlite3, fifa_dwh_lib <br>

<br> 

<b> Analysis of game results </b> 

Analysis of 25979 soccer games in 11 European leagues between the seasons 2008/2009 and 2015/2016. Four tables are nedded  from the European soccer database. These are leque, country, match, and team. The analysis consists of three major parts. <br>
- imvestigation of game results: most likely results, relation between home--team victory and away-team victory, number of goals
- ivestigation of home-team advantage for each legue and season including statistical evaluation with t-tesst
- performance of Hamburger SV in comparison to FC Bayern Munich: points and goals 
 
<i> soccer_game_results.ipynb </i>  <br>
 - About: Analysis of soccer game results, home-team advantage, and performance of Hamburger SV in comparison to FC Bayern Munich
 - Code: Jupyter Notebook 6.1 <br>
 - Data: database.sqlite.zip  <br>
 - Packages: os, sys, zipfile, numpy, pandas, datetime, time, scipy, statsmodels, sqlite3, string, seaborn, matplotlib  <br>

 <br> 
  
<b> Players of Hamburger SV </b>

