###
# c1-function a
# First Query: List the Lewis and another driver's score in same games.
# Second Query: Compare their total score 
###
import json
import sys
pwd = sys.argv[1]

import pandas as pd
from sqlalchemy import create_engine

from . import engine as eg

engine = eg.engine_gen(pwd)

#TODO
# def c1_function_get_competitive_drivers(driverId, query_engine=engine):
#     return

def c1_function_a(driverId,query_engine=engine):

    query = '''
            WITH L_races(raceId, points) AS(
                SELECT r.raceId, r.points
                FROM DRIVERS d 
                INNER JOIN results r ON d.driverId=r.driverID
                WHERE d.driverId = 1
            )
            SELECT ra.year, ra.name, d.forename, d.surname, ra.raceId , r.points as someone_points, lr.points as L_point
            FROM DRIVERS d 
            INNER JOIN results r ON d.driverId=r.driverID
            INNER JOIN races ra ON r.raceId = ra.raceId
            INNER JOIN L_races lr ON lr.raceId = ra.raceId
            WHERE d.driverId = {}'''.format(driverId) 

    data = pd.read_sql(query, query_engine)
    json_all_result = data.to_json(orient="table")

    query = '''
            WITH L_races(raceId, forename, surname, points) AS(
                SELECT r.raceId, d.forename, d.surname, r.points
                FROM DRIVERS d 
                INNER JOIN results r ON d.driverId=r.driverID
                WHERE d.driverId = 1
            )
            SELECT d.forename as someone_forename, d.surname as someone_surname,
                   sum(r.points)/count(r.raceId) as someone_avg_points, 
                   lr.forename as Lewis_forename, lr.surname as Lewis_surname,
                   sum(lr.points)/count(r.raceId) as L_avg_point, 
                   sum(r.points)/sum(lr.points) as likes
            FROM DRIVERS d 
            INNER JOIN results r ON d.driverId=r.driverID
            INNER JOIN races ra ON r.raceId = ra.raceId
            INNER JOIN L_races lr ON lr.raceId = ra.raceId
            WHERE d.driverId = {}
            GROUP BY d.driverID, d.forename, d.surname,lr.forename,lr.surname
            '''.format(driverId) 

    data = pd.read_sql(query, query_engine)
    json_compare_result = data.to_json(orient="table")
    return json_all_result, json_compare_result

compare_in_each_race, compare_all_same_race = c1_function_a(2)
#print(compare_in_each_race)
#print(compare_all_same_race)

def c1_function_b(driverId, query_engine=engine):
    query = '''
             WITH L_first_3_year_races(forename, surname, points, rank) AS(
                SELECT forename, surname, sum(r.points), RANK() OVER (ORDER BY ra.year ASC) rank
                FROM results r
                INNER JOIN races ra USING (raceId)
                INNER JOIN drivers d USING (driverId)
                WHERE driverId = 1
                GROUP BY ra.year, forename, surname
                ORDER BY ra.year ASC
                FETCH FIRST 3 ROW ONLY
            ),
            A_first_3_year_races(forename, surname, points, rank) AS(
                SELECT forename, surname, sum(r.points), RANK() OVER (ORDER BY ra.year ASC) rank
                FROM results r
                INNER JOIN races ra USING (raceId)
                INNER JOIN drivers d USING (driverId)
                WHERE driverId = {}
                GROUP BY ra.year, forename, surname
                ORDER BY ra.year ASC
                FETCH FIRST 3 ROW ONLY
            )
            SELECT rank AS year,
                    l.forename as Lewis_forename,l.surname as Lewis_surname, l.points AS Lewis_score,
                    a.forename as Others_forename, a.surname as Others_surname, a.points AS Others_score 
            FROM L_first_3_year_races l
            INNER JOIN A_first_3_year_races a USING (rank)
            ORDER BY rank ASC
            '''.format(driverId) 
    data = pd.read_sql(query, query_engine)
    json_all_result = data.to_json(orient="table")
    return json_all_result

# json_all_result = c1_function_b(2)
# print(json_all_result)

def c1_function_c(driverId, query_engine=engine):
    query = '''
            WITH L_laps(raceId, lap, forename, surname, milliseconds) AS(
                SELECT l.raceId, l.lap, d.forename, d.surname, milliseconds
                FROM lapTimes l
                INNER JOIN drivers d ON l.driverId=d.driverID
                WHERE d.driverId = 1
            )
            SELECT ra.year, ra.raceId, ra.name, l.lap,
            d.forename as someone_forename, d.surname as someone_surname, l.milliseconds as someone_lap_time, 
            ll.forename as Lewis_forename, ll.surname as someone_surname, ll.milliseconds as L_point
            FROM DRIVERS d 
            INNER JOIN lapTimes l ON d.driverId=l.driverID
            INNER JOIN races ra ON l.raceId = ra.raceId
            INNER JOIN L_laps ll ON ll.raceId = l.raceId AND LL.lap=l.lap
            WHERE d.driverId = {}'''.format(driverId) 
    data = pd.read_sql(query, query_engine)
    compare_in_each_lap = data.to_json(orient="table")

    query = '''
            WITH L_laps(raceId, lap, forename, surname, milliseconds) AS(
                SELECT l.raceId, l.lap, d.forename, d.surname, milliseconds
                FROM lapTimes l
                INNER JOIN drivers d ON l.driverId=d.driverID
                WHERE d.driverId = 1
            )
            SELECT d.forename as someone_forename, d.surname as someone_surname,
                   sum(l.milliseconds)/count(l.lap) as someone_avg_lap_time_milliseconds, 
                   ll.forename as Lewis_forename, ll.surname as Lewis_surname,
                   sum(ll.milliseconds)/count(ll.lap) as L_avg_lap_time_milliseconds, 
                   sum(l.milliseconds)/sum(ll.milliseconds) as likes
            FROM DRIVERS d 
            INNER JOIN lapTimes l ON d.driverId=l.driverID
            INNER JOIN races ra ON l.raceId = ra.raceId
            INNER JOIN L_laps ll ON ll.raceId = l.raceId AND LL.lap=l.lap
            WHERE d.driverId = {}
            GROUP BY d.driverID, d.forename, d.surname,ll.forename,ll.surname
            '''.format(driverId) 

    data = pd.read_sql(query, query_engine)
    compare_avg_laps_time = data.to_json(orient="table")

    return compare_in_each_lap, compare_avg_laps_time

# compare_in_each_lap, compare_avg_laps_time = c1_function_c(2)
# print(compare_in_each_lap, compare_avg_laps_time)



#TODO
# def c1_function_c(driverId, query_engine=engine):
#     return

# https://www.oracletutorial.com/python-oracle/connecting-to-oracle-database-in-python/
# https://oracle.github.io/python-cx_Oracle/
# https://stackoverflow.com/questions/55823744/how-to-fix-cx-oracle-databaseerror-dpi-1047-cannot-locate-a-64-bit-oracle-cli
