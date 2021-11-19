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

import engine as eg

engine = eg.engine_gen(pwd)
"""
def c1_function_get_competitive_drivers(query_engine=engine):
    query = '''
            WITH L_races(raceId, forename, surname, points) AS(
                SELECT r.raceId, d.forename, d.surname, r.points
                FROM DRIVERS d 
                INNER JOIN results r ON d.driverId=r.driverID
                WHERE d.driverId = 1 AND r.points<>0
            ), someone_performance_compare_with_lewis_when_playing_in_same_game(driverId, Like_lewis) AS (
            SELECT d.driverId as driverId,
                   sum(r.points)/sum(lr.points) as like_lewis
                   --RANK() OVER (ORDER BY sum(r.points)/sum(lr.points) ASC) rank
            FROM DRIVERS d 
            INNER JOIN results r ON d.driverId=r.driverID
            INNER JOIN races ra ON r.raceId = ra.raceId
            INNER JOIN L_races lr ON lr.raceId = ra.raceId
            GROUP BY d.driverID
            ORDER BY like_lewis DESC
            FETCH FIRST 10 ROWS ONLY
            ),

            ''' 
    data = pd.read_sql(query, query_engine)
    print(data)
    most_10_likes = data.to_json(orient="table")
    return most_10_likes

most_10_likes = c1_function_get_competitive_drivers()
#print(most_10_likes)
#print(compare_all_same_race)
"""

def c1_function_a(driverId,query_engine=engine):

    query = '''
            WITH L_races(raceId, points) AS(
                SELECT r.raceId, r.points
                FROM DRIVERS d 
                INNER JOIN results r ON d.driverId=r.driverID
                WHERE d.driverId = 1 AND r.points<>0
            )
            SELECT ra.year, ra.name, d.forename, d.surname, 
                   ra.raceId , r.points as someone_points, lr.points as L_point
            FROM DRIVERS d 
            INNER JOIN results r ON d.driverId=r.driverID
            INNER JOIN races ra ON r.raceId = ra.raceId
            INNER JOIN L_races lr ON lr.raceId = ra.raceId
            WHERE d.driverId = {} AND r.points<>0'''.format(driverId) 

    data = pd.read_sql(query, query_engine)
    json_all_result = data.to_json(orient="table")

    query = '''
            WITH L_races(raceId, forename, surname, points) AS(
                SELECT r.raceId, d.forename, d.surname, r.points
                FROM DRIVERS d 
                INNER JOIN results r ON d.driverId=r.driverID
                WHERE d.driverId = 1 AND r.points<>0
            )
            SELECT d.forename as someone_forename, d.surname as someone_surname,
                   sum(r.points)/count(r.raceId) as someone_avg_points, 
                   lr.forename as Lewis_forename, lr.surname as Lewis_surname,
                   sum(lr.points)/count(r.raceId) as L_avg_point, 
                   sum(r.points)/sum(lr.points) as similarity
            FROM DRIVERS d 
            INNER JOIN results r ON d.driverId=r.driverID
            INNER JOIN races ra ON r.raceId = ra.raceId
            INNER JOIN L_races lr ON lr.raceId = ra.raceId
            WHERE d.driverId = {} AND r.points<>0
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
                SELECT forename, surname, AVG(r.points), RANK() OVER (ORDER BY ra.year ASC) rank
                FROM results r
                INNER JOIN races ra USING (raceId)
                INNER JOIN drivers d USING (driverId)
                WHERE driverId = 1 and r.points<>0
                GROUP BY ra.year, forename, surname
                ORDER BY ra.year ASC
                FETCH FIRST 3 ROW ONLY
            ),
            A_first_3_year_races(forename, surname, points, rank) AS(
                SELECT forename, surname, AVG(r.points), RANK() OVER (ORDER BY ra.year ASC) rank
                FROM results r
                INNER JOIN races ra USING (raceId)
                INNER JOIN drivers d USING (driverId)
                WHERE driverId = {} and r.points<>0
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
    compare_first_three_years = data.to_json(orient="table")

    query = '''
             WITH L_first_3_year_races(driverId, points, rank) AS(
                SELECT driverId, AVG(r.points), RANK() OVER (ORDER BY ra.year ASC) rank
                FROM results r
                INNER JOIN races ra USING (raceId)
                WHERE driverId = 1 and r.points<>0
                GROUP BY ra.year, driverId
                ORDER BY ra.year ASC
                FETCH FIRST 3 ROW ONLY
            ),
            A_first_3_year_races(driverId, points, rank) AS(
                SELECT driverId, AVG(r.points), RANK() OVER (ORDER BY ra.year ASC) rank
                FROM results r
                INNER JOIN races ra USING (raceId)
                WHERE driverId = {} and r.points<>0
                GROUP BY ra.year, driverId
                ORDER BY ra.year ASC
                FETCH FIRST 3 ROW ONLY
            )
            SELECT sum(l.points)/sum(a.points) AS similarity
            FROM L_first_3_year_races l
            INNER JOIN A_first_3_year_races a USING (rank)
            GROUP BY l.driverId
            '''.format(driverId) 
    data = pd.read_sql(query, query_engine)
    similarity_of_first_three_years = data.to_json(orient="table")

    return compare_first_three_years, similarity_of_first_three_years

compare_first_three_years, similarity_of_first_three_years = c1_function_b(2)
#print(compare_first_three_years)
#print(similarity_of_first_three_years)

def c1_function_c(driverId, query_engine=engine):
    query = '''
            WITH L_laps(raceId, lap, forename, surname, milliseconds) AS(
                SELECT l.raceId, l.lap, d.forename, d.surname, l.milliseconds
                FROM lapTimes l
                INNER JOIN drivers d ON l.driverId=d.driverID
                INNER JOIN results r ON l.raceId=r.raceId
                WHERE d.driverId = 1 and r.points<>0
            )
            SELECT ra.year, ra.raceId, ra.name,
            d.forename as someone_forename, d.surname as someone_surname, AVG(l.milliseconds)/100 as someone_lap_time_in_sec, 
            ll.forename as Lewis_forename, ll.surname as Lewis_surname, AVG(ll.milliseconds)/100 as L_lap_time_in_sec
            FROM DRIVERS d 
            INNER JOIN lapTimes l ON d.driverId=l.driverID
            INNER JOIN races ra ON l.raceId = ra.raceId
            INNER JOIN results r ON l.raceId=r.raceId
            INNER JOIN L_laps ll ON ll.raceId = l.raceId AND LL.lap=l.lap
            WHERE d.driverId = {} AND r.points<>0
            GROUP BY ra.year, ra.raceId, ra.name, d.forename, d.surname,ll.forename,ll.surname
            '''.format(driverId) 
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
                   sum(l.milliseconds)/count(l.lap)/100 as someone_avg_lap_time_in_sec, 
                   ll.forename as Lewis_forename, ll.surname as Lewis_surname,
                   sum(ll.milliseconds)/count(ll.lap)/100 as L_avg_lap_time_in_sec, 
                   sum(l.milliseconds)/sum(ll.milliseconds) as similarity
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

compare_in_each_lap, compare_avg_laps_time = c1_function_c(2)
#print(compare_in_each_lap, compare_avg_laps_time)




# https://www.oracletutorial.com/python-oracle/connecting-to-oracle-database-in-python/
# https://oracle.github.io/python-cx_Oracle/
# https://stackoverflow.com/questions/55823744/how-to-fix-cx-oracle-databaseerror-dpi-1047-cannot-locate-a-64-bit-oracle-cli
