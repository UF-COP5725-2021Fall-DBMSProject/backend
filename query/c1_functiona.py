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

def c1_functiona(driverId,query_engine=engine):

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

compare_in_each_race, compare_all_same_race = c1_functiona(engine, 2)
#print(compare_in_each_race)
#print(compare_all_same_race)






# https://www.oracletutorial.com/python-oracle/connecting-to-oracle-database-in-python/
# https://oracle.github.io/python-cx_Oracle/
# https://stackoverflow.com/questions/55823744/how-to-fix-cx-oracle-databaseerror-dpi-1047-cannot-locate-a-64-bit-oracle-cli
