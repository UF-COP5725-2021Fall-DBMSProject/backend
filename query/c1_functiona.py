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

def c1_functiona(query_engine,driverId):
    query = '''
            WITH L_races(raceId, points) AS(
                SELECT r.raceId, r.points
                FROM DRIVERS d 
                INNER JOIN results r ON d.driverId=r.driverID
                WHERE d.driverId = 1
            )
            SELECT ra.year, ra.raceId , r.points as someone_points, lr.points as L_point
            FROM DRIVERS d 
            INNER JOIN results r ON d.driverId=r.driverID
            INNER JOIN races ra ON r.raceId = ra.raceId
            INNER JOIN L_races lr ON lr.raceId = ra.raceId
            WHERE d.driverId = {}'''.format(driverId) 

    data = pd.read_sql(query, query_engine)
    json_all_result = data.to_json(orient="table")

    query = '''
            WITH L_races(raceId, points) AS(
                SELECT r.raceId, r.points
                FROM DRIVERS d 
                INNER JOIN results r ON d.driverId=r.driverID
                WHERE d.driverId = 1
            )
            SELECT sum(r.points)/count(r.raceId) as someone_avg_points, sum(lr.points)/count(r.raceId) as L_avg_point, sum(r.points)/sum(lr.points) as likes
            FROM DRIVERS d 
            INNER JOIN results r ON d.driverId=r.driverID
            INNER JOIN races ra ON r.raceId = ra.raceId
            INNER JOIN L_races lr ON lr.raceId = ra.raceId
            WHERE d.driverId = {}
            GROUP BY d.driverID
            '''.format(driverId) 

    data = pd.read_sql(query, query_engine)
    json_compare_result = data.to_json(orient="table")
    return json_all_result, json_compare_result

json_all_result, json_compare_result = c1_functiona(engine, 2)
print(json_compare_result)






# https://www.oracletutorial.com/python-oracle/connecting-to-oracle-database-in-python/
# https://oracle.github.io/python-cx_Oracle/
# https://stackoverflow.com/questions/55823744/how-to-fix-cx-oracle-databaseerror-dpi-1047-cannot-locate-a-64-bit-oracle-cli
