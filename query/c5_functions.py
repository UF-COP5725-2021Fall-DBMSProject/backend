###
# c5-function
###
import json
import sys
pwd = sys.argv[1]

import pandas as pd
from sqlalchemy import create_engine

import engine as eg

engine = eg.engine_gen(pwd)

def c5_function_get_top_10_spoliers(query_engine=engine):

    query = '''
            WITH avg_pitstops(raceId, driverId, duration_sec) AS(
                SELECT raceId, driverId, AVG(milliseconds)/1000 as duration_sec
                FROM pitstops ps
                GROUP BY raceId, driverId
            )
            SELECT d.driverId, d.forename, d.surname, AVG(q.position - re.position) as position_diff, COUNT(ra.raceId) as race_num
            FROM drivers d
            INNER JOIN qualifying q ON d.driverId = q.driverId
            INNER JOIN races ra ON q.raceId = ra.raceId
            INNER JOIN results re ON re.raceId = ra.raceId AND re.driverId = d.driverId
            INNER JOIN avg_pitstops aps ON aps.raceId = ra.raceId AND aps.driverId = d.driverId
            WHERE aps.duration_sec < (SELECT AVG(duration_sec) FROM avg_pitstops)
            AND q.position is not NULL 
            AND re.position is not NULL
            GROUP BY d.driverId, d.forename, d.surname
            HAVING COUNT(ra.raceId) > 10
            ORDER BY AVG(q.position - re.position) ASC
            FETCH FIRST 10 ROW ONLY
            '''
    data = pd.read_sql(query, query_engine)
    top_ten_spoliers = data.to_json(orient="table")

    return top_ten_spoliers

top_ten_spoliers= c5_function_get_top_10_spoliers()

def c5_function_get_spolier_record(driverId,query_engine=engine):
    query = '''
            WITH avg_pitstops(raceId, driverId, duration_sec) AS(
                SELECT raceId, driverId, AVG(milliseconds)/1000 as duration_sec
                FROM pitstops ps
                GROUP BY raceId, driverId
            )
            SELECT ra.year, ra.raceId, ra.name as Race_name, d.driverId, d.forename as Forename, d.surname as Surname, 
                   q.position as qualifying_position, re.position as result_position ,
                   q.position - re.position as position_diff
            FROM drivers d
            INNER JOIN qualifying q ON d.driverId = q.driverId
            INNER JOIN races ra ON q.raceId = ra.raceId
            INNER JOIN results re ON re.raceId = ra.raceId AND re.driverId = d.driverId
            INNER JOIN avg_pitstops aps ON aps.raceId = ra.raceId AND aps.driverId = d.driverId
            WHERE d.driverId={did}
            AND q.position is not NULL AND re.position is not NULL
            AND aps.duration_sec < (SELECT AVG(duration_sec) FROM avg_pitstops)
            ORDER BY q.position - re.position ASC
            FETCH FIRST 10 ROW ONLY
            '''.format(did=driverId)

    data = pd.read_sql(query, query_engine)
    spolier_record = data.to_json(orient="table")

    return spolier_record

spolier_record = c5_function_get_spolier_record(2)
#print(detail_record_of_any_risky_drivers)


# https://www.oracletutorial.com/python-oracle/connecting-to-oracle-database-in-python/
# https://oracle.github.io/python-cx_Oracle/
# https://stackoverflow.com/questions/55823744/how-to-fix-cx-oracle-databaseerror-dpi-1047-cannot-locate-a-64-bit-oracle-cli
