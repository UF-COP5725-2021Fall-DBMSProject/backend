###
# c5-function
###
import json
import sys
pwd = sys.argv[1]

import pandas as pd
from sqlalchemy import create_engine

from . import engine as eg

engine = eg.engine_gen(pwd)

def c5_function_get_top_10_spoilers(query_engine=engine):

    query = '''
            WITH avg_pitstops(raceId, driverId, duration_sec) AS(
                SELECT raceId, driverId, AVG(milliseconds)/1000 as duration_sec
                FROM pitstops ps
                GROUP BY raceId, driverId
            )
            SELECT d.driverId, d.forename, d.surname, AVG(q.position - re.position) as Lose_position, COUNT(ra.raceId) as race_num
            FROM drivers d
            INNER JOIN qualifying q ON d.driverId = q.driverId
            INNER JOIN races ra ON q.raceId = ra.raceId
            INNER JOIN results re ON re.raceId = ra.raceId AND re.driverId = d.driverId
            INNER JOIN avg_pitstops aps ON aps.raceId = ra.raceId AND aps.driverId = d.driverId
            WHERE aps.duration_sec < (SELECT AVG(duration_sec) FROM avg_pitstops)
            AND q.position is not NULL AND re.position is not NULL
            GROUP BY d.driverId, d.forename, d.surname
            HAVING COUNT(ra.raceId) > 20
            ORDER BY AVG(q.position - re.position) 
            FETCH FIRST 10 ROW ONLY
            '''
    data = pd.read_sql(query, query_engine)
    top_ten_spoilers = data.to_json(orient="table")

    return top_ten_spoilers

# top_ten_spoilers= c5_function_get_top_10_spoilers()

def c5_function_get_spoiler_record(driverId,query_engine=engine):
    query = '''
            WITH avg_pitstops(raceId, driverId, duration_sec) AS(
                SELECT raceId, driverId, AVG(milliseconds)/1000 as duration_sec
                FROM pitstops ps
                GROUP BY raceId, driverId
            )
            SELECT ra.year, ra.raceId as race_id, ra.name as Race_name, d.driverId, d.forename as Forename, d.surname as Surname, 
                   q.position as qualifying_position, re.position as result_position ,
                   q.position - re.position as Lose_position
            FROM drivers d
            INNER JOIN qualifying q ON d.driverId = q.driverId
            INNER JOIN races ra ON q.raceId = ra.raceId
            INNER JOIN results re ON re.raceId = ra.raceId AND re.driverId = d.driverId
            INNER JOIN avg_pitstops aps ON aps.raceId = ra.raceId AND aps.driverId = d.driverId
            WHERE d.driverId={did}
            AND q.position is not NULL AND re.position is not NULL
            AND aps.duration_sec < (SELECT AVG(duration_sec) FROM avg_pitstops)
            ORDER BY q.position - re.position
            FETCH FIRST 10 ROW ONLY
            '''.format(did=driverId)

    data = pd.read_sql(query, query_engine)
    spoiler_record = data.to_json(orient="table")

    return spoiler_record

# spoiler_record = c5_function_get_spoiler_record(2)
#print(detail_record_of_any_risky_drivers)


# https://www.oracletutorial.com/python-oracle/connecting-to-oracle-database-in-python/
# https://oracle.github.io/python-cx_Oracle/
# https://stackoverflow.com/questions/55823744/how-to-fix-cx-oracle-databaseerror-dpi-1047-cannot-locate-a-64-bit-oracle-cli
