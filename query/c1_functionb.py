###
# c1-function b
# List the Lewis and another driver's first three years score to show if this driver 
# is potential to become next Lewis.
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
             WITH L_first_3_year_races(points, rank) AS(
                SELECT sum(r.points), RANK() OVER (ORDER BY ra.year ASC) rank
                FROM results r
                INNER JOIN races ra USING (raceId)
                WHERE r.driverId = 1
                GROUP BY ra.year
                ORDER BY ra.year ASC
                FETCH FIRST 3 ROW ONLY
            ),
            A_first_3_year_races(points, rank) AS(
                SELECT sum(r.points), RANK() OVER (ORDER BY ra.year ASC) rank
                FROM results r
                INNER JOIN races ra USING (raceId)
                WHERE r.driverId = {}
                GROUP BY ra.year
                ORDER BY ra.year ASC
                FETCH FIRST 3 ROW ONLY
            )
            SELECT rank AS year, l.points AS Lewis_score, a.points AS Others_score 
            FROM L_first_3_year_races l
            INNER JOIN A_first_3_year_races a USING (rank)
            ORDER BY rank ASC
            '''.format(driverId) 
    data = pd.read_sql(query, query_engine)
    json_all_result = data.to_json(orient="table")
    return json_all_result

json_all_result = c1_functiona(engine, 2)
print(json_all_result)






# https://www.oracletutorial.com/python-oracle/connecting-to-oracle-database-in-python/
# https://oracle.github.io/python-cx_Oracle/
# https://stackoverflow.com/questions/55823744/how-to-fix-cx-oracle-databaseerror-dpi-1047-cannot-locate-a-64-bit-oracle-cli
