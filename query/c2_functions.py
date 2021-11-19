###
# c2-function
###
import json
import sys
pwd = sys.argv[1]

import pandas as pd
from sqlalchemy import create_engine

from . import engine as eg

engine = eg.engine_gen(pwd)

def c2_function(start_year, end_year, query_engine=engine):

    query = '''
            WITH Cons_avg_pits(constructorId, year, avg_pits) AS(
                SELECT r.constructorId, ra.year, AVG(cast(regexp_replace(p.duration, '[^0-9.]+', '') as number)) as duration
                FROM results r
                INNER JOIN races ra ON r.raceId=ra.raceId
                INNER JOIN pitStops p ON r.raceId=p.raceId AND r.driverId = p.driverId
                WHERE ra.year BETWEEN {sy} and {ey}
                GROUP By constructorId, year
            ), Cons_total_points(constructorId, year, totlal_point) AS(
                SELECT cr.constructorId, ra.year, SUM(cr.points)
                FROM ConstructorResults cr
                INNER JOIN races ra ON cr.raceId=ra.raceId
                WHERE ra.year BETWEEN {sy} and {ey}
                GROUP By cr.constructorId, ra.year
            ), Cons_total_errors(ConstructorId, year, errors) AS(
                SELECT r.constructorId, ra.year, COUNT(s.STATUSID)
                FROM results r
                INNER JOIN races ra ON r.raceId=ra.raceId
                INNER JOIN status s ON r.statusId = s.statusId
                WHERE ra.year BETWEEN {sy} AND {ey}
                AND s.statusId >=5
                AND s.status NOT Like '+%'
                GROUP By r.constructorId, ra.year
            ), Cons_budget(ConstructorId, year, budgets) AS(
                SELECT b.constructorId, b.year, b.budget
                FROM ConstructorBudget b
                WHERE b.year BETWEEN {sy} AND {ey}
            )
            SELECT ConstructorId, year, c.name, cap.avg_pits as avg_pits,
                    ctp.totlal_point as totlal_point,
                    cte.errors as errors,
                    cb.budgets as budgets
            FROM Constructors c 
            INNER JOIN Cons_avg_pits cap USING (ConstructorID)
            INNER JOIN Cons_total_points ctp USING (ConstructorID, year)
            INNER JOIN Cons_total_errors cte USING (ConstructorID, year)
            INNER JOIN Cons_budget cb USING (ConstructorID, year)
            ORDER By ConstructorId, year ASC'''.format(sy=start_year, ey=end_year)

    data = pd.read_sql(query, query_engine)
    json_all_result = data.to_json(orient="table")
    return json_all_result

   

# Constructor_performance = c2_function(2015,2017)
# print(Constructor_performance)


#TODO
# def c1_function_c(driverId, query_engine=engine):
#     return

# https://www.oracletutorial.com/python-oracle/connecting-to-oracle-database-in-python/
# https://oracle.github.io/python-cx_Oracle/
# https://stackoverflow.com/questions/55823744/how-to-fix-cx-oracle-databaseerror-dpi-1047-cannot-locate-a-64-bit-oracle-cli
