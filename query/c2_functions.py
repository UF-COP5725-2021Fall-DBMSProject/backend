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
                SELECT r.constructorId, ra.year, AVG(p.milliseconds)/1000 as duration
                FROM results r
                INNER JOIN races ra ON r.raceId=ra.raceId
                INNER JOIN pitStops p ON r.raceId=p.raceId AND r.driverId = p.driverId
                WHERE ra.year BETWEEN {sy} and {ey} AND p.milliseconds<60000
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
            SELECT ConstructorId as constructor_id, year, c.name, cap.avg_pits as avg_pits_time,
                    ctp.totlal_point as total_points,
                    cte.errors as errors,
                    cb.budgets as budgets
            FROM Constructors c 
            INNER JOIN Cons_avg_pits cap USING (ConstructorID)
            INNER JOIN Cons_total_points ctp USING (ConstructorID, year)
            INNER JOIN Cons_total_errors cte USING (ConstructorID, year)
            INNER JOIN Cons_budget cb USING (ConstructorID, year)
            ORDER By ConstructorId, year ASC'''.format(sy=start_year, ey=end_year)

    data = pd.read_sql(query, query_engine)
    Constructor_performance = data.to_json(orient="table")
    
    query = '''
            WITH Cons_first_year_budget(ConstructorId,name, budgets) AS(
                SELECT b.constructorId, b.name, b.budget
                FROM ConstructorBudget b
                WHERE b.year = {sy}
            ),Cons_last_year_budget(ConstructorId, name, budgets) AS(
                SELECT b.constructorId, b.name, b.budget
                FROM ConstructorBudget b
                WHERE b.year = {ey}
            )

            SELECT distinct b1.constructorId as constructor_id, b1.name, 
            b1.budgets as First_year_budget,
            b2.budgets as Last_year_budget,
            b2.budgets/b1.budgets as increase_percent
            FROM Cons_first_year_budget b1
            INNER JOIN Cons_last_year_budget b2 ON b1.ConstructorId= b2.ConstructorId
            WHERE b2.budgets < b1.budgets*1.3
            ORDER BY b2.budgets/b1.budgets ASC
            '''.format(sy=start_year, ey=end_year)
    data = pd.read_sql(query, query_engine)
    cons_whos_budget_increase_less_than_30percent = data.to_json(orient="table")

    query = '''
            WITH Cons_avg_pits(constructorId, name, year, avg_pits) AS(
                SELECT c.constructorId, c.name, ra.year, AVG(p.milliseconds)/1000 as duration
                FROM Constructors c 
                INNER JOIN results r ON c.constructorID=r.constructorID
                INNER JOIN races ra ON r.raceId=ra.raceId
                INNER JOIN pitStops p ON r.raceId=p.raceId AND r.driverId = p.driverId
                WHERE ra.year BETWEEN {sy} and {ey} AND p.milliseconds<60000
                GROUP By c.constructorId, c.name, year
            ), all_avg_pits(all_avg_pits) AS(
                SELECT AVG(avg_pits)
                FROM Cons_avg_pits
            )

            SELECT distinct constructorId as constructor_id, name, avg_pits
            FROM Cons_avg_pits
            WHERE avg_pits <= (SELECT * FROM all_avg_pits)
            ORDER BY avg_pits ASC
            '''.format(sy=start_year, ey=end_year)
    data = pd.read_sql(query, query_engine)
    cons_has_pit_time_less_than_avg = data.to_json(orient="table")

    query = '''
            WITH Cons_total_errors(ConstructorId,name, year, errors) AS(
                SELECT c.constructorId,c.name, ra.year, COUNT(s.STATUSID)
                FROM Constructors c 
                INNER JOIN results r ON c.constructorID=r.constructorID
                INNER JOIN races ra ON r.raceId=ra.raceId
                INNER JOIN status s ON r.statusId = s.statusId
                WHERE ra.year BETWEEN {sy} AND {ey}
                AND s.statusId >=5
                AND s.status NOT Like '+%'
                GROUP By c.constructorId, c.name, ra.year
            ),all_avg_errors(all_avg_errors) AS(
                SELECT AVG(errors)
                FROM Cons_total_errors
            )

            SELECT distinct ConstructorId as constructor_id, name, errors
            FROM Cons_total_errors
            WHERE errors <= (SELECT * FROM all_avg_errors)
            ORDER BY errors ASC
            '''.format(sy=start_year, ey=end_year)
    data = pd.read_sql(query, query_engine)
    cons_has_errors_less_than_avg = data.to_json(orient="table")

    return Constructor_performance,cons_whos_budget_increase_less_than_30percent, cons_has_pit_time_less_than_avg, cons_has_errors_less_than_avg


# Constructor_performance,cons_whos_budget_increase_less_than_30percent, cons_has_pit_time_less_than_avg, cons_has_errors_less_than_avg = c2_function(2015,2017)
# print(Constructor_performance)
# print(cons_whos_budget_increase_less_than_30percent,"\n\n")
# print(cons_has_pit_time_less_than_avg,"\n\n")
# print(cons_has_errors_less_than_avg,"\n\n")



# https://www.oracletutorial.com/python-oracle/connecting-to-oracle-database-in-python/
# https://oracle.github.io/python-cx_Oracle/
# https://stackoverflow.com/questions/55823744/how-to-fix-cx-oracle-databaseerror-dpi-1047-cannot-locate-a-64-bit-oracle-cli
