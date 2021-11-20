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

def c4_function_get_useless_risky_aggresive_drivers_list(query_engine=engine):

    query = '''
            WITH driver_points(driverId, year, all_points) AS(
                SELECT d.driverId, ra.year, SUM(re.points)
                FROM drivers d
                INNER JOIN results re ON d.driverId=re.driverId
                INNER JOIN races ra ON re.raceId=ra.raceId
                GROUP BY d.driverId, ra.year
                ORDER BY d.driverId, ra.year
            ), driver_error(driverId, year, crash) AS(
                SELECT d.driverId, ra.year, COUNT(re.statusId) as crash
                FROM drivers d
                INNER JOIN results re ON d.driverId=re.driverId
                INNER JOIN races ra ON re.raceId=ra.raceId
                WHERE statusId BETWEEN 3 and 4
                GROUP BY d.driverId, ra.year

                HAVING COUNT(re.statusId)>3
                ORDER BY d.driverId, ra.year
            ), driver_point_and_error_each_year(driverId, year, points, crash) AS (
                SELECT DISTINCT dp.driverId, dp.year, dp.all_points as points, de.crash as crash  --, dp.all_points/de.crash as ratio
                FROM driver_points dp
                INNER JOIN driver_error de ON dp.driverId=de.driverId AND dp.year=de.year
                ORDER BY dp.driverId, dp.Year
            )
            SELECT d.driverId, d.forename as forename, d.surname as surname, 
                    SUM(points) as total_point, SUM(crash) as total_crash
            FROM driver_point_and_error_each_year dpe
            INNER JOIN drivers d ON dpe.driverId=d.driverId
            GROUP BY d.driverId, d.forename , d.surname
            HAVING SUM(points)<10
            ORDER BY SUM(crash) DESC, SUM(points) DESC
            '''
    data = pd.read_sql(query, query_engine)
    useless_drivers = data.to_json(orient="table")

    query = '''
            WITH driver_points(driverId, year, all_points) AS(
                SELECT d.driverId, ra.year, SUM(re.points)
                FROM drivers d
                INNER JOIN results re ON d.driverId=re.driverId
                INNER JOIN races ra ON re.raceId=ra.raceId
                GROUP BY d.driverId, ra.year
                ORDER BY d.driverId, ra.year
            ), driver_error(driverId, year, crash) AS(
                SELECT d.driverId, ra.year, COUNT(re.statusId) as crash
                FROM drivers d
                INNER JOIN results re ON d.driverId=re.driverId
                INNER JOIN races ra ON re.raceId=ra.raceId
                WHERE statusId BETWEEN 3 and 4
                GROUP BY d.driverId, ra.year
                ORDER BY d.driverId, ra.year
            ), driver_point_and_error_each_year(driverId, year, points, crash) AS (
                SELECT DISTINCT dp.driverId, dp.year, dp.all_points as points, de.crash as crash  --, dp.all_points/de.crash as ratio
                FROM driver_points dp
                INNER JOIN driver_error de ON dp.driverId=de.driverId AND dp.year=de.year
                ORDER BY dp.driverId, dp.Year
            )
            SELECT d.driverId, d.forename as forename, d.surname as surname, 
                    SUM(points) as total_point, SUM(crash) as total_crash, sum(crash)/sum(points) as crash_devide_by_point
            FROM driver_point_and_error_each_year dpe
            INNER JOIN drivers d ON dpe.driverId=d.driverId
            GROUP BY d.driverId, d.forename , d.surname
            HAVING SUM(points)>10
            ORDER BY  sum(crash)/sum(points) DESC
            FETCH First 10 ROWS ONLY
            '''
    data = pd.read_sql(query, query_engine)
    top_ten_risky_drivers = data.to_json(orient="table")

    query = '''
            WITH driver_points(driverId, year, all_points) AS(
                SELECT d.driverId, ra.year, SUM(re.points)
                FROM drivers d
                INNER JOIN results re ON d.driverId=re.driverId
                INNER JOIN races ra ON re.raceId=ra.raceId
                WHERE re.points>0
                GROUP BY d.driverId, ra.year
                ORDER BY d.driverId, ra.year
            ), driver_error(driverId, year, crash) AS(
                SELECT d.driverId, ra.year, COUNT(re.statusId) as crash
                FROM drivers d
                INNER JOIN results re ON d.driverId=re.driverId
                INNER JOIN races ra ON re.raceId=ra.raceId
                WHERE statusId BETWEEN 3 and 4
                GROUP BY d.driverId, ra.year
                ORDER BY d.driverId, ra.year
            ), driver_point_and_error_each_year(driverId, year, points, crash) AS (
                SELECT DISTINCT dp.driverId, dp.year, dp.all_points as points, de.crash as crash  --, dp.all_points/de.crash as ratio
                FROM driver_points dp
                INNER JOIN driver_error de ON dp.driverId=de.driverId AND dp.year=de.year
                ORDER BY dp.driverId, dp.Year
            )
            SELECT d.driverId, d.forename as forename, d.surname as surname, 
                    SUM(points) as total_point, SUM(crash) as total_crash, sum(points)/sum(crash) as point_devide_by_crash
            FROM driver_point_and_error_each_year dpe
            INNER JOIN drivers d ON dpe.driverId=d.driverId
            GROUP BY d.driverId, d.forename , d.surname
            ORDER BY sum(points)/sum(crash) DESC
            FETCH First 10 ROWS ONLY
            '''
    data = pd.read_sql(query, query_engine)
    top_ten_risky_drivers = data.to_json(orient="table")


<<<<<<< HEAD
    return top_ten_risky_drivers

top_ten_risky_drivers = c4_function_get_top_10_risky_drivers()
=======
    return useless_drivers,top_ten_risky_drivers, top_ten_aggresive_drivers

useless_drivers,top_ten_risky_drivers, top_ten_aggresive_drivers = c4_function_get_useless_risky_aggresive_drivers_list()
>>>>>>> cab04a2 (update c4 - add function which return three list)

def c4_function_a(driverId,query_engine=engine):
    query = '''
            WITH driver_points(driverId, year, all_points) AS(
                SELECT d.driverId, ra.year, SUM(re.points)
                FROM drivers d
                INNER JOIN results re ON d.driverId=re.driverId
                INNER JOIN races ra ON re.raceId=ra.raceId
                WHERE d.driverId = {did}
                GROUP BY d.driverId, ra.year
                ORDER BY d.driverId, ra.year
            ), driver_error(driverId, year, crash) AS(
                SELECT d.driverId, ra.year, COUNT(re.statusId) as crash
                FROM drivers d
                INNER JOIN results re ON d.driverId=re.driverId
                INNER JOIN races ra ON re.raceId=ra.raceId
                WHERE statusId BETWEEN 3 and 4 AND d.driverId = {did}
                GROUP BY d.driverId, ra.year
                ORDER BY d.driverId, ra.year
            )
            SELECT DISTINCT dp.driverId, d.forename, d.surname, dp.year, 
                            dp.all_points as points, de.crash as crash ,
                            dp.all_points/de.crash as point_devide_by_crash,
                            de.crash/dp.all_points as crash_devide_by_point
            FROM driver_points dp
            INNER JOIN drivers d ON d.driverId = dp.driverId
            INNER JOIN driver_error de ON dp.driverId=de.driverId AND dp.year=de.year
            WHERE dp.all_points>0
            ORDER BY dp.driverId, dp.Year
            '''.format(did=driverId)
    data = pd.read_sql(query, query_engine)
    detail_record_of_any_risky_drivers = data.to_json(orient="table")

    return detail_record_of_any_risky_drivers

detail_record_of_any_risky_drivers = c4_function_a(2)

<<<<<<< HEAD
# print(detail_record_of_any_risky_drivers)
print(top_ten_risky_drivers)
=======
#print(detail_record_of_any_risky_drivers)

>>>>>>> cab04a2 (update c4 - add function which return three list)

# https://www.oracletutorial.com/python-oracle/connecting-to-oracle-database-in-python/
# https://oracle.github.io/python-cx_Oracle/
# https://stackoverflow.com/questions/55823744/how-to-fix-cx-oracle-databaseerror-dpi-1047-cannot-locate-a-64-bit-oracle-cli
