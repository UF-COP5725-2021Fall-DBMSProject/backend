###
# c3-function
###
import json
import sys
pwd = sys.argv[1]

import pandas as pd
from sqlalchemy import create_engine

import engine as eg

engine = eg.engine_gen(pwd)

def c3_function_get_top_10_defender_for_teammate(query_engine=engine):

    query = '''
            WITH driver_lap(raceId, lap, driverId, position) AS(
                SELECT raceId, lap, d.driverId, position
                FROM drivers d
                INNER JOIN lapTimes l ON d.driverId=l.driverId
            ), cartesian_product_driver_lap(raceId, lap, driverId_1, position_1, driverId_2, position_2) AS(
                SELECT d1.raceId, d1.lap, d1.driverId, d1.position, d2.driverId, d2.position
                FROM driver_lap d1, driver_lap d2
                WHERE d1.raceId=d2.raceId
                AND d1.raceId=d2.raceId
                AND d1.lap=d2.lap 
                AND d1.driverId<>d2.driverId
                AND d1.position < d2.position
            ), cartesian_product_same_team_driver_lap(raceId, lap, driverId_1, position_1, driverId_2, position_2) AS(
                SELECT c.raceId, c.lap, c.driverId_1, c.position_1, c.driverId_2, c.position_2
                FROM cartesian_product_driver_lap c
                INNER JOIN results r1 ON c.raceId=r1.raceId AND c.driverId_1=r1.driverId
                INNER JOIN results r2 ON c.raceId=r2.raceId AND c.driverId_2=r2.driverId
                WHERE r1.constructorId=r2.constructorId
                -- WHERE c.position_1 < c.position_2, already exist
            ), cartesian_product_driver_lap_add_same_team_advanced_driver
                            (raceId, lap, defender_driverId, defender_position, 
                                          victim_driverId, victim_position, 
                                          teammate_driverId, teammate_position) AS(
                SELECT c1.raceId, c1.lap, c1.driverId_1, c1.position_1, c1.driverId_2, c1.position_2, c_with_teammate.driverId_1, c_with_teammate.position_1
                FROM cartesian_product_driver_lap c1
                INNER JOIN cartesian_product_same_team_driver_lap c_with_teammate 
                        ON c1.raceId=c_with_teammate.raceId 
                           AND c1.lap=c_with_teammate.lap
                           AND c1.driverId_1 = c_with_teammate.driverId_2
            ), defender_records(raceId, startlap, defender_driverId, victim_driverId, teammate_driverId) AS(
                SELECT c1.raceId, c1.lap, c1.defender_driverId, c1.victim_driverId, c1.teammate_driverId   --COUNT(DISTINCT c1.driverId_1)
                FROM cartesian_product_driver_lap_add_same_team_advanced_driver c1
                WHERE c1.victim_position-c1.defender_position BETWEEN 1 AND 2
                AND EXISTS(
                    SELECT *
                    FROM cartesian_product_driver_lap_add_same_team_advanced_driver c2
                    WHERE c1.raceId=c2.raceId
                    AND c1.lap=c2.lap-1
                    AND c1.defender_driverId=c2.defender_driverId
                    AND c1.victim_driverId=c2.victim_driverId
                    AND c2.victim_position-c2.defender_position BETWEEN 1 AND 2)
                AND EXISTS(
                    SELECT *
                    FROM cartesian_product_driver_lap_add_same_team_advanced_driver c3
                    WHERE c1.raceId=c3.raceId
                    AND c1.lap=c3.lap-2
                    AND c1.defender_driverId=c3.defender_driverId
                    AND c1.victim_driverId=c3.victim_driverId
                    AND c3.victim_position-c3.defender_position BETWEEN 1 AND 2)
                AND EXISTS(
                    SELECT *
                    FROM cartesian_product_driver_lap_add_same_team_advanced_driver c4
                    WHERE c1.raceId=c4.raceId
                    AND c1.lap=c4.lap-3
                    AND c1.defender_driverId=c4.defender_driverId
                    AND c1.victim_driverId=c4.victim_driverId
                    AND c4.victim_position-c4.defender_position BETWEEN 1 AND 2)
                AND EXISTS(
                    SELECT *
                    FROM cartesian_product_driver_lap_add_same_team_advanced_driver c5
                    WHERE c1.raceId=c5.raceId
                    AND c1.lap=c5.lap-4
                    AND c1.defender_driverId=c5.defender_driverId
                    AND c1.victim_driverId=c5.victim_driverId
                    AND c5.victim_position-c5.defender_position BETWEEN 1 AND 2)
                ORDER BY c1.raceId, c1.lap, c1.defender_driverId, c1.victim_driverId ASC
            )
            SELECT defender_driverId, COUNT(startlap) as defend_point
            FROM defender_records dr
            GROUP BY defender_driverId
            ORDER BY COUNT(startlap) DESC
            FETCH FIRST 10 ROWS ONLY
            '''
    data = pd.read_sql(query, query_engine)
    top_10_defender_for_teammate = data.to_json(orient="table")

    return top_10_defender_for_teammate

top_10_defender_for_teammate = c3_function_get_top_10_defender_for_teammate()



def c3_function_get_defender_best_10_records(defender_driverId, query_engine=engine):

    query = '''
            WITH driver_lap(raceId, lap, driverId, position) AS(
                SELECT raceId, lap, d.driverId, position
                FROM drivers d
                INNER JOIN lapTimes l ON d.driverId=l.driverId
            ), cartesian_product_driver_lap(raceId, lap, driverId_1, position_1, driverId_2, position_2) AS(
                SELECT d1.raceId, d1.lap, d1.driverId, d1.position, d2.driverId, d2.position
                FROM driver_lap d1, driver_lap d2
                WHERE d1.raceId=d2.raceId
                AND d1.raceId=d2.raceId
                AND d1.lap=d2.lap 
                AND d1.driverId<>d2.driverId
                AND d1.position < d2.position
            ), cartesian_product_same_team_driver_lap(raceId, lap, driverId_1, position_1, driverId_2, position_2) AS(
                SELECT c.raceId, c.lap, c.driverId_1, c.position_1, c.driverId_2, c.position_2
                FROM cartesian_product_driver_lap c
                INNER JOIN results r1 ON c.raceId=r1.raceId AND c.driverId_1=r1.driverId
                INNER JOIN results r2 ON c.raceId=r2.raceId AND c.driverId_2=r2.driverId
                WHERE r1.constructorId=r2.constructorId
                -- WHERE c.position_1 < c.position_2, already exist
            ), cartesian_product_driver_lap_add_same_team_advanced_driver
                            (raceId, lap, defender_driverId, defender_position, 
                                          victim_driverId, victim_position, 
                                          teammate_driverId, teammate_position) AS(
                SELECT c1.raceId, c1.lap, c1.driverId_1, c1.position_1, c1.driverId_2, c1.position_2, c_with_teammate.driverId_1, c_with_teammate.position_1
                FROM cartesian_product_driver_lap c1
                INNER JOIN cartesian_product_same_team_driver_lap c_with_teammate 
                        ON c1.raceId=c_with_teammate.raceId 
                           AND c1.lap=c_with_teammate.lap
                           AND c1.driverId_1 = c_with_teammate.driverId_2
                WHERE c1.driverId_1={df_did}
            ), defend_record(raceId, start_lap, defender_driverId, defender_position,
                                                victim_driverId, victim_position,
                                                teammate_driverId, teammate_position) AS (
            SELECT c1.raceId, c1.lap as start_lap, c1.defender_driverId, c1.defender_position, 
                                                    c1.victim_driverId, c1.victim_position,
                                                     c1.teammate_driverId, c1.teammate_position
            FROM cartesian_product_driver_lap_add_same_team_advanced_driver c1
            WHERE c1.victim_position-c1.defender_position BETWEEN 1 AND 2
            AND EXISTS(
                SELECT *
                FROM cartesian_product_driver_lap_add_same_team_advanced_driver c2
                WHERE c1.raceId=c2.raceId
                AND c1.lap=c2.lap-1
                AND c1.defender_driverId=c2.defender_driverId
                AND c1.victim_driverId=c2.victim_driverId
                AND c2.victim_position-c2.defender_position BETWEEN 1 AND 2)
            AND EXISTS(
                SELECT *
                FROM cartesian_product_driver_lap_add_same_team_advanced_driver c3
                WHERE c1.raceId=c3.raceId
                AND c1.lap=c3.lap-2
                AND c1.defender_driverId=c3.defender_driverId
                AND c1.victim_driverId=c3.victim_driverId
                AND c3.victim_position-c3.defender_position BETWEEN 1 AND 2)
            AND EXISTS(
                SELECT *
                FROM cartesian_product_driver_lap_add_same_team_advanced_driver c4
                WHERE c1.raceId=c4.raceId
                AND c1.lap=c4.lap-3
                AND c1.defender_driverId=c4.defender_driverId
                AND c1.victim_driverId=c4.victim_driverId
                AND c4.victim_position-c4.defender_position BETWEEN 1 AND 2)
            AND EXISTS(
                SELECT *
                FROM cartesian_product_driver_lap_add_same_team_advanced_driver c5
                WHERE c1.raceId=c5.raceId
                AND c1.lap=c5.lap-4
                AND c1.defender_driverId=c5.defender_driverId
                AND c1.victim_driverId=c5.victim_driverId
                AND c5.victim_position-c5.defender_position BETWEEN 1 AND 2)
            ORDER BY c1.raceId, c1.lap, c1.defender_driverId, c1.victim_driverId ASC
            )
            SELECT raceId, defender_driverId, victim_driverId,teammate_driverId, COUNT(start_lap) as defend_points
            FROM defend_record
            GROUP BY raceId, defender_driverId, victim_driverId,teammate_driverId
            ORDER BY COUNT(start_lap) DESC
            FETCH FIRST 10 ROWS ONLY
            '''.format(df_did=defender_driverId)
    data = pd.read_sql(query, query_engine)
    defender_best_10_records = data.to_json(orient="table")

    return defender_best_10_records

defender_driverId=13
defender_best_10_records = c3_function_get_defender_best_10_records(defender_driverId)
#print(defender_best_10_records)

def c3_function_get_defender_record_detail(raceId, defender_driverId, victim_driverId, teammate_driverId, query_engine=engine):

    query = '''
            WITH Race_lap(raceId, lap, driverId, position) AS(
                SELECT raceId, lap, driverId, position
                FROM lapTimes l
                WHERE l.raceId={rid}
            ), their_record(raceId, lap, defender_driverId, defender_position,
                                victim_driverId, victim_position,
                                teammate_driverId, teammate_position) AS(
                SELECT r1.raceId, r1.lap, r1.driverId, r1.position,
                                          r2.driverId, r2.position,
                                          r3.driverId, r3.position
                FROM Race_lap r1, Race_lap r2, Race_lap r3
                WHERE r1.driverId={df.did}
                AND r2.driverId={vt_did}
                AND r3.driverId={tm_did}
                AND r1.lap=r2.lap
                AND r1.lap=r3.lap
                ORDER BY r1.lap
            )
            SELECT r.raceId as raceId, ra.name as race_name, 
                   def.driverId as defender_driverId, def.forename as defender_forename, def.surname as defender_surname, r.defender_position as defender_position,
                   vic.driverId as victim_driverId, vic.forename  as victim_forename, vic.surname as victim_surname, r.victim_position as victim_position,
                   tmt.driverId as teammate_driverId, tmt.forename as teammmate_forename, tmt.surname as teammate_surname, r.teammate_position as teammate_position
            FROM their_record r
            INNER JOIN drivers def ON r.defender_driverId=def.driverId
            INNER JOIN drivers vic ON r.victim_driverId=vic.driverId
            INNER JOIN drivers tmt ON r.teammate_driverId=tmt.driverId
            INNER JOIN races ra ON r.raceId=ra.raceId
            
            '''.format(rid=raceId, df_did=defender_driverId,vt_did=victim_driverId, tm_did=teammate_driverId)
    data = pd.read_sql(query, query_engine)
    defender_best_10_records = data.to_json(orient="table")

    return defender_best_10_records

defender_driverId=2
defender_best_10_records = c3_function_get_defender_best_10_records(defender_driverId)
#print(defender_best_10_records)


# https://www.oracletutorial.com/python-oracle/connecting-to-oracle-database-in-python/
# https://oracle.github.io/python-cx_Oracle/
# https://stackoverflow.com/questions/55823744/how-to-fix-cx-oracle-databaseerror-dpi-1047-cannot-locate-a-64-bit-oracle-cli
