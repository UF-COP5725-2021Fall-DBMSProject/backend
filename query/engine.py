#import sys
#pwd = sys.argv[1]

from sqlalchemy import create_engine

def engine_gen(pwd):
    oracle_connection_string = 'oracle+cx_oracle://{username}:{password}@{hostname}:{port}/{database}'

    engine = create_engine(
        oracle_connection_string.format(
            username='admin',
            password='{}'.format(pwd),
            hostname='ufl-dbms-oracle.cd9u1miwbskg.us-east-1.rds.amazonaws.com',
            port='1521',
            database='orcl',
        )
    )
    return engine

