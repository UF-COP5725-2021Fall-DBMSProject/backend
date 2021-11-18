#import sys
#pwd = sys.argv[1]

from sqlalchemy import create_engine

def engine_gen(pwd):
    oracle_connection_string = 'oracle+cx_oracle://{username}:{password}@{hostname}:{port}/{database}'

    engine = create_engine(
        oracle_connection_string.format(
            username='huang.c1',
            password='{}'.format(pwd),
            hostname='oracle.cise.ufl.edu',
            port='1521',
            database='orcl',
        )
    )
    return engine

