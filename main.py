import warnings
import argparse
import duckdb

import pandas as pd

from packages import data
from packages import database

from packages.queries import *
from packages.logger import setup as setup_logs
from packages.logger import CustomLogger as log

setup_logs()

warnings.simplefilter('ignore', UserWarning)
warnings.simplefilter('ignore', FutureWarning)
warnings.simplefilter('ignore', pd.errors.DtypeWarning)


class ControlFlow():
    
    @staticmethod
    def connect():
        connection = database.Connection()

        return connection

    @staticmethod
    def importSQL(connection: database.Connection) -> list:
        data = connection.read()

        return data

    @staticmethod
    def readCSV():
        report = data.ExecutionHandler.run()
        
        return report
    
    @staticmethod
    def merge(*, left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
        left = left.rename(columns={'id': 'index'})
        right = right.rename(columns={'id': 'index'})

        df = pd.concat(
            [left, right],
            ignore_index = True,
            axis = 0
        )

        condensed = df.copy(deep=True)

        condensed.sort_values(by="Shipped", ascending=False, inplace=True)
        condensed.drop_duplicates(subset="Record", keep="first", inplace=True)
        log.debug(f'Removed {df.shape[0] - condensed.shape[0]} duplicate row entires')

        return condensed
    
    @classmethod
    def append(cls):
        database = cls.connect()
        report = cls.readCSV()

        log.state('Preparing to Upload Data...')
        database.write(report)
        log.debug(f'Uploaded {report.shape[0]} rows')

        return

    @classmethod
    def overwrite(cls):
        database = cls.connect()
        data = cls.importSQL(database)

        report = cls.readCSV()

        log.state('Merging Datasets...')
        merged = cls.merge(left=data, right=report)

        log.state('Preparing to Upload...')
        database.write(merged, overwrite=True)

        log.debug(f'Uploaded {merged.shape[0]} rows')

        return

    @classmethod
    def execute(cls, method: str = 'append'):
        if method == 'append':
            cls.append()
        
        else:
            cls.overwrite()

            

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--overwrite', action='store_true', help = 'Overwrite Database with Records')
    args = parser.parse_args()

    if args.overwrite:
        ControlFlow.execute(method='overwrite')
    else:
        ControlFlow.execute() 



    

    




