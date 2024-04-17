import warnings

import pandas as pd

from packages import data
from packages import database

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
    def importSQLdata(connection: database.Connection) -> list:
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

        return df

    @classmethod
    def execute(cls):
        db = cls.connect()
        data = cls.importSQLdata(db)
        report = cls.readCSV()

        log.state('Adding Report Data...')
        final = cls.merge(left=data, right=report)

        
        log.state('Preparing to Upload Data...')
        db.write(final, overwrite=True)

        log.debug(f'Uploaded {final.shape[0] - data.shape[0]} rows')

        return

if __name__ == '__main__':
    ControlFlow.execute()

    

    




