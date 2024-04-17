import os
import sys
import pyodbc
import sqlalchemy

import pandas as pd

from tqdm import tqdm
from dotenv import load_dotenv

from sqlalchemy import engine

from .queries import *
from .types import CustomDict
from .data import Sanitization
from .logger import CustomLogger as log


class Connection():

    _structure: dict = Sanitization._structure
    structure: CustomDict = Sanitization.structure


    def __init__(self):
        self.env: CustomDict = None

        self.cursor: pyodbc.Cursor = None
        self.connection: pyodbc.Connection = None

        self._initialize()
    
    @staticmethod
    def _loadEnv() -> CustomDict:
        load_dotenv()

        keys = [
            'AzureServerIP',
            'AzureDatabase',
            'AzureUsername',
            'AzurePassword',
            'AzureDBSchema',
            'DatabaseTable'
        ]

        vars = {}

        for key in keys:
            log.trace(f'Retrieving Environment Variable: {key}')

            try:
                value = os.getenv(key)
            except KeyError:
                log.issue(f'Unable to locate environment variable: {key}')
            else:
                vars.update({key: value})

        if any([key not in vars.keys() for key in keys]):
            log.fatal('Missing Required Environment Variables - Exiting...')
            
            return sys.exit()

        else:
            return CustomDict(vars)
    
    @staticmethod
    def _getConnectionString(system: CustomDict) -> str:
        log.trace('Generating SQL Server Connection String...')

        dsn = f'''
            DRIVER={{ODBC Driver 18 for SQL Server}};
            SERVER={system.AzureServerIP};
            DATABASE={system.AzureDatabase};
            UID={system.AzureUsername};
            PWD={system.AzurePassword};
            TrustServerCertificate=yes;
        '''

        return dsn
    
    @staticmethod
    def getEngine(dsn: str):
        url = engine.URL.create("mssql+pyodbc", query={"odbc_connect": dsn})
        _engine = sqlalchemy.create_engine(url)

        return _engine

    @staticmethod
    def connect(dsn: str) -> pyodbc.Connection:
        log.debug('Attempting to Authenticate with SQL Server...')

        try:
            connection = pyodbc.connect(dsn)
            log.state('Connected to SQL Server')

        except Exception as e:
            log.error(f'Connection Request Failed: {e}')
            connection = None

        finally:
            if connection:
                return connection
            else:
                log.fatal('Failed to Open a Connection: Server Not Found')

        return sys.exit()     
            
    def _initialize(self) -> None:
        env = self._loadEnv()
        self.env = env

        dsn = self._getConnectionString(env)
        conn = self.connect(dsn)

        self.connection = conn
        self.cursor = conn.cursor()

        self.engine = self.getEngine(dsn)
    
    def read(self, query: str = SelectAll) -> pd.DataFrame:
        log.trace(f'Exporting SQL Server to DataFrame...')

        data = pd.read_sql(query, self.connection)

        return data

    def write(self, data: pd.DataFrame, overwrite: bool = False) -> None:
        data = Sanitization.clean(data)

        if overwrite:
            log.debug('Preparing to Overwrite All Data...')
            self.connection.execute(ClearTable)

        else:
            log.debug('Preparing to Append New Data...')

        def get_chunks(df: pd.DataFrame, size: int):
            return (
                df[pos : pos + size]
                for pos in range(0, len(df), size)
            )
        
        chunk_size = 1000

        log.state('Writing Data...')
        print()
        
        kwargs = {
            'ascii': ' ❙',
            'bar_format': '{desc}: {percentage:.1f}% | {bar} | {n_fmt}/{total_fmt} [{elapsed}<{remaining}] ({rate_fmt}{postfix})',
            'desc': 'Writing DataFrame to SQL Server',
            'unit': ' rows',
            'colour': '#00FF00'
        }

        with tqdm(total=len(data), **kwargs) as pbar:
            
            for i, chunk in enumerate(get_chunks(data, chunk_size)):
                chunk.to_sql(
                    self.env.DatabaseTable,
                    self.engine,
                    schema = self.env.AzureDBSchema,
                    index = False,
                    dtype = self._structure,
                    if_exists = 'append'
                )

                pbar.update(chunk_size)
        
        log.state('Removing Duplicate Records...')
        count = self.execute(RemoveDuplicates)
        # log.debug(f'Deleted {count} records')

        self.connection.commit()
    
    def execute(self, query: str, *, args: tuple = None) -> list:
        if not args:
            result = self.cursor.execute(query)
        else:
            result = self.cursor.execute(query, args)

        return result
