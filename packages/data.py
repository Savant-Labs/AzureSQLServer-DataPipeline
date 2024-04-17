import sys
import numpy as np
import pandas as pd

from pathlib import Path
from os.path import abspath

from sqlalchemy.dialects import mssql as sql

from .types import CustomDict
from .logger import CustomLogger as log

class Sanitization():

    _structure: dict = {
        'Record': sql.TEXT,                     # Account.Invoice.Item
        'Invoice': sql.TEXT,                    # Invoice Number
        'ShipDate': sql.DATE,                   # Invoice Date
        'Account': sql.VARCHAR(length=16),      # Store Number
        'Customer': sql.VARCHAR(length=32),     # DC Account Number
        'Item': sql.VARCHAR(length=32),         # DC Item Number
        'Product': sql.VARCHAR(length=32),      # Brand Item Number
        'Description': sql.TEXT,                # Brand Item Description
        'Ordered': sql.SMALLINT,                # Quantity Ordered
        'Shipped': sql.SMALLINT,                # Quantity Shipped
        'UnitPrice': sql.FLOAT(precision=2),    # Unit Price (To Operator)
        'UnitCost': sql.FLOAT(precision=2),     # Unit Cost (To Distributor)
        'ExtPrice': sql.FLOAT(precision=2),     # Total Price (To Operator)
        'ExtCost': sql.FLOAT(precision=2),      # Total Cost (To Distributor)
    }

    structure = CustomDict(_structure)

    @classmethod
    def clean(cls, data: pd.DataFrame, *, record: bool = True) -> pd.DataFrame:
        log.debug('Sanitizing Data...')

        def convertPrice(x):
            if str(x) == 'nan':
                return np.NaN
            else:
                x = str(x)
            
            x = x.replace('$', '')
            x = x.replace('(', '-')
            x = x.replace(')', '')
            x = x.replace(',', '')

            x = x.split()[0]

            x = float(x)

            return round(x, 2)

        def convertQuantity(x):
            if str(x) == 'nan':
                return np.NaN
            else:
                x = str(x)

            x = x.replace(',', '')
            x = x.replace('(', '-')
            x = x.replace(')', '')
            x = x.split('.')[0]
            x = x.split()[0]

            return int(x)
        
        def convertString(x):
            x = str(x).split('.')[0]
            
            return str(x)

        log.trace('Asserting Column Reference Names...')
        missing = []
        for name in cls._structure.keys():
            try:
                assert name in list(data.columns)
            except AssertionError:
                log.issue(f'Dataset does not include column: [{name}]')
                missing.append(name)
        
        if len(missing) == 1:
            if missing[0] == 'Record' and not record:
                log.debug('Missing Column: [Record] will be indexed after sanitation')
            else:
                try:
                    match = [col for col in list(data.columns) if col not in cls._structure.keys()][0]
                except IndexError:
                    log.fatal('Unable to resolve missing column reference - Exiting...')

                    return sys.exit()
                
                else:
                    data.rename(columns={match: missing[0]})
        elif len(missing) > 1:
            log.fatal('Unable to resolve missing column references - Exiting...')

            return sys.exit()
        
        log.debug('Ensuring Correct Data Types...')
        
        for col in ['Invoice', 'Customer', 'Item', 'Product']:
            log.trace(f'Converting {col} to <sql.TEXT> values...')
            data[col] = data[col].apply(lambda x: convertString(x))
        
        for col in ['Ordered', 'Shipped']:
            log.trace(f'Converting {col} to <sql.SMALLINT> values...')
            data[col] = data[col].apply(lambda x: convertQuantity(x))

        for col in ['UnitPrice', 'UnitCost', 'ExtPrice', 'ExtCost']:
            log.trace(f'Converting {col} to <sql.FLOAT(2)> values...')
            data[col] = data[col].apply(lambda x: convertPrice(x))

        return data


class ExecutionHandler():
    
    report: pd.DataFrame = None
    
    directory: str = abspath(__file__).replace('bin\\packages\\data.py', 'Exports\\Week Ending')

    @staticmethod
    def loadReport(path: str) -> pd.DataFrame:
        columnMap = {
            'Invoice #': 'Invoice',
            'Invoice Date': 'ShipDate',
            'Store': 'Account',
            'Customer #': 'Customer',
            'DC Item #': 'Item',
            'Brand Item #': 'Product',
            'Brand Item Description': 'Description',
            'Qty Ordered': 'Ordered',
            'Qty Shipped': 'Shipped',
            'Average Unit Price': 'UnitPrice',
            'Average Unit Cost': 'UnitCost',
            'Ext Price': 'ExtPrice',
            'Ext Cost': 'ExtCost'
        }

        log.trace('Loading CSV File...')
        df = pd.read_csv(path)

        log.trace('Renaming Column References...')
        df = df.rename(columns=columnMap)
        df = Sanitization.clean(df, record=False)

        log.trace('Calculating [Record] values...')
        df.insert(0, 'Record', None)
        df['Record'] = df['Record'].astype(str)
        df['Record'] = df.apply(lambda row: f'{row["Account"]}.{row["Invoice"]}.{row["Item"]}', axis=1)

        print()
        print(df.head())
        print()

        return df
    
    @classmethod
    def _getUserInput(cls):
        log.state('Waiting for User Input...')
        
        date = None

        while date is None:
            print()
            date = input('Please Enter the Report Date as mm-dd-yyyy: ')
            print()

            path = Path(cls.directory + ' ' + date + '.csv')

            try:
                assert path.exists()

            except AssertionError:
                log.issue(f'File Not Found: Exports\\Week Ending {date}.csv - Please try again...')
                date = None

            else:
                log.debug(f'Located Report File: Exports\\Week Ending {date}.csv')

        return path
    
    @classmethod
    def run(cls):
        path = cls._getUserInput()
        data = cls.loadReport(path)

        return data






                  


        


