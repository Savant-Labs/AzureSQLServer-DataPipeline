import os
from dotenv import load_dotenv

load_dotenv()

schema = os.getenv('AzureDBSchema')
table = os.getenv('DatabaseTable')

RemoveDuplicates = f'''
    WITH CTE AS (
        SELECT *,
        ROW_NUMBER() 

        OVER(
            PARTITION BY CAST(Record AS NVARCHAR(MAX)) 
            ORDER BY Shipped DESC
        ) 

        AS RowNum
        
        FROM {schema}.{table}
    )

    DELETE FROM CTE WHERE RowNum > 1;
'''

SelectAll = f'''
    SELECT * FROM {schema}.{table};
'''

ClearTable = f'''
    DELETE FROM d{schema}.{table};
'''

ImportRecord = f'''
    INSERT INTO {schema}.{table}
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''