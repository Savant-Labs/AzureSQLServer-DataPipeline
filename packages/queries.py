RemoveDuplicates = '''
    WITH CTE AS (
        SELECT *,
        ROW_NUMBER() 

        OVER(
            PARTITION BY CAST(Record AS NVARCHAR(MAX)) 
            ORDER BY Shipped DESC
        ) 

        AS RowNum
        
        FROM dbo.ArrowStream
    )

    DELETE FROM CTE WHERE RowNum > 1;
'''

SelectAll = '''
    SELECT * FROM dbo.ArrowStream;
'''

ClearTable = '''
    DELETE FROM dbo.ArrowStream;
'''

ImportRecord = '''
    INSERT INTO dbo.ArrowStream
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''