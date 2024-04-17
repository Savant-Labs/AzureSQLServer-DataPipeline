# **Power BI Data Validation Tool**


### **Intended Goals**
This project uploads data from .CSV files into our Azure SQL Server database at the click of a button - seriously, it's just a batch script!

### **Where's the Data?**
This project collects and analyzes data from two individual sources:
- Azure SQL Server (sales reports to be reviewed)
- A Local .CSV Export File (new records to upload)

```
├── Project
│   ├── Exports
|   |   ├── Week Ending mm-dd-yyyy.csv
│   ├── Pipeline
|   |   ├── packages
|   |   |   ├── logfiles
|   |   |   |   ├── events
|   |   |   |   ├── errors
|   |   |   ├── data.py
|   |   |   ├── types.py
|   |   |   ├── logger.py
|   |   |   ├── queries.py
|   |   |   ├── database.py
|   |   ├── .env
|   |   ├── main.py
|   |   ├── .gitignore
```

### The Process
This program follows 5 simple steps to prevent against SQL injections and malformed data.
1. Load and Sanitize CSV Export
    - Prompt for User Input to locate correct file in `/Projects/Exports/` directory
    - Load file data into DataFrame
    - Map CSV Column Headers to Database Column Names
    - Convert CSV Column Types to Database Column Types
    - Create Calculated Column `Record` to Identify Duplicate Rows

2. Load SQL Server Data
3. Vertical Join Sanitized Data
4. Re-sanitize Joined Data
    - Ensure all Database columns are present in DataFrame
    - Convert DataFrame columns types to Database Column Types
5. Upload Sanitized Data
    - Delete all records in Azure Database (optional)
    - Upload all records from Sanitized DataFrame

We can now connect our Azure SQL Server database to Power BI and other scripts to help validate and display this data.
