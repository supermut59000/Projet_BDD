from DatabaseHandler import DataBaseHandler
import pandas as pd
import os

PATH = os.getcwd()

def SCD2(DatabaseObject, TableName: str, ColumnName: str, Columntype= "VARCHAR(50)"):
    ColomnNames = ["StartTime","EndTime","IsChanged"]
    for Colomn in ColomnNames:
        DatabaseObject.AlterTable(TableName, ColumnName + Colomn, Columntype)
    
    return 

def CreateMetadata(DatabaseObject):
    if "Metadata" not in os.listdir(PATH):
        os.mkdir(PATH+ "/Metadata/")
        
    request = "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name;"
    Tables = DatabaseObject._RetrieveData(request)
    
    for Table in Tables:
        with open(PATH+ "/Metadata/"+ Table[0]+".txt", "w") as writer:
            writer.write("ColumnName,Historique\n")
            for Column in DatabaseObject.GetColumnFromTable(Table[0]):
                TempText = f"{Column[0]},0\n"
                writer.write(TempText)

def ReadMetadata():
    FileNames = os.listdir(PATH+ "/Metadata/")
    Metadatas = []
    for FileName in FileNames:
        fd = open(PATH+ "/Metadata/"+ FileName, 'r', encoding="utf8")
        sqlFile = [x.split(',') for x in fd.read().split("\n") if x != '']
        fd.close()
        Metadata = [{sqlFile[0][0]: x[0], sqlFile[0][1]: x[1]} for x in sqlFile[1:]]
        Metadatas.append(Metadata)
        
    return Metadatas, FileNames

def create_date_table(start='1990-01-01', end='2099-12-31'):
    df = pd.DataFrame({'Date_D': pd.date_range(start, end)})
    df['date_id'] = df.index + 1
    df['year_D'] = df.Date_D.dt.year
    df['month_D'] = df.Date_D.dt.month
    df['MonthNumberOfDay'] = df.Date_D.dt.daysinmonth
    df['day_D'] = df.Date_D.dt.day
    df['day_name'] = df.Date_D.dt.day_name()
    df['day_week'] = df.Date_D.dt.dayofweek
    df['week'] = df.Date_D.dt.isocalendar().week
    df['quarter'] = df.Date_D.dt.quarter
    
    df = df[['date_id', 'Date_D', 'year_D', 'month_D', 'MonthNumberOfDay', 'day_D', 'day_name', 'day_week', 'week', 'quarter']] 
    
    return df


def CreateTrackTable(DatabaseObject, metadata=[]):
    Table1 = "Track LEFT JOIN Genre ON Track.GenreId = Genre.GenreId"
    Table2 = "LEFT JOIN MediaType ON Track.MediaTypeId = MediaType.MediaTypeId"
    Table3 = "LEFT JOIN Album ON Album.AlbumId = Track.AlbumId"
    Table4 = "LEFT JOIN Artist ON Album.ArtistId = Artist.ArtistId"
    request = f"""
    SELECT Track.TrackId, Track.Name, Track.Composer, Track.Milliseconds, Track.Bytes, Track.UnitPrice, MediaType.Name, Genre.Name, Album.Title, Artist.Name
    FROM ((({Table1}) {Table2}){Table3}){Table4};
    """
    for Criteria in metadata:
        if Criteria.get("Historique") == '1':
            SCD2(DatabaseObject,
                 "track_dim",
                 Criteria.get("ColumnName"))
            
            
            
            
    return 































