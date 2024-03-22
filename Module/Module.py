from DatabaseHandler import DataBaseHandler
import pandas as pd
import os
import sqlite3 as sql

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
                TempText = f"{Column},0\n"
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


def CreateTrackTable(DataBaseOp, DWH, metadata=[]):
    path="OP.DB"
    DataBaseOp.cur.execute(f'ATTACH DATABASE "{path}" AS source')
    Table1 = "source.Track LEFT JOIN source.Genre ON source.Track.GenreId = source.Genre.GenreId"
    Table2 = "LEFT JOIN source.MediaType ON source.Track.MediaTypeId = source.MediaType.MediaTypeId"
    Table3 = "LEFT JOIN source.Album ON source.Album.AlbumId = source.Track.AlbumId"
    Table4 = "LEFT JOIN source.Artist ON source.Album.ArtistId = source.Artist.ArtistId"
    request = f"""
    SELECT Track.TrackId, Track.Name, Track.Composer, Track.Milliseconds, Track.Bytes, Track.UnitPrice, MediaType.Name, Genre.Name, Album.Title, Artist.Name
    FROM ((({Table1}) {Table2}){Table3}){Table4};
    """
    
    """
    for Criteria in metadata:
        if Criteria.get("Historique") == '1':
            SCD2(DataBaseWH,
                 "track_dim",
                 Criteria.get("ColumnName"))
    """

    data=DataBaseOp._RetrieveData(request)
    
    ###Implémentation de la dimension INVOICE ####
    
    
    DWH.cur.execute('ATTACH DATABASE DWH.DB AS source')
    request='select * from track_dim'
    data=DataBaseOp._RetrieveData(request)
    
    
    return data
    
def CreateInvoiceDim(DataBaseOp,DWH, metadata=[]):
    
    ###Récupération des données de la base OP###
    request="""SELECT invoiceid,
    billingaddress,
    billingcity,
    billingstate,
    billingcountry,
    billingpostalcode,
    Total 
    FROM Invoice;"""
    
    """
    for Criteria in metadata:
        if Criteria.get("Historique") == '1':
            SCD2(DataBaseWH,
                 "invoice_dim",
                 Criteria.get("ColumnName"))
    """
    data=DataBaseOp._RetrieveData(request)
    ###Implémentation de la dimension INVOICE ####
    
    Headers= DWH.GetColumnFromTable('invoice_dim')
    DWH.InsertWithSCD2('invoice_dim', data, Headers, IdsColumnsName= ['invoice_id'])
       
    
    return data
    

def CreateCustomerDim(DataBaseOp, DWH,  metadata=[]):
    path="OP.DB"
    DataBaseOp.cur.execute(f'ATTACH DATABASE "{path}" AS source')
    request="""SELECT CustomerId,
    FirstName,
    LastName,
    Company,
    Address,
    City,
    State,
    Country,
    PostalCode,
    Phone,
    Fax,
    Email,
    SupportRepId
    FROM Customer;"""
    
    """
    for Criteria in metadata:
        if Criteria.get("Historique") == '1':
            SCD2(DataBaseWH,
                 "invoice_dim",
                 Criteria.get("ColumnName"))
    """
    data=DataBaseOp._RetrieveData(request)
    
    ###implementation customer
    
    for d in data : 
        DWH.cur.execute("INSERT INTO customer_dim (customer_id, first_name, last_name, company, address, city, state, country, postal_code, phone, fax, email, support_rep_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", d)
    
    return data
            

def CreateEmployeDim(DataBaseOp, DWH, metadata=[]):
    path="OP.DB"
    DataBaseOp.cur.execute(f'ATTACH DATABASE "{path}" AS source')
    coltemp=path.GetColumnFromTable("Employee")
    col=[]
    for c in coltemp:
        col.append(c[0])
        
    columns = ', '.join(col)  # Concaténation des noms de colonnes

    request = f"SELECT {columns} FROM Employee;"
    
    """
    for Criteria in metadata:
        if Criteria.get("Historique") == '1':
            SCD2(DataBaseWH,
                 "invoice_dim",
                 Criteria.get("ColumnName"))
    """
    data=DataBaseOp._RetrieveData(request)
    
    ###implementation Employe
    
    DWH.cur.execute(f'ATTACH DATABASE DWH.DB AS source')
    
    coltemp=path.GetColumnFromTable("Employee")
    col=[]
    for c in coltemp:
        col.append(c[0])
    
    

    































