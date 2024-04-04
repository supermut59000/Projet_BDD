from DatabaseHandler import DataBaseHandler
import pandas as pd
import os
import sqlite3 as sql
import asyncio

PATH = os.getcwd()



def SCD2(DatabaseObject, TableName: str, Columntype= "VARCHAR(50)"):
    ColomnNames = ["StartTime","EndTime","IsActif"]
    for Colomn in ColomnNames:
        DatabaseObject.AlterTable(TableName, Colomn, Columntype)
    
    return 

def DoesSCD2Apply(TableName: str, metadata: list):
    DoesSCD2Apply= False
    for Table in metadata:
        if Table.get('TableName') == TableName and Table.get('Historique') == '1':
            DoesSCD2Apply= True
        
    return DoesSCD2Apply


def CreateMetadata(DatabaseObject):
    if "Metadata" not in os.listdir(PATH):
        os.mkdir(PATH+ "/Metadata/")
        
    request = "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name;"
    Tables = DatabaseObject._RetrieveData(request)

    with open(PATH+ "/Metadata/Metadata.txt", "w") as writer:
        writer.write("TableName,Historique\n")
        for Table in Tables:
            TempText = f"{Table[0]},0\n"
            writer.write(TempText)

def ReadMetadata():
    fd = open(PATH+ "//Metadata/Metadata.txt", 'r', encoding="utf8")
    sqlFile = [x.split(',') for x in fd.read().split("\n") if x != '']
    fd.close()
    Metadata = [{sqlFile[0][0]: x[0], sqlFile[0][1]: x[1]} for x in sqlFile[1:]]
        
    return Metadata


def create_date_table(DWH,start='1990-01-01', end='2099-12-31'):
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
    
    df.to_sql(name='date_dim', con=DWH.con, if_exists='replace')
    """
    for d in df : 
        DWH.cur.execute(INSERT INTO date_dim (
        date_id,
        Date_D,
        year_D,
        month_D,
        MonthNumberOfDay,
        day_D,
        day_name,
        day_week,
        week,
        quarter) VALUES (?,?,?,?,?,?,?,?,?,?), d)
        """
    return df



def CreateTrackTable(DataBaseOp, DWH, metadata=[]):
    TableName = 'track_dim'
    path="OP.DB"
    #DataBaseOp.cur.execute(f'ATTACH DATABASE "{path}" AS source')
    Table1 = "Track LEFT JOIN Genre ON Track.GenreId = Genre.GenreId"
    Table2 = "LEFT JOIN MediaType ON Track.MediaTypeId = MediaType.MediaTypeId"
    Table3 = "LEFT JOIN Album ON Album.AlbumId = Track.AlbumId"
    Table4 = "LEFT JOIN Artist ON Album.ArtistId = Artist.ArtistId"
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
    
    ###Implémentation de la dimension TRACK ####
    
    SCD2Apply = DoesSCD2Apply(TableName, metadata)
    if SCD2Apply:
        SCD2(DWH, TableName)
    Headers= DWH.GetColumnFromTable(TableName)
    DWH.InsertWithSCD2(TableName, data, Headers, ['track_id'], SCD2Apply)
    

    
def CreateInvoiceDim(DataBaseOp,DWH, metadata=[]):
    TableName = 'invoice_dim'
    ###Récupération des données de la base OP###
    path="OP.DB"
    #DataBaseOp.cur.execute(f'ATTACH DATABASE "{path}" AS source')
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
    
    SCD2Apply = DoesSCD2Apply(TableName, metadata)
    if SCD2Apply:
        SCD2(DWH, TableName)
    Headers= DWH.GetColumnFromTable(TableName)
    DWH.InsertWithSCD2(TableName, data, Headers, ['invoice_id'], SCD2Apply)
    
    return data
    

def CreateCustomerDim(DataBaseOp, DWH,  metadata=[]):
    TableName = 'customer_dim'
    path="OP.DB"
    #DataBaseOp.cur.execute(f'ATTACH DATABASE "{path}" AS source')
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
    
    SCD2Apply = DoesSCD2Apply(TableName, metadata)
    if SCD2Apply:
        SCD2(DWH, TableName)
    Headers= DWH.GetColumnFromTable(TableName)
    DWH.InsertWithSCD2(TableName, data, Headers, ['customer_id'], SCD2Apply)
    #for d in data : 
    #    DWH.cur.execute("INSERT INTO customer_dim (customer_id, first_name, last_name, company, address, city, state, country, postal_code, phone, fax, email, support_rep_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", d)
    
    return data
            


def CreateEmployeDim(DataBaseOp, DWH, metadata=[]):
    TableName = 'employe_dim'
    path="OP.DB"
    #DataBaseOp.cur.execute(f'ATTACH DATABASE "{path}" AS source')
    request="""SELECT EmployeeId,
    LastName,
    FirstName,
    Title,
    BirthDate,
    HireDate,
    Address,
    City,
    State,
    Country,
    PostalCode,
    Phone,
    Fax,
    Email
    FROM Employee;"""
    
    """
    for Criteria in metadata:
        if Criteria.get("Historique") == '1':
            SCD2(DataBaseWH,
                 "invoice_dim",
                 Criteria.get("ColumnName"))
    """
    data=DataBaseOp._RetrieveData(request)
    
    ###implementation employe_dim
    
    SCD2Apply = DoesSCD2Apply(TableName, metadata)
    if SCD2Apply:
        SCD2(DWH, TableName)
    Headers= DWH.GetColumnFromTable(TableName)
    DWH.InsertWithSCD2(TableName, data, Headers, ['EmployeeId'], SCD2Apply)
    

    return data
    



def CreateInvoiceFact(DataBaseOp, DWH, metadata=[]):
    path="OP.DB"
    #DataBaseOp.cur.execute(f'ATTACH DATABASE "{path}" AS source')
    request="""SELECT
    InvoiceLine.InvoiceLineId,
    InvoiceLine.Quantity,
    Customer.CustomerId,
    Track.TrackId,
    Invoice.InvoiceDate,
    Invoice.InvoiceId,
    Employee.EmployeeId   
    FROM (((((Track right join InvoiceLine on Track.TrackId = InvoiceLine.TrackId)
          left join Invoice on InvoiceLine.InvoiceId = Invoice.InvoiceId)
          left join Customer on Invoice.CustomerId = Customer.CustomerId)
          left join Employee on Customer.SupportRepId = Employee.EmployeeId))
    
 
    """
    
    """
    for Criteria in metadata:
        if Criteria.get("Historique") == '1':
            SCD2(DataBaseWH,
                 "invoice_dim",
                 Criteria.get("ColumnName"))
    """
    data=DataBaseOp._RetrieveData(request)
    
    ###implementation employe_dim
    
    for d in data : 
        DWH.cur.execute("""INSERT INTO invoice_fact (
        invoice_line_id,
        quantity,
        TPK_customer,
        TPK_track,
        date_id,
        TPK_invoice,
        TPK_employe)
        VALUES (?,?,?,?,?,?,?)""", d)
    
    return data    

    































