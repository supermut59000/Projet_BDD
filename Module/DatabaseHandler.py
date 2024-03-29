import sqlite3
from sqlite3 import OperationalError
import datetime

class DataBaseHandler:
    def __init__(self, databasename):
        self.con = sqlite3.connect(databasename)
        self.cur = self.con.cursor()
        return

    def CloseDatabase(self):
        self.con.commit()
        self.con.close()

    def executeScriptsFromFile(self, filename):
        # Open and read the file as a single buffer
        fd = open(filename, 'r', encoding="utf8")
        sqlFile = fd.read()
        fd.close()

        sqlCommands = sqlFile.split(';\n')

        # Execute every command from the input file
        for command in sqlCommands:
            self._ExecuteRequest(command)
            
    def AlterTable(self, TableName: str, ColumnName: str, Columntype: str):
        request = f"ALTER TABLE {TableName} ADD {ColumnName} {Columntype};"
        self._ExecuteRequest(request)
        return
    
    def GetColumnFromTable(self, TableName, IsPk = False):
        request = f"SELECT l.name FROM pragma_table_info('{TableName}') as l "
        if IsPk :
            request += "WHERE l.pk = 1"
        request += ";"
        data = []
        try:
            data = self.cur.execute(request).fetchall()
        except OperationalError:
            print('Error on request',request)
        return [x[0] for x in data]
    
    def InsertWithSCD2(self, TableName, data, Headers, IdsColumnsName= [], SC2Columns= []):
        import time
        request = self._CreateInsertRequest(TableName)
        IndexIds = [Headers.index(x)-1 for x in IdsColumnsName]
        
        for row in data[:10]:
            AlreadyExist = False
            for IndexId in IndexIds:
                print(IndexId,row[IndexId],Headers[IndexId+1])
                Exist, RowData= self._DoesThisIdExist(row[IndexId],Headers[IndexId+1], TableName)
                if Exist:
                    AlreadyExist= True
            row = list(row)
            RowData = list(RowData)
            if AlreadyExist:
                for Column in SC2Columns:
                    if row[Column] != RowData[1:][Column]:
                        print('a')
                        row.append(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                        row.append('')
                        row.append('1')
                    else:
                        print('b')
                        row.append('')
                        row.append('')
                        row.append('')
                
                if row[:-3*len(SC2Columns)] != RowData[1:-3*len(SC2Columns)]:
                    self._ExecuteRequest(request, row)
                
            else:
                row.append(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                row.append('')
                row.append('1')
                self._ExecuteRequest(request, row)
        
        return

    
    def _CreateInsertRequest(self, TableName):
        coltemp= self.GetColumnFromTable(TableName)[1:]
        
        INSERT = f"INSERT INTO {TableName} "
        COLUMNS = f"({','.join(coltemp)}) "
        VALUES = f"VALUES ({','.join(['?' for x in coltemp])});"
        return INSERT + COLUMNS + VALUES

    def _ExecuteRequest(self, request: str, data=[]):
        try:
            if data!= []:
                self.cur.execute(request,data)
            else:
                self.cur.execute(request)
        except OperationalError:
            print('Error on request',request)
            
    def _RetrieveData(self, request: str):
        data = []
        try:
            data = self.cur.execute(request).fetchall()
        except OperationalError:
            print('Error on request',request)
        return data
    
    def _DoesThisIdExist(self, Id, ColumnName:str, TableName: str):
        Exist = False
        
        request = f"SELECT * FROM {TableName} WHERE {ColumnName} = {Id} ORDER BY TPK_invoice ASC;"
        try:
            data = self.cur.execute(request).fetchall()
            print(data)
            if data != []:
                Exist = True
                print(data)
                data = data[-1]
        except OperationalError:
            print('Error on request',request)
        
        return Exist, data

    

    


    
    
        
            
            
        
        
        