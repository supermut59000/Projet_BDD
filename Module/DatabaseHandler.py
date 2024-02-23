import sqlite3
from sqlite3 import OperationalError

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
        return data

        

    def _ExecuteRequest(self, request: str):
        try:
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
        
        request = f"SELECT {ColumnName} FROM {TableName} WHERE {ColumnName} = {Id}"
        try:
            data = self.cur.execute(request).fetchall()
            if data != []:
                Exist = True
        except OperationalError:
            print('Error on request',request)
        
        return Exist

    

    


    
    
        
            
            
        
        
        