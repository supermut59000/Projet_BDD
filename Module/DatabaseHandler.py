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

    def InsertWithSCD2(self, TableName, data, Headers, IdsColumnsName, SCD2):
        request = self._CreateInsertRequest(TableName)
        IndexIds = [Headers.index(x)-1 for x in IdsColumnsName]
        for row in data:
            AlreadyExist = False
            for IndexId in IndexIds:
                Exist, RowData= self._DoesThisIdExist(row[IndexId],Headers[IndexId+1], TableName)
                if Exist:
                    AlreadyExist= True
                    
            row = list(row)
            RowData = list(RowData)
            
            if SCD2:
                if AlreadyExist:
                    StartingSCD2Index = len(RowData)-3
                    
                    
                    if row != RowData[1:-3]:
                        row.append(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                        row.append('')
                        row.append('1')
                        TPKID = self._GetLastActifEntry(TableName,
                                                    Headers,
                                                    IndexIds,
                                                    RowData,
                                                    Headers[StartingSCD2Index:StartingSCD2Index+3])
                        
                        self._CloseLastActifEntry(TableName,
                                                      Headers,
                                                      Headers[StartingSCD2Index:StartingSCD2Index+3],
                                                      TPKID)
                            
                        
                    if row != RowData[1:-3]:
                        self._ExecuteRequest(request, row)
                    
                else:
                    
                    row.append(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                    row.append('')
                    row.append('1')
                    self._ExecuteRequest(request, row)

            else:
                if AlreadyExist and row != RowData[1:]:
                    self._UpdateLine(TableName, Headers, IndexIds, row)
                elif not AlreadyExist:
                    self._ExecuteRequest(request, row)
        
        return


    def _UpdateLine(self, Table, Headers, IndexIds, Row):
        UPDATE = f"UPDATE {Table} "
        TempSET = []
        for x in range(1,len(Row)):
            if str(Row[x]) != 'None':
                TempSET.append(Headers[x+1] + '=' + "'"+ str(Row[x]) +"'")
            elif Headers[x+1] == 'support_rep_id':
                TempSET.append(Headers[x+1] + '=' + str(Row[x]))
        
        SET = f"SET {','.join(TempSET)} "
        WHERE = f"WHERE {Headers[1]} = {Row[0]}"
        request = UPDATE + SET + WHERE

        self._ExecuteRequest(request)
        return

    def _CloseLastActifEntry(self, Table, Headers, SCD2Columns, TPK):
        
        UPDATE= f"UPDATE {Table} "
        SET = f"""SET {SCD2Columns[1]} = '{datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}',
        {SCD2Columns[2]} = '0' """
        WHERE = f"WHERE {Headers[0]} = {TPK}"
        request = UPDATE + SET + WHERE

        self._ExecuteRequest(request)
        return

    def _GetLastActifEntry(self, Table, Headers, IndexIds, Row, SCD2Columns):
        
        #Definition des variables
        TPKName = Headers[0]
        Ids = [f"{Headers[x+1]} = {Row[x+1]}" for x in IndexIds]
                
        #Creation de la requete
        
        SELECT = f"SELECT {TPKName} FROM {Table} "
        WHERE  = f"WHERE {SCD2Columns[2]} = '1' AND {'AND'.join(Ids)} "
        ORDERBY = f"ORDER BY {SCD2Columns[0]} DESC"
        try:
            request = SELECT+WHERE+ORDERBY
            data= self._RetrieveData(request)[0][0]
            return data
        except:
            print("Probleme pas d'entrer precedante")
        

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
        TPK = self.GetColumnFromTable(TableName, True)
        request = f"SELECT * FROM {TableName} WHERE {ColumnName} = {Id} ORDER BY {TPK[0]} ASC;"
        try:
            data = self.cur.execute(request).fetchall()
            if data != []:
                Exist = True
                data = data[-1]
        except OperationalError:
            print('Error on request',request)
        
        return Exist, data

    

    


    
    
        
            
            
        
        
        