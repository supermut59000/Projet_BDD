import time

import sys
sys.path.insert(1, ".//Module//")

from DatabaseHandler import DataBaseHandler
import Module 


metadata = [{"ColumnName":"name","Historique":1},
            {"ColumnName":"UnitPrice","Historique":0},
            {"ColumnName":"album","Historique":1}]


T1 = time.time()
OperationnalDatabase = DataBaseHandler('OP.db')
OperationnalDatabase.executeScriptsFromFile('./SQL/Chinook_Sqlite.sql')


DataWareHouse = DataBaseHandler('DATA.db')
DataWareHouse.executeScriptsFromFile('./SQL/DataWareHouse.sql')
Module.CreateMetadata(DataWareHouse)

#print(Module.ReadMetadata("Metadata.txt"))

"""
print(Database.GetColumnFromTable("track_dim", IsPk= False))
print(Module.CreateTrackTable(Database, metadata))
print(Database.GetColumnFromTable("track_dim", IsPk= False))
"""
OperationnalDatabase.CloseDatabase()

DataWareHouse.CloseDatabase()









T2 = time.time()
print(T2-T1)