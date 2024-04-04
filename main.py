import time
import asyncio
import sys
sys.path.insert(1, ".//Module//")

from DatabaseHandler import DataBaseHandler
import Module 


T1 = time.time()
OperationnalDatabase = DataBaseHandler('OP.db')
DataWareHouse = DataBaseHandler('DWH.db')


try:
    #OperationnalDatabase.executeScriptsFromFile('./SQL/Chinook_Sqlite.sql')
    #DataWareHouse.executeScriptsFromFile('./SQL/DataWareHouse.sql')
    #Module.CreateMetadata(DataWareHouse)
    
    SCDData = Module.ReadMetadata()
    Module.create_date_table(DataWareHouse)

    data=Module.CreateTrackTable(OperationnalDatabase,DataWareHouse, metadata=SCDData)
    
    data=Module.CreateInvoiceDim(OperationnalDatabase,DataWareHouse, metadata=SCDData)
    
    data=Module.CreateCustomerDim(OperationnalDatabase,DataWareHouse, metadata=SCDData)
    
    data=Module.CreateEmployeDim(OperationnalDatabase,DataWareHouse, metadata=SCDData)
    
    Module.CreateInvoiceFact(OperationnalDatabase,DataWareHouse)
    
except Exception as error:
    print("Cassé", error)
finally:
    OperationnalDatabase.CloseDatabase()
    DataWareHouse.CloseDatabase()
    T2 = time.time()
    print(T2-T1)
    
    
    
########Implémentation du DWH#########




