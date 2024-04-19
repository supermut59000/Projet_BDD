import time
import asyncio
import sys
sys.path.insert(1, ".//Module//")

from DatabaseHandler import DataBaseHandler
import Module 


T1 = time.time()
OperationnalDatabase = DataBaseHandler('OP.db')
DataWareHouse = DataBaseHandler('DWH1.db')


try:    
    SCDData = Module.ReadMetadata()
    
    Module.create_date_table(DataWareHouse)
    data=Module.CreateTrackTable(OperationnalDatabase,DataWareHouse, metadata=SCDData)
    DataWareHouse.Commit()
    data=Module.CreateInvoiceDim(OperationnalDatabase,DataWareHouse, metadata=SCDData)
    DataWareHouse.Commit()
    data=Module.CreateCustomerDim(OperationnalDatabase,DataWareHouse, metadata=SCDData)
    DataWareHouse.Commit()
    data=Module.CreateEmployeDim(OperationnalDatabase,DataWareHouse, metadata=SCDData)
    DataWareHouse.Commit()
    Module.CreateInvoiceFact(OperationnalDatabase,DataWareHouse, metadata=SCDData)
    
except Exception as error:
    print("Cassé", error)
finally:
    OperationnalDatabase.CloseDatabase()
    DataWareHouse.CloseDatabase()
    T2 = time.time()
    print(T2-T1)
    
    
    
########Implémentation du DWH#########




