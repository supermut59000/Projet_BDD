import time

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
    Module.CreateMetadata(DataWareHouse)
    SCDData = Module.ReadMetadata()
    #track_dim_metadata = SCDData[0][SCDData[1].index("track_dim.txt")]
    data=Module.CreateTrackTable(OperationnalDatabase,DataWareHouse, metadata=[])
    #data=Module.CreateInvoiceDim(OperationnalDatabase,DataWareHouse, metadata=[])
    #data=Module.CreateCustomerDim(OperationnalDatabase,DataWareHouse, metadata=[])
    
except Exception as error:
    print("Cassé", error)
finally:
    OperationnalDatabase.CloseDatabase()
    DataWareHouse.CloseDatabase()
    T2 = time.time()
    print(T2-T1)
    
    
########Implémentation du DWH#########






