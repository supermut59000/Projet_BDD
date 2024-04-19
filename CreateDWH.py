import time
import os
import sys
sys.path.insert(1, ".//Module//")

######################
# IMPORT DES MODULES #
######################
from DatabaseHandler import DataBaseHandler


PATH = os.getcwd()

#############
# FONCTIONS #
#############
def CreateMetadata(DatabaseObject):
    if "Metadata" not in os.listdir(PATH):
        os.mkdir(PATH+ "/Metadata/")
        
    request = "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name;"
    Tables = DatabaseObject._RetrieveData(request)

    with open(PATH+ "/Metadata/Metadata.txt", "w") as writer:
        writer.write("TableName,Historique\n")
        for Table in Tables:
            if Table[0] == 'track_dim':
                TempText = f"{Table[0]},1\n"
            else:
                TempText = f"{Table[0]},0\n"
            
            writer.write(TempText)

T1 = time.time()

######################
# Création de la BDD #
######################
StarDataBase = DataBaseHandler('DWH.db')

try:
    ##########################
    # Alimentation de la BDD #
    ##########################
    StarDataBase.executeScriptsFromFile('./SQL/DataWareHouse.sql')
    CreateMetadata(StarDataBase)
except Exception as error:
    print("Cassé", error)
finally:
    #########################################
    # Fermeture et enregistrement de la BDD #
    #########################################
    StarDataBase.CloseDatabase()
T2 = time.time()

print("Temps d'execution",str(round(T2-T1,2))+"s")