import time

import sys
sys.path.insert(1, ".//Module//")

######################
# IMPORT DES MODULES #
######################
from DatabaseHandler import DataBaseHandler
import Module 



T1 = time.time()

######################
# Création de la BDD #
######################
OperationnalDatabase = DataBaseHandler('OP.db')

try:
    ##########################
    # Alimentation de la BDD #
    ##########################
    OperationnalDatabase.executeScriptsFromFile('./SQL/Chinook_Sqlite.sql')
except Exception as error:
    print("Cassé", error)
finally:
    #########################################
    # Fermeture et enregistrement de la BDD #
    #########################################
    OperationnalDatabase.CloseDatabase()
T2 = time.time()

print("Temps d'execution",str(round(T2-T1,2))+"s")