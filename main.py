import sqlite3
from sqlite3 import OperationalError

import sys
sys.path.insert(1, ".//Module//")

from DatabaseHandler import DataBaseHandler


Database = DataBaseHandler('db.db')
Database.executeScriptsFromFile('./SQL/Chinook_Sqlite.sql')


