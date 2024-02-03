import sqlite3
from sqlite3 import OperationalError

class DataBaseHandler:
    def __init__(self, databasename):
        self.con = sqlite3.connect(databasename)
        self.cur = self.con.cursor()
        return


    def executeScriptsFromFile(self, filename):
        # Open and read the file as a single buffer
        fd = open(filename, 'r', encoding="utf8")
        sqlFile = fd.read()
        fd.close()

        sqlCommands = sqlFile.split(';\n')

        # Execute every command from the input file
        for command in sqlCommands:
            try:
                self.cur.execute(command)
            except OperationalError:
                print("Command skipped: ", command)

        self.con.commit()
        self.con.close()