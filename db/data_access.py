import pandas as pd
import numpy as np
import sys
import os
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))


from db import JsonParser
from db import StatementFactory
from db import DbOperator



# Datenzugriff. 
# Der db-Zugriff müsste noch implementiert werden. Gegenwärtig läuft der 
# Zugriff lediglich über .csv
class DataAccessor:
    
    def __init__(self):
        self.main_config = JsonParser("config/program.json").parse()
        self.data_model = JsonParser("config/db.json").parse()
        self.dbOperator = DbOperator()
        self.statementFactory = StatementFactory()
        
    def getTable(self, table):
        if(self.main_config["datasource"]=="csv"):
            print("getting Data fom csv")

            return pd.read_csv(self.data_model["tables"][table]["source"])
        elif(self.main_config["datasource"]=="postgres"):
            print("getting Data fom postges")

            selectStatements = self.statementFactory.createSeletStatementByTableName(table)
            df = self.dbOperator.select(selectStatements)
            #return df #falls df schon ein dataframe ist
            return pd.DataFrame(np.array(df))
        else:
            print("data source must be either 'csv' or 'postgres'")