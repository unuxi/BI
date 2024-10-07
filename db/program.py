#!/usr/bin/python

from json_parse import JsonParser 
from db import DbOperator
from statementFactory import StatementFactory

'''
ProgramController: program flow controller creates a SQL statement and executes it in pstgresql depending on main.json
@Institution: TH Luebeck
@author: Jakob Poley
@date: 24. Januar 2021
'''

class ProgramController:

    def __init__(self, main_json_path):
        self.main_json = JsonParser(main_json_path).parse() 
        self.data_model = JsonParser("../config/db.json").parse()
        self.dbOperator = DbOperator()
        self.statementFactory = StatementFactory()

    # executes program parts
    def execute(self):
        self.initializeDatabase()

    # initializes database with steps that are defined in ../config/program.json
    def initializeDatabase(self):
        if(self.main_json["db"]["create"]=="y"):
            print("create database")
            createTableStatements = self.statementFactory.createCreateTablesStatement(self.data_model)
            for val in createTableStatements.values():
                self.dbOperator.execute(val)
        if(self.main_json["db"]["insert"]=="y"):
            print("insert csv data into tables")
            statements = self.statementFactory.createStatementInsertCsvIntoTable(self.data_model) 
            for query in statements.values():
                self.dbOperator.execute(query)
        if(self.main_json["db"]["drop"]=="y"):
            print("drop tables from database")
            dropTableStatements = self.statementFactory.createDropTablesStatement(self.data_model)
            for val in dropTableStatements.values():
                self.dbOperator.execute(val)
        if(self.main_json["db"]["delete"]=="y"):
            print("delete content from tables")
            deleteContentStatements = self.statementFactory.createDeleteContentFromTablesStatement(self.data_model)
            for val in deleteContentStatements.values():
                self.dbOperator.execute(val)


if __name__ == '__main__':
    programController = ProgramController("../config/program.json")
    programController.execute()
