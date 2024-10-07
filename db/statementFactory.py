#!/usr/bin/python

import pandas as pd


""" 
 class StatementFactory 
 creates SQL-Statements

 @institution: TH Luebeck
 @author: Jakob Poley
 @date: 23. Januar 2021
"""
class StatementFactory:


    # creates tables
    # @param datamodel: datamodel parsed from ../config/db.json with JsonParser in program.py
    def createCreateTablesStatement(self, datamodel):
        
        statement = {}
        for table in datamodel["tables"]:
            primarykey = datamodel["tables"][table]["primarykeys"]
            foreignkeys = datamodel["tables"][table]["foreignkeys"]
            fields = datamodel["tables"][table]["fields"]
            types = datamodel["tables"][table]["types"]
            foreignkeyparts = {}
            if(foreignkeys!=['']):
                for fkey in foreignkeys:
                    fkeyparts=fkey.split(".")
                    foreignkeyparts[fkeyparts[0]]=[fkeyparts[1], fkeyparts[2]]
            
            parts = ""
            for i in range(len(datamodel["tables"][table]["fields"])):
                if(fields[i] in foreignkeyparts):
                    parts = parts + fields[i] + " " + types [i] + " REFERENCES" + " " + foreignkeyparts[fields[i]][0]+ "(" + foreignkeyparts[fields[i]][1] + "), "
                elif(fields[i] in primarykey):
                    parts = parts + fields[i] + " " + types[i] + " PRIMARY KEY,"
                else:
                    parts = parts + fields[i]+ " " + types[i] + "," 
            statement["createTable"+table]="CREATE TABLE " + table + " ("+ parts[0:len(parts)-1] + ");"
        return statement


    # inserts csv-files into tables
    # @param datamodel: datamodel parsed from ../config/db.json with JsonParser in program.py
    def createStatementInsertCsvIntoTable(self, datamodel):
        statement = {}
        for table in datamodel["tables"]:
            csvSource = datamodel["tables"][table]["source"]
            statement["insertCsv"+table] = "copy " + table + " FROM '" + csvSource + "' DELIMITER ',' CSV HEADER;"
        return statement


    # deletes content from tables
    # @param datamodel: datamodel parsed from ../config/db.json with JsonParser in program.py
    def createDeleteContentFromTablesStatement(self, datamodel):
        statement = {}
        for table in datamodel["tables"]:
            statement["delete" + table]="TRUNCATE " + table + " CASCADE;"
        return statement


    # dropes tables
    # @param datamodel: datamodel parsed from ../config/db.json with JsonParser in program.py
    def createDropTablesStatement(self, datamodel):
        statement = {}
        for table in datamodel["tables"]:
            statement["drop" + table]="DROP TABLE " + table + ";"
        return statement

    # gets data form postges
    # @param tablename: name of the table to select in DataAccessor
    # @param fields: fields of the table to select in DataAccessor -> not implemented yet!!!
    def createSeletStatementByTableName(self, tablename, fields="*"):
        return "SELECT " + fields +" FROM " +tablename