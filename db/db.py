#!/usr/bin/python
from configparser import ConfigParser
import psycopg2


""" 
class DbOperator
DbOperator connects to database via ../config/database.ini and 
executes postgres statements or selects data based on ../conf/db.json

@institution: TH Luebeck
@author: Jakob Poley
@date: 23 Januar 2021 
"""

class DbOperator:


    def __init__(self):
        self.params = self.config()


    # setting config parameters for database connection
    # @param filename: path to db-access config file database.ini
    # @param section: postgres section (if in future further databases are allowed)
    def config(self, filename='../config/database.ini', section='postgresql'):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        # get section, default is postgresql (see section='postgresql' in line 28)
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

        return db


    # executes query
    # @param query: query string
    def execute(self, query):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**self.params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            cur.execute(query)
            
            # close the communication with the PostgreSQL
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')


    # selects data
    # @param query: query string
    def select(self, query):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**self.params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            cur.execute(query)
            
            # display the postgreSQL select content 
            content = cur.fetchall()

            # close the communication with the PostgreSQL
            cur.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
                return content
