#!/usr/bin/env python3
# -*- coding: utf-8 -*-

######## Class containing functions to execute database utilities #########
#
# Author: Arthur Telles
#
# Description: 
# 
# This script contains a class that initiates receiving a configuration dictionary
# and contains functions that execute query and insert operations, while registering 
# the log properly.
# 
# -------------------- USAGE -------------------- #
#
##### - query_request - #
#
# * INPUT *
# ---------
# 1. String containing query
#
# * OUTPUT *
# ----------
# 1. Dataframe containing the result
#
##### - query_jdbc - #
#
# * INPUT *
# ---------
# 1. String containing query
#
# * OUTPUT *
# ----------
# 1. Dataframe containing the result
#
##### - insert_jdbc - #
#
# * INPUT *
# ---------
# 1. Dataframe containing the data to insert in the database.
#
##### - chunker - #
#
# * INPUT *
# ---------
# 1. String containing query
#
# * OUTPUT *
# ----------
# 1. Dataframe containig the result
#
##### - insert_chunks_with_progress - #
#
# Description: Insert dataframe in batches or chunks, useful for big pieces of data
#
# * INPUT *
# ---------
# 1. Dataframe containing the data to insert in the database.

import os, sys, traceback
import requests
import pandas as pd
import time
import math
from sqlalchemy import create_engine, text, update
import re
from tqdm import tqdm

class query():
    def __init__(self,config_dict):
        
        if (type(config_dict).__name__ != 'dict'):
            raise Exception('Configuration info should be of type dict.')
            
        
        self.username = config_dict['user']
        self.password = config_dict['password']
        self.endpoint = config_dict['endpoint']
        self.sql_engine = config_dict['sql_engine']
        self.port = config_dict['port']
        self.database = config_dict['database']
        self.jdbc_string = str(self.sql_engine) + "://" + str(self.username) + ":" + str(self.password) + \
                        "@" + str(self.endpoint) + ":" + str(self.port) + "/" + self.database
        
    def query_request(self,query):
        
        # remove special characters if query is not string
        try:
#            query = text(query)
            query = query.decode('utf-8', 'ignore').encode('ASCII', 'ignore')
        except AttributeError:
            query = re.sub(r"(ç|ã|á|à|â|ú|ü|õ|ô|ó)",'',query)
            pass
        
        startTime = time.time()
        print(str(pd.to_datetime('now').tz_localize('UTC').tz_convert('America/Sao_Paulo')) +\
              ": Starting query ...") 
        print("Query's first 150 characters: {}".format(str(query)[:150]))
        self.query = query
        content_type ="text/plain"
        
        
        self.header = {'Content-Type':content_type}
        param = {'query':query}
        
        session = requests.Session()
        
        try:
            r = session.post(self.endpoint,auth = (self.username,self.password), headers = self.header, data = self.query)
            #        print(r.content)
            json_data = r.json()
        except Exception as e:
            print("Error: " + str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            traceback.print_exc(file=sys.stdout)
            print(str(r.content))
            print("# -- #" + "\n" + "Query: " + "\n" + str(query) + "\n" + "# -- #")
            print(str(pd.to_datetime('now').tz_localize('UTC').tz_convert('America/Sao_Paulo')) +\
            "\n" + "Error in query execution. Repeating operation...")
            r = session.post(self.endpoint,auth = (self.username,self.password), headers = self.header, data = self.query)
            json_data = r.json()
            
        df = pd.DataFrame(json_data)
        
        print("Result's first rows: " + "\n" + str(df.head()) + " ." + "\n" +\
              "Result's shape: {}".format(str(df.shape)) + "\n" +\
              "Result's columns: {}".format(str(list(df.dtypes.index))) + "\n" +\
              "Result's dtypes: {}".format(str(list(df.dtypes))))
        
        # if the df row size is bigger than 100, prin nan occurrences
        if df.shape[0] > 100:
#            print("Result's nan columns: {}".format(str(list(df.isnull().sum().index))))
            print("Result's nan values: {}".format(str(list(df.isnull().sum().values))))
        
        print("Query completed. Elapsed Time: {} minutes and {} seconds.".format(str(math.floor((time.time() - startTime)/60)),\
          str(round(time.time() - startTime - math.floor((time.time() - startTime)/60)*60,2))))
        
        return df
    
    def query_jdbc(self,query):
        
        # remove special characters if query is not string
        try:
#            query = text(query)
            query = query.decode('utf-8', 'ignore').encode('ASCII', 'ignore')
        except AttributeError:
            pass
        
        startTime = time.time()
        print(str(pd.to_datetime('now').tz_localize('UTC').tz_convert('America/Sao_Paulo')) +\
              ": Starting query ...")
                        
        print("Query's first 150 characters: {}".format(str(query)[:150]))
        engine = create_engine(self.jdbc_string)
        
        try:
            df = pd.read_sql_query(query,engine)
        except Exception as e:
            print("Error: " + str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            traceback.print_exc(file=sys.stdout)
            print("# -- #" + "\n" + "Query: " + "\n" + str(query) + "\n" + "# -- #")
            print(str(pd.to_datetime('now').tz_localize('UTC').tz_convert('America/Sao_Paulo')) +\
            "\n" + "Error in query execution. Repeating operation...")
            df = pd.read_sql(query,engine)
        
        print("Result's first rows: " + "\n" + str(df.head()) + " ." + "\n" +\
              "Result's shape: {}".format(str(df.shape)) + "\n" +\
              "Result's columns: {}".format(str(list(df.dtypes.index))) + "\n" +\
              "Result's dtypes: {}".format(str(list(df.dtypes))))
        
        # if the df row size is bigger than 100, prin nan occurrences
        if df.shape[0] > 100:
#           print("Result's nan columns: {}".format(str(list(df.isnull().sum().index))))
           print("Result's nan values: {}".format(str(list(df.isnull().sum().values))))
        
        print("Query completed. Elapsed Time: {} minutes and {} seconds.".format(str(math.floor((time.time() - startTime)/60)),\
          str(time.time() - startTime - math.floor((time.time() - startTime)/60)*60)))
        
        return df
    
    def insert_jdbc(self,df,name,schema, if_exists):
        if if_exists != 'append' and if_exists != 'replace':
            if_exists = 'fail'
        
        startTime = time.time()
        print(str(pd.to_datetime('now').tz_localize('UTC').tz_convert('America/Sao_Paulo')) + ": Inserting data in "\
              + str(schema) + " " + str(name) + "...") 
                        
        engine = create_engine(self.jdbc_string)
        
        try:
            df.to_sql(name, engine, schema, if_exists=if_exists, index=False)
        except Exception as e:
            print("Error: " + str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            traceback.print_exc(file=sys.stdout)
            print("Error in insert. Repeating operation...")
            df.to_sql(name, engine, schema, if_exists=if_exists, index=False)
        
        print("Inserted data's first rows: " + "\n" + str(df.head()) + " .")
        
        print("Insert completed. Elapsed Time: {} minutes and {} seconds.".format(str(math.floor((time.time() - startTime)/60)),\
          str(time.time() - startTime - math.floor((time.time() - startTime)/60)*60)))
        
    def chunker(self, seq, size):
        # from http://stackoverflow.com/a/434328
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))
    
    def insert_chunks_with_progress(self, df, name, schema, if_exists):
        if if_exists != 'append' and if_exists != 'replace':
            if_exists = 'fail'
        
        startTime = time.time()
        print(str(pd.to_datetime('now').tz_localize('UTC').tz_convert('America/Sao_Paulo')) + ": Inserting data in "\
              + str(schema) + " " + str(name) + "...") 
                        
        engine = create_engine(self.jdbc_string)
        
        chunksize = int(len(df) / 10) # 10%
        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(self.chunker(df, chunksize)):
                try:
    #                replace = ("replace" if i == 0 else "append") # choose if exists method
                    cdf.to_sql(name, engine, schema, if_exists=if_exists, index=False) 
                    pbar.update(chunksize)
                    print("\n" + "Chunksize inserted: " + str(chunksize))
                except Exception as e:
                    print("Error: " + str(e))
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                    traceback.print_exc(file=sys.stdout)
                    print("Error in insert. Repeating operation...")
                    cdf.to_sql(name, engine, schema, if_exists=if_exists, index=False) 
                    pbar.update(chunksize)
                    print("\n" + "Chunksize inserted: " + str(chunksize))
                
        print("Insert completed. Elapsed time: " + str(time.time() - startTime) + ' seconds.')
        
    def update_jdbc(self, query):
        
        try:
#            query = text(query)
            query = query.decode('utf-8', 'ignore').encode('ASCII', 'ignore')
        except AttributeError:
            pass
        
        startTime = time.time()
        print(str(pd.to_datetime('now').tz_localize('UTC').tz_convert('America/Sao_Paulo')) +\
              ": Starting query ...")
                        
        print("Query's first 200 characters: {}".format(str(query)[:200]))
        engine = create_engine(self.jdbc_string)
        
        with engine.connect() as con:

            con.execute(query)
            
        print("Update completed. Elapsed Time: {} minutes and {} seconds.".format(str(math.floor((time.time() - startTime)/60)),\
          str(time.time() - startTime - math.floor((time.time() - startTime)/60)*60)))
        
    def insert_raw_query_jdbc(self, query):
        
        try:
#            query = text(query)
            query = query.decode('utf-8', 'ignore').encode('ASCII', 'ignore')
        except AttributeError:
            pass
        
        startTime = time.time()
        print(str(pd.to_datetime('now').tz_localize('UTC').tz_convert('America/Sao_Paulo')) +\
              ": Starting query ...")
                        
        print("Query's first 200 characters: {}".format(str(query)[:200]))
        engine = create_engine(self.jdbc_string)
        
        with engine.connect() as con:

            con.execute(query)
            
        print("Insert completed. Elapsed Time: {} minutes and {} seconds.".format(str(math.floor((time.time() - startTime)/60)),\
          str(time.time() - startTime - math.floor((time.time() - startTime)/60)*60)))