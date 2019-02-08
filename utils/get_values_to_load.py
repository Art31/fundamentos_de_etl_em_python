######## Group of functions to get a list of values to load #########
#
# Author: Arthur Telles
#
# Description: 
# 
# This script groups functions used to request values in order to load, register in
# log tables and control which combinations have been loaded.
# 
# -------------------- USAGE -------------------- #
#
##### - check_first_day_to_load - #
#
# Description: Generate a date clause that works for partitioning in queries,
# aggregating all the dates contemplated by the input.
#
# * INPUT *
# ---------
# 1. date_to_load and timezone are compulsory
#
# * OUTPUT *
# ----------
# 1. date_string is the date array string used in sql queries for partitioning
#
##### - get_days_to_load - #
#
# Description: Generate a date clause that works for partitioning in queries,
# aggregating all the dates in the range contemplated by the input.
#
# * INPUT *
# ---------
# 1. context 
# 2. log_schema_table
# 3. params_schema_table
# 4. attribute
# 5. first_day_to_load_attribute
#
# * OUTPUT *
# ----------
# 1. dates_to_load
# 2. last_day_loaded
# 
##### - get_requests_to_load - #
#
# Description: Generate a date clause that works for partitioning in queries,
# aggregating all the dates in the range contemplated by the input.
#

from query_executor import query
import config
import time
import pandas as pd
import math
#from datetime import timedelta

def check_first_day_to_load(executor,context,schema_table,attribute='first_day_to_load'):
    
    print("Checking first day to load...")
    query = "select value from {} where attribute = '{}' and context = '{}' limit 1"\
    .format(str(schema_table),str(attribute),str(context))
    
    try:
        fdtl = executor.query_jdbc(query)
        first_day = fdtl.iloc[0,0]
        first_day = pd.to_datetime(first_day).date()
    except: 
        print("# ---- #" + "\n" + "Couldnt find first day to load for this context. Using today as" + \
              "default config..." + "\n" + "# ---- #")
        time.sleep(120)
        first_day = pd.to_datetime('now').tz_localize('UTC').tz_convert('America/Sao_Paulo')\
                             .tz_localize(None).date()
        date_log_data = pd.DataFrame(data={"attribute": attribute,
                             "value": first_day,
                             "context": context},index=[0])
        executor.insert_jdbc(date_log_data,schema_table.split('.')[1],schema_table.split('.')[0], if_exists='append')
    
    return first_day

def get_days_to_load(context,log_schema_table,params_schema_table,attribute=None,\
                     first_day_to_load_attribute='first_day_to_load',last_day='now'):
    
    # instantiate classes and configure connections
    pg_config_dict = config.get_all_as_dict('postgres') 
    postgres_executor = query(pg_config_dict)
    
    first_day = check_first_day_to_load(postgres_executor,context,params_schema_table,first_day_to_load_attribute)
    
    if attribute == None:
        get_loaded_dates_query = "select date from {} where context = '{}'\
        order by date desc".format(str(log_schema_table),str(context))
    else:
        get_loaded_dates_query = "select date from {} where attribute = '{}' and context = '{}'\
        order by date desc".format(str(log_schema_table),str(attribute),str(context))
    
    # check all days loaded and convert to date list
    full_load = postgres_executor.query_jdbc(get_loaded_dates_query)
    full_load = [pd.to_datetime(date).date() for date in full_load['date'].values]
    
    if len(full_load) > 0:
        last_day_loaded = full_load[0]
    else: 
        last_day_loaded = None
    
    if last_day == 'now':
        desired_load_list = pd.date_range(start=first_day, end=(pd.to_datetime('now').tz_localize('UTC')\
                            .tz_convert('America/Sao_Paulo').date())).date.tolist()
    else:
        assert type(last_day).__name__ == 'date' or type(last_day).__name__ == 'Timestamp', "Last_day is not a date format!"
        desired_load_list = pd.date_range(start=first_day, end=last_day).date.tolist()
    
    dates_to_load = []
    for date in desired_load_list:
        if date not in full_load:
            dates_to_load.append(date)
            
    return dates_to_load, last_day_loaded   

def get_requests_to_load(context,schema_table,load_check_attribute,second_attribute,actual_response_number):
    
    # instantiate classes and configure connections
    pg_config_dict = config.get_all_as_dict('postgres')
    postgres_executor = query(pg_config_dict)
    
#    load_check_day = check_load_check_day_to_load(postgres_executor,context,schema_table_date)
    
    # load_check_attribute = page for survey monkey etl
    check_last_load_check_attribute_query = "select date, value from " + str(schema_table) + " where " + \
    "context = '" + str(context) + "' and attribute = '" + str(load_check_attribute) + \
    "' order by date desc, value desc"
    # second attribute = id for survey monkey etl
    check_last_second_attribute_query = "select date, value from " + str(schema_table) + " where " + \
    "context = '" + str(context) + "' and attribute = '" + str(second_attribute) + \
    "' order by date desc, value desc"
    
    load_check_load = postgres_executor.query_jdbc(check_last_load_check_attribute_query)
    attribute_to_return = postgres_executor.query_jdbc(check_last_second_attribute_query)

    print("Attribute " + str(load_check_attribute) + " at day " + str(load_check_load['date'].iloc[0]) + \
          " has value " + str(load_check_load['value'].iloc[0]))
    print("Attribute " + str(second_attribute) + " at day " + str(attribute_to_return['date'].iloc[0]) + \
          " has value " + str(attribute_to_return['value'].iloc[0]))
    
    # considering a fix and max number of 100 responses per request
    first_page = math.floor(int(load_check_load['value'].iloc[0]))
    last_page = math.ceil(actual_response_number/100)
#    last_req = full_load['value'].iloc[0]
    
    return first_page,last_page,attribute_to_return['value'].iloc[0]
    