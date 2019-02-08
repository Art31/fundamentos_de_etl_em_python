#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 16:53:57 2018

@author: arthur.telles
"""

import sys, os, traceback
#change to the utils folder
sys.path.append(os.path.abspath(os.path.join('..', 'utils')))

from query_executor import query
import config
import pandas as pd
import re
import math
import time
import emailutil
#import math
import numpy as np
from datetime import timedelta
from get_values_to_load import get_days_to_load
import string_manipulators

initial_time = time.time()

# get configuration info from config.ini through config.py
pg_config_dict = config.get_all_as_dict('postgres')
presto_config_dict = config.get_all_as_dict('presto')

# instantiate classes and configure connections
postgres_executor = query(pg_config_dict)
presto_executor = query(presto_config_dict)

query_to_execute = """select approval_date,
b.list_id_nk,
reason_removed_detail_name,
state_name,
case when b.category_id_fk = 46 then 'Autos'
     when b.category_id_fk in (79,40,86,44,3,2,80) then 'RE'
     when b.category_id_fk is null then 'no cat'
     else 'Other' end as category,
account_id_fk,
price
from ods.ad b
join ods.dm_area c on b.area_id_fk=c.area_id_pk
join ods.dm_reason_removed_detail d on b.reason_removed_detail_id_fk=d.reason_removed_detail_id_pk
where {DATE_CLAUSE}
limit 300000"""

# ---- Define these variables ---- #
etl_context = 'assignment'
schema_to_load = 'etl_class'
table_in_schema_to_load = 'assignment'
prefixo_email_olx = '' # ex: arthur.telles
# -------------------------------- #


print("\n Running Name of ETL # -------------- # \n ")

dates_to_load, last_day_loaded = get_days_to_load(etl_context,'etl_class.date_log',\
                                                    'etl_class.config_params',\
                                                    last_day=pd.to_datetime('2018-09-01'))
# load in a desc manner
for date in list(dates_to_load):

    # Loading at D-1
    date_clause_string, today_date_string = string_manipulators.generate_date_string(date - timedelta(days=1),False)
    
    edited_query = string_manipulators.substitute_params_in_string(['{DATE_CLAUSE}'],\
                                                [date_clause_string],query_to_execute)
    
    presto_result = presto_executor.query_request(edited_query)
    
    pg_result = postgres_executor.query_jdbc("select * from etl_class.accounts_table")
    
    
    # ----- manipulação de dados ocorre aqui ----- #

    
    # -------------------------------------------- #
    
    result = ''
    
    if result.shape[0] > 100:
        postgres_executor.insert_chunks_with_progress(result, table_in_schema_to_load, schema_to_load,\
                                                    if_exists='append')
    else:
        postgres_executor.insert_jdbc(result,table_in_schema_to_load,schema_to_load, if_exists='append')
    
    date_log_data = pd.DataFrame(data={"date": date,
                        "load_date": str(pd.to_datetime('now').tz_localize('UTC').tz_convert('America/Sao_Paulo')\
                                    .tz_localize(None)),
                        "context": etl_context}, index=[0])
    postgres_executor.insert_jdbc(date_log_data,'date_log','etl_class', if_exists='append')
    
print('No more days to load!')

# gravar no log o tempo para execução
print("Total Elapsed Time: {} minutes and {} seconds.".format(str(math.floor((time.time() - initial_time)/60)),\
      str(time.time() - initial_time - math.floor((time.time() - initial_time)/60)*60))) 
    