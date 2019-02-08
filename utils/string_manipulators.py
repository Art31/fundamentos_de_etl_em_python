######## Functions to manipulate text for data science #########
#
# Author: Arthur Telles
#
# Description: 
# 
# This is a collection of functions that perform an specific task, almost all of
# them related to manipulating queries and customizing pre defined text with regex. 
# 
# -------------------- USAGE -------------------- #
#
##### - generate_date_string - #
#
# Description: Generate a date clause that works for partitioning in queries,
# aggregating all the dates contemplated by the input.
#
# * INPUT *
# ---------
# 1. date_to_load is compulsory
# 2. only ONE optional argument is accepted, and the date array string 
# between date_to_load the the argument will be returned in the date_string variable
#
# * OUTPUT *
# ----------
# 1. date_string is the date array string used in sql queries for partitioning
# 2. date_to_load is just the date string with the sql date function (always the first argument)
#
##### - substitute_params_in_string - #
#
# Description: Substitute a list of strings placed between braces - {} - for the strings
# given in the input. Developed for customizing queries.
#
# * INPUT *  params_keys_list,params_values_list,string_to_sub
# ---------
# 1. params_keys_list: the words between braces placed in the text
# 2. params_values_list: the respective strings to replace the old ones
# 3. string_to_sub: text containing the words between braces
#
# * OUTPUT *
# ----------
# 1. string_to_sub: text containing the words already replaced 
#
##### - build_cube_query (UNDER CONSTRUCTION) - #
#
# Description: Take a query and use regex to identify its commas in order to perform 
# the "cube" function from presto. If you use commas before the from clause, its recommended
# to replace them with {}

from datetime import timedelta
import pandas as pd
import re
import os

def generate_date_string(date_to_load,timezone=False, *args, **kwargs):
    if (type(timezone).__name__ != 'bool'):
        raise Exception('Timezone should be of type boolean.')
    elif (type(timezone).__name__ != 'date'):
        date_to_load = pd.to_datetime(date_to_load).date()

    if len(args) > 0:
        assert len(args) == 1,str("# -- #" + "\n" + "Please use only one optional argument!" + "\n" + "# -- #")
        print("Generate date string: Argument with values between {} and {} has been called"\
              .format(str(date_to_load),str(args[0])))
          
        if pd.to_datetime(args[0]) > pd.to_datetime(date_to_load):
            start_d = date_to_load; end_d = pd.to_datetime(args[0])
        else:
            start_d = pd.to_datetime(args[0]); end_d = date_to_load
            
        if timezone:
            end_d = end_d + timedelta(days=1)
            
        desired_date_list = pd.date_range(start=start_d, end=end_d).date.tolist()
        
        dates = pd.DataFrame(desired_date_list,columns=['dates']) 
        dates['day'] = dates['dates'].apply(lambda x: str(x.day))
        dates['month'] = dates['dates'].apply(lambda x: str(x.month))
        dates['year'] = dates['dates'].apply(lambda x: str(x.year))
        grouped = dates.groupby(['year','month'])['day'].apply(lambda x: "%s" % ', '.join(x))
        month_count = 0
        for year,month in list(grouped.index):
            if month_count == 0:
                date_string = "(year = " + str(year) + " and month = " + str(month) + " and day in (" \
                + str(grouped[year,month]) + "))"
            else:
                date_string += " or (year = " + str(year) + " and month = " + str(month) + " and day in (" \
                + str(grouped[year,month]) + "))"
            month_count += 1
        date_string = "(" + date_string + ")"
    else:
        year_n = date_to_load.year; year_b = (date_to_load + timedelta(days=1)).year; 
        month_n = date_to_load.month; month_b = (date_to_load + timedelta(days=1)).month; 
        day_n = date_to_load.day; day_b = (date_to_load + timedelta(days=1)).day;
        if timezone:
            if (year_n != year_b) and (month_n != month_b):
                date_string = "((year = " + str(year_b) + " and month = " + str(month_b) + " and day = " + str(day_b) \
                + ") or (year = " + str(year_n) + " and month = " + str(month_n) + " and day = " + str(day_n) + "))"
            elif (year_n == year_b) and (month_n != month_b):
                date_string = "((year = " + str(year_n) + " and month = " + str(month_b) + " and day = " + str(day_b) \
                + ") or (year = " + str(year_n) + " and month = " + str(month_n) + " and day = " + str(day_n) + "))"
            else:
                date_string = "((year = " + str(year_n) + " and month = " + str(month_n) + " and day = " + str(day_b) \
                + ") or (year = " + str(year_n) + " and month = " + str(month_n) + " and day = " + str(day_n) + "))"
        else:
            date_string = "year = " + str(year_n) + " and month = " + str(month_n) + " and day = " + str(day_n)
        
    date_to_load = "date('" + str(date_to_load) + "')"
        
    print("The date clause string is {}.".format(date_string))
    
    return date_string, date_to_load
# ----------------------------------------------- #

def substitute_params_in_string(params_keys_list,params_values_list,string_to_sub):
    if (type(params_keys_list).__name__ != 'list'):
        raise Exception('Keys list should be of type list.')
    elif (type(params_values_list).__name__ != 'list'):
        raise Exception('Values list should be of type list.')
    key_n = 0
    for key in params_keys_list:
        key = str(key)
        char_n = 0
        for char in range(len(key)):
            re_string_toappend = "[" + str(key[char]) + "]"
            if char == 0:
                re_string = str(re_string_toappend)
#            elif char == (len(key)-1):
#                re_string = str(re_string) + str(re_string_toappend) + "[}]"
            else:
                re_string = str(re_string) + str(re_string_toappend)
            char_n += 1
        re_string = str(re_string)
        regex = re.compile(re_string)
        string_to_sub = regex.sub(str(params_values_list[key_n]),string_to_sub)
        key_n += 1
    return string_to_sub
# ----------------------------------------------- #
    
def build_cube_query(file_string, columns_to_segment_list):
    print("This func uses commas to regex and construct query, ensure that your query\
          doesnt have commas before the from clause rather than the necessary ones")
    assert type(file_string).__name__ == 'str', "File string should be of type string"
    assert type(columns_to_segment_list).__name__ == 'list', "File string should be of type list"
    
    query = open(file_string,'r') 
    query = query.read()
    
    # take all the words followed by comma before 'from' clause
    before_comma_dims = re.findall(r"[\w]+[,]", re.split(r"[f][r][o][m]",query)[0])
    before_comma_dims = [word[:-1] for word in before_comma_dims]
    # take the last word that isnt followed by comma
    last_dim = re.split(r"[f][r][o][m]",query)[0].rsplit(None, 1)[-1]
    all_dims = before_comma_dims
    all_dims.append(last_dim)
    
    # take the words that wont be included in cube
    non_cube_dimensions = [word for word in all_dims if word not in columns_to_segment_list]
    cube_dimensions = columns_to_segment_list
    
    complete_non_cube_dimensions = []
    n_dim = 0
    for dim in non_cube_dimensions:
        if n_dim == 0:
            before_dim = re.split(r"\b({})\b".format(str(dim)), re.split(r"[f][r][o][m]",query)[0])[0]
            before_dim_after_select = re.split(r"(select)",before_dim)[-1]
            before_dim_after_select_clean = re.sub(r"(\n)",'',before_dim_after_select)
            complete_non_cube_dimensions.append(str(before_dim_after_select_clean) + str(dim))
        else:
            before_dim = re.split(r"\b({})\b".format(str(dim)), re.split(r"[f][r][o][m]",query)[0])[0]
            before_dim_after_comma = re.split(r"[,]",before_dim)[-1]
            before_dim_after_comma_clean = re.sub(r"(\n)",'',before_dim_after_comma)
            complete_non_cube_dimensions.append(str(before_dim_after_comma_clean) + str(dim))
        n_dim += 1

    complete_cube_dimensions = []
    n_dim = 0
    for dim in cube_dimensions:
        if n_dim == 0:
            before_dim = re.split(r"\b({})\b".format(str(dim)), re.split(r"[f][r][o][m]",query)[0])[0]
            before_dim_after_select = re.split(r"(select)",before_dim)[-1]
            before_dim_after_select_clean = re.sub(r"(\n)",'',before_dim_after_select)
            complete_cube_dimensions.append(str(before_dim_after_select_clean) + str(dim))
        else:
            before_dim = re.split(r"\b({})\b".format(str(dim)), re.split(r"[f][r][o][m]",query)[0])[0]
            before_dim_after_comma = re.split(r"[,]",before_dim)[-1]
            before_dim_after_comma_clean = re.sub(r"(\n)",'',before_dim_after_comma)
            complete_cube_dimensions.append(str(before_dim_after_comma_clean) + str(dim))
        n_dim += 1
    
    # generate the most simple query
    n_dim = 0
    for dim in complete_non_cube_dimensions:
        if n_dim == 0:
            simple_query = "select {}".format(complete_non_cube_dimensions[n_dim])
        else:
            simple_query += ", {}".format(complete_non_cube_dimensions[n_dim])
        n_dim += 1
    
    # aggregate cube dimensions
    simple_query_list = []
    for chosen_dimension in cube_dimensions:
        n_dim = 0
        simple_query2 = simple_query
        for dim in cube_dimensions:
            if dim == chosen_dimension:
                simple_query2 += ", 'All' {}".format(cube_dimensions[n_dim])
            elif dim != chosen_dimension:
                simple_query2 += ", {}".format(cube_dimensions[n_dim])
            n_dim += 1
        simple_query_list.append(simple_query2)
        
    simple_query += re.split(r"[f][r][o][m]",query)[1]
        