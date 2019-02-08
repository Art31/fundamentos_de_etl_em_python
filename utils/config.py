######## Script containing functions that extract encrypted configurations #########
#
# Author: Arthur Telles
#
# Description: 
# 
# This script uses the Fernet library to read encrypted files and unlock them with
# a properly placed key.
# 
# -------------------- USAGE -------------------- #
#
##### - get_all_as_dict - #
#
# Description: Extract the encrypted configuration info using a specified key file,
# at the moment the only 2 used are default databases and survey monkey accounts.
#
# * INPUT *
# ---------
# 1. section - the type of configuration to get as dictionary
#
# * OUTPUT *
# ----------
# 1. section_dict - dictionary containing the requested info
#


# inspired from https://www.mssqltips.com/sqlservertip/5173/encrypting-passwords-for-use-with-python-and-sql-server/

import configparser
import os

def get_all_as_dict(section):
    parser = configparser.ConfigParser()
    config_path = os.path.abspath(os.path.join('..','utils','config.ini'))
#        print ('Using CONFIG.INI from: '+config_path)

    with open(config_path, 'rb') as file_object:
        l_n = 0
        for line in file_object:
            l_n += 1
            if l_n == 1:
                text = str(line.decode("utf-8"))
            else:
                text += str(line.decode("utf-8"))
    
    #parser.read(text)
    parser.read_string(text)
    section_dict = {}
    for e in parser.items(section):
        section_dict[e[0]] = e[1]
    return section_dict