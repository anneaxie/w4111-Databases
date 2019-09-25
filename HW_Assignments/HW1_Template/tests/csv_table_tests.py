# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from HW_Assignments.HW1_Template.src.CSVDataTable import CSVDataTable
import logging
import os
import time
import json

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.WARNING)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.WARNING)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_load():

    print("Testing load data.")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=['playerID'])

    print("Created table = " + str(csv_tbl))


def t_find_by_template():

    print("Testing find by template.")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    tmp = {'teamID': 'BOS', 'yearID': '1960'}

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    res = csv_tbl.find_by_template(template=tmp, field_list=fields)

    print("Query result = \n" + json.dumps(res, indent=2))


def test_matches():

    print("Testing matches template.")

    r = {
        "playerID": "webstra01",
        "teamID": "BOS",
        "yearID": "1960",
        "AB": "3",
        "H": "0",
        "HR": "0",
        "RBI": "1"
    }
    tmp = {'playerID': "webstra01"}

    test = CSVDataTable.matches_template(r, tmp)
    print("Matches = ", test)


def t_find_by_pk():

    print("Testing find by primary key.")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']
    fields = ['playerID', 'teamID', 'yearID', 'AB', 'H', 'HR', 'RBI']
    key_val = ['willite01', 'BOS', '1960', '1']

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    res = csv_tbl.find_by_primary_key(key_fields=key_val)

    print("Query result = \n" + json.dumps(res, indent=2))


def t_insert():

    print("Testing insert works.")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    new_row = {
        'playerID': 'yike',
        'teamID': 'yay',
        'yearID': '2019',
        'stint': '1',
        'H': '100',
        'AB': '100'
    }

    print("Testing find...")
    result = csv_tbl.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)

    print("Doing insert...")
    res = csv_tbl.insert(new_row)

    print("Testing find...")
    result = csv_tbl.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)


def t_insert_and_delete_by_template():

    print("Testing insert and delete (by template).")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    new_row = {
        'playerID': 'yike',
        'teamID': 'yay',
        'yearID': '2019',
        'stint': '1',
        'H': '100',
        'AB': '100'
    }

    print("Testing find...")
    result = csv_tbl.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)

    print("Doing insert...")
    res = csv_tbl.insert(new_row)

    print("Testing find...")
    result = csv_tbl.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)

    print("Doing delete...")
    res = csv_tbl.delete_by_template({"playerID": "yike"})

    print("Testing find again....")
    result = csv_tbl.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)


def t_insert_and_delete_by_pk():

    print("Testing insert and delete (by key).")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    new_row = {
        'playerID': 'yike',
        'teamID': 'yay',
        'yearID': '2019',
        'stint': '1',
        'H': '100',
        'AB': '100'
    }

    new_key_fields = ['yike', 'yay', '2019', '1']

    print("Testing find...")
    result = csv_tbl.find_by_primary_key(key_fields=new_key_fields)
    print("Result of find = ", result)

    print("Doing insert...")
    result = csv_tbl.insert(new_row)

    print("Testing find...")
    result = csv_tbl.find_by_primary_key(key_fields=new_key_fields)
    print("Result of find = ", result)

    print("Doing delete...")
    result = csv_tbl.delete_by_key(key_fields=new_key_fields)

    print("Testing find again....")
    result = csv_tbl.find_by_primary_key(key_fields=new_key_fields)
    print("Result of find = ", result)


def t_insert_update_delete_by_template():

    print("Testing insert, update, and delete (by template).")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    new_row = {
        'playerID': 'yike',
        'teamID': 'yay',
        'yearID': '2019',
        'stint': '1',
        'H': '100',
        'AB': '100'
    }

    print("Testing find...")
    result = csv_tbl.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)

    print("Doing insert...")
    res = csv_tbl.insert(new_row)

    print("Testing find...")
    result = csv_tbl.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)

    new_cols = {'AB': '5', 'H': '60'}

    print("Testing update, new result = ", new_cols)
    result = csv_tbl.update_by_template({"playerID": "yike"}, new_values=new_cols)
    print("Update result = ", result)

    print("Testing find again....")
    result = csv_tbl.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)

    print("Doing delete...")
    res = csv_tbl.delete_by_template({"playerID": "yike"})

    print("Testing find again....")
    result = csv_tbl.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)


def t_insert_update_delete_by_pk():

    print("Testing insert, update, and delete (by key).")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_cols = ['playerID', 'teamID', 'yearID', 'stint']

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=key_cols)

    new_row = {
        'playerID': 'yike',
        'teamID': 'yay',
        'yearID': '2019',
        'stint': '1',
        'H': '100',
        'AB': '100'
    }

    new_key_fields = ['yike', 'yay', '2019', '1']

    print("Testing find...")
    result = csv_tbl.find_by_primary_key(key_fields=new_key_fields)
    print("Result of find = ", result)

    print("Doing insert...")
    res = csv_tbl.insert(new_row)

    print("Testing find...")
    result = csv_tbl.find_by_primary_key(key_fields=new_key_fields)
    print("Result of find = ", result)

    new_cols = {'AB': '5', 'H': '60'}

    print("Testing update, new result = ", new_cols)
    result = csv_tbl.update_by_key(key_fields=new_key_fields, new_values=new_cols)
    print("Update result = ", result)

    print("Testing find...")
    result = csv_tbl.find_by_primary_key(key_fields=new_key_fields)
    print("Result of find = ", result)

    print("Doing delete...")
    res = csv_tbl.delete_by_template({"playerID": "yike"})

    print("Testing find...")
    result = csv_tbl.find_by_primary_key(key_fields=new_key_fields)
    print("Result of find = ", result)


t_load()
t_find_by_template()
test_matches()
t_find_by_pk()
t_insert()
t_insert_and_delete_by_template()
t_insert_and_delete_by_pk()
t_insert_update_delete_by_template()
t_insert_update_delete_by_pk()
