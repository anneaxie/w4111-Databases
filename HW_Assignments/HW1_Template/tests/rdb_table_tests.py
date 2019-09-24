from HW_Assignments.HW1_Template.src.RDBDataTable import RDBDataTable
import HW_Assignments.HW1_Template.src.dbutils as dbutils

import pymysql
import json

import logging

# logging.basicConfig(level=logging.WARNING)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# logger.setLevel(logging.WARNING)


def t1():

    print("Testing load data")

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    print("RDB table = ", r_dbt)


def t2():

    print("Testing find by template")

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    res = r_dbt.find_by_template({"nameLast": "Williams", "birthCity": "San Diego"},
                                 field_list=['playerId', 'nameLast', 'nameFirst', 'birthCity', 'birthYear'])
    print("Res = ", json.dumps(res, indent=2))


def t3():

    print("Testing find by key")

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("Batting", connect_info=c_info,
                         key_columns=['playerID', 'teamID', 'yearID', 'stint'])
    res = r_dbt.find_by_primary_key(key_fields=['willite01', 'BOS', '1960', '1'],
                                    field_list=['playerID', 'teamID', 'yearID', 'stint', 'H', 'AB'])
    print("Res = ", json.dumps(res, indent=2))


def t4():

    print("Testing insert works")

    new_row = {"playerID": "willite01", "teamID": "BOS", "USMC": "Cool"}
    sql, args = dbutils.create_insert("People", new_row=new_row)
    print("SQL = ", sql)
    print("args = ", args)


def t5():

    print("Testing insert and delete (by template)")

    new_row = {"playerID": "yike", "nameLast": "xie", "nameFirst": "anne"}

    print("Row to insert = ", new_row)

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])

    print("Testing find...")
    result = r_dbt.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)

    print("Doing insert...")
    result = r_dbt.insert(new_row)

    print("Testing find again...")
    result = r_dbt.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)

    print("Testing delete...")
    result = r_dbt.delete_by_template({"playerID": "yike"})
    print("Delete result = ", result)

    print("Testing find again...")
    result = r_dbt.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)


def t6():

    print("Testing update works")

    new_cols = {"playerID": "yike", "birthCountry": "Neptune"}
    tmp = {"playerID": "willite01"}

    sql, args = dbutils.create_update("People", template=tmp, changed_cols=new_cols)
    print("SQL = ", sql)
    print("Args = ", args)


def t7():

    print("Testing insert, update, and delete (by template)")

    new_row = {"playerID": "yike", "nameLast": "xie", "nameFirst": "anne"}

    print("Row to insert = ", new_row)

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])

    print("Testing find...")
    result = r_dbt.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)

    print("Doing insert...")
    result = r_dbt.insert(new_row)

    print("Testing find again...")
    result = r_dbt.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)

    new_cols = {"nameLast": "Bonk", "birthCountry": "Neptune"}

    print("Testing update, new result = ", new_cols )
    result = r_dbt.update_by_template({"playerID": "yike"}, new_values=new_cols)
    print("Update result = ", result)

    print("Testing find again...")
    result = r_dbt.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)

    print("Testing delete...")
    result = r_dbt.delete_by_template({"playerID": "yike"})
    print("Delete result = ", result)

    print("Testing find again...")
    result = r_dbt.find_by_template({"playerID": "yike"})
    print("Result of find = ", result)


def t8():

    print("Testing insert and delete (by key)")

    new_row = {"playerID": "yike", "teamID": "YAY", "yearID": "2019", "stint": "1", "H": "1", "AB": "1"}
    new_key_fields = ['yike', 'YAY', '2019', '1']
    new_field_list = ['playerID', 'teamID', 'yearID', 'stint', 'H', 'AB']

    print("Row to insert = ", new_row)

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("Batting", connect_info=c_info, key_columns=['playerID', 'teamID', 'yearID', 'stint'])

    print("Testing find...")
    result = r_dbt.find_by_primary_key(key_fields=new_key_fields, field_list=new_field_list)
    print("Result of find = ", result)

    print("Doing insert...")
    result = r_dbt.insert(new_row)

    print("Testing find again...")
    result = r_dbt.find_by_primary_key(key_fields=new_key_fields, field_list=new_field_list)
    print("Result of find = ", result)

    print("Testing delete...")
    result = r_dbt.delete_by_key(key_fields=new_key_fields)
    print("Delete result = ", result)

    print("Testing find again...")
    result = r_dbt.find_by_primary_key(key_fields=new_key_fields, field_list=new_field_list)
    print("Result of find = ", result)


def t9():

    print("Testing insert, update, and delete (by key)")

    new_row = {"playerID": "yike", "teamID": "YAY", "yearID": "2019", "stint": "1", "H": "1", "AB": "1"}
    new_key_fields = ['yike', 'YAY', '2019', '1']
    new_field_list = ['playerID', 'teamID', 'yearID', 'stint', 'H', 'AB']

    print("Row to insert = ", new_row)

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("Batting", connect_info=c_info, key_columns=['playerID', 'teamID', 'yearID', 'stint'])

    print("Testing find...")
    result = r_dbt.find_by_primary_key(key_fields=new_key_fields, field_list=new_field_list)
    print("Result of find = ", result)

    print("Doing insert...")
    result = r_dbt.insert(new_row)

    print("Testing find again...")
    result = r_dbt.find_by_primary_key(key_fields=new_key_fields, field_list=new_field_list)
    print("Result of find = ", result)

    new_cols = {"H": "5", "AB": "10"}

    print("Testing update, new result = ", new_cols )
    result = r_dbt.update_by_key(key_fields=new_key_fields, new_values=new_cols)
    print("Update result = ", result)

    print("Testing find again...")
    result = r_dbt.find_by_primary_key(key_fields=new_key_fields, field_list=new_field_list)
    print("Result of find = ", result)

    print("Testing delete...")
    result = r_dbt.delete_by_key(key_fields=new_key_fields)
    print("Delete result = ", result)

    print("Testing find again...")
    result = r_dbt.find_by_primary_key(key_fields=new_key_fields, field_list=new_field_list)
    print("Result of find = ", result)

t1()
t2()
t3()
t4()
t5()
t6()
t7()
t9()


