from HW_Assignments.HW1_Template.src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd
import uuid

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)


class Index:

    def __init__(self, index_name, index_columns, index_kind):
        self._data = {
            "index_name": index_name,
            "index_columns": index_columns,
            "index_kind": index_kind
        }

        self._buckets = {}

    def compute_index_value(self, row):
        vs = [row[k] for k in self._data["index_columns"]]
        i_string = ("_").join(vs)
        return i_string

    def add_row(self, rid, row):
        # i_value = self.compute_index_value(row)
        # b = self._buckets.get(i_value)
        # if b is None:
        #     b = []
        # if (self._data["index_kind"] in ["PRIMARY", "UNIQUE"]) and len(b) > 0 :
        #     raise DataTableException(DataTableException.duplicate_key, "Duplicate key in Row = " + str(row))
        # b.append(rid)
        # self._buckets(i_value) = b
        pass

    def get_by_key(self, cols):
        result = self._buckets.get(k)
        return result

    def delete_row(self, row, rid):
        key = self.compute_index_value(row)
        b = self._buckets.get(key, None)
        if b is not None:
            b.remove(rid)

    def get_key_columns(self):
        return self._data["index_columns"]


class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        self._indexes = {}

        if key_columns is not None and len(key_columns) > 0:
            self._add_index("PRIMARY", key_columns)

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1 * temp_r) - 1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_index(self, i_name, columns):
        self._indexes[i_name] = Index(i_name, columns, "PRIMARY")

    def _add_row(self, r):
        # rid = str(uuid.uuid4())
        #
        # self._rows[rid] = copy.copy(r)
        #
        # for idx in self._indexes.values():
        #     idx.add_row(rid, r)

        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _delete_from_indexes(self, rid):
        if self._indexes is not None:
            row = self._rows[rid]
            for i, iv in self._indexes.items():
                iv.delete_row(row, rid)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        delimiter = self._data["connect_info"].get('delimiter', ',')
        full_name = os.path.join(dir_info, file_n)

        t_columns = None

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:

                if t_columns is None:
                    t_cols = list(r.keys())

                    key_cols = self._data.get('key_columns', None)
                    if key_cols is not None:
                        key_cols = set(key_cols)
                        # if not key_cols.issubset(set(t_cols)):
                        #     raise DataTableException(DataTableException.invalid_key_definition,
                        #                              "Key column not in table")

                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

    def key_to_template(self, key):

        tmp = {}
        for k in self._data['key_columns']:
            new_field = {k: key[k]}
            tmp.update(new_field)

        return tmp

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    def _find_by_index(self, i_name, key_fields):
        idx = self._indexes.get(i_name)
        b = idx.get_by_key(key_fields)
        return b

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        idx = self._indexes['PRIMARY']
        key_cols = idx.get_key_columns()
        tmp = dict(zip(key_cols, key_fields))
        result = self.find_by_template(template=tmp, field_list=field_list)

        if result is not None and len(result) > 0:
            result = result[0]
        else:
            result = None

        return result

    def find_by_primary_key_fast(self, key_fields, field_list=None):
        result = self._find_by_index("PRIMARY", key_fields)
        if result:
            final_result = dict(self._rows[result[0]])
            final_result = CSVDataTable.__project(final_result, field_list)
            return final_result
        else:
            return None

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        result = []
        for r in self._rows:
            if CSVDataTable.matches_template(r, template):
                new_r = CSVDataTable.__project(r, field_list)
                result.append(new_r)

        return result

    @staticmethod
    def __project(row, field_list):

        result = {}

        if field_list is None:
            return row

        for f in field_list:
            result[f] = row[f]

        return result

    def _validate_template_and_fields(self, tmp, fields):

        c_set = set(self._data['table_columns'])

        if tmp is not None:
            t_set = set(tmp.keys())
        else:
            t_set = None

        if fields is not None:
            f_set = set(fields)
        else:
            f_set = None

        # if f_set is not None and not f_set.issubset(c_set):
        #     raise DataTableException(DataTableException.invalid_input, "Fields are invalid")
        # if t_set is not None and not t_set.issubset(c_set):
        #     raise DataTableException(DataTableException.invalid_input, "Fields are invalid")

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        #same as find_by_primary_key
        #instead of returning the key, pull it out of the array

        pass

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        # does find_my_template but deletes rows instead of returning them

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        # this part is extra credit
        # given new values, for every row that matches template,
        # replace current values with the ones that were passed in
        # update by key

        pass

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """

        if new_record is None:
            raise ValueError("There's nothing to add.")

        new_cols = set(new_record.keys())
        tbl_cols = set(self._data["key_columns"])

        if not tbl_cols.issubset(new_cols):
            raise ValueError("The column doesn't exist.")

        key_cols = self._data.get("key_columns", None)

        if key_cols is not None:
            key_cols = set(key_cols)
            if not key_cols.issubset(new_cols):
                raise ValueError("You did not provide all the necessary key columns.")

            for k in key_cols:
                if new_record.get(k, None) is None:
                    raise ValueError("Key columns cannot be null.")

            key_tmp = self.key_to_template(new_record)  # given a row returns the template
            if self.find_by_template(key_tmp) is not None and len(self.find_by_template(key_tmp)) > 0:
                raise ValueError("You're trying to insert a duplicate.")

        self._rows.append(new_record)

    def get_rows(self):
        return self._rows
