# W4111_F19_HW1
Anne Xie
aax2000

Note: unit_tests.py should not be used for testing! It has some functions
that I had used originally to test the CSVDataTable, but I later moved
all of these functions to csv_table_tests.py and added more tests to this 
file. 
------------------------------------------------------------------
RDBDataTable

The RDBDataTable connects to MySQL to find, delete, update, and insert data 
into tables.

I decided not to do too much error catching, as I'm assuming that inputs are valid
(ie. no duplicate keys) for most cases. The unit tests that I created work, as
they take in valid inputs.

All _key functions are dependent on their corresponding _template functions.
The key is first mapped to a template.

I used helper functions imported from dbutils.py, which can be found in the
src folder. Most of these helper functions were found from the Notebook online,
Piazza, and/or office hours. 

The tests I ran can be found in rdb_table_tests.py, and the results of
these tests can be found in rdb_table_test.txt. Each of the tests has
comments in the code that shows what each test tests (since the tests are
numbered in the case of rdb_table_tests.py).

------------------------------------------------------------------
CSVDataTable

The CSVDataTable reads a CSV file to find, delete, update, and insert data
into tables.

Again, I decided not to do too much error checking, as I assume that the inputs
are valid (in the tests that I do in csv_table_tests.py, the inputs are valid).
For instance, update_by_key does not check to see whether the keys are duplicate.
delete_by_template does not check to see whether the template exists. 
In the case of delete_by_template, nothing actually happens if the template
that is being deleted is not actually found in the CSV file. I do this with 
the assumption that user inputs are valid and do in fact exist. 

Again, as with RDBDataTable, all _key functions are dependent on their
corresponding _template functions. The key is first mapped to a template.

I decided not to implement a save function, as I assume that the CSV file
is not meant to be permanently modified. The results of each function call 
(eg. update_by_key, update_by_template, delete_by_key, etc.) can be found
in the output of the tests that are completed (there is an output for every
function that is called in csv_table_tests.py). The results to the tests
can be found in csv_table_test.txt. 

