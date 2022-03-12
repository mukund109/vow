where was i
* check the column names using https://github.com/duckdb/duckdb/blob/master/examples/python/duckdb-python.py#L76
* "Enter" on freq sheet should work on the entire row
* need lazy eval in case someone opens a deeply nested sheet, all source sheet queries shouldn't be run
* get as far as possible can with server-side
* re-write frontend in Elm in July?

need to think of a better way in which JS code can construct operations

use color to indicate the kind of sheet

operations todo
* filter
* pivot
* insert

# small things
* distinct counts
* multiple col shift-F
* show keypress hints (for available ops)


# potential bugs
rename columns
* if shift-F is done twice, both columns are called `count_star()`

frontend-interactivity
* how would column shifting work?
* hiding columns

# how will these be tackled?
column types
* if we make all of them string, how will we plot floats
* NaN values
* operations on columns

interface for excluding columns

[Base]
[Freq]
-> special "Enter" action - filter action on source table

[Base]
-> `|` (regex, column) -> [Base]
-> `F` (one or more columns) -> [Freq]
-> `W` (key cols..., target col, agg col, agg op) [Pivot?]

# few interesting things
RestrictedPython package
https://stackoverflow.com/questions/63160370/how-can-i-accept-and-run-users-code-securely-on-my-web-app

there's an easy way to check the column names in duckdb, and their types
https://github.com/duckdb/duckdb/blob/master/examples/python/duckdb-python.py#L76
