where was i
* BUG: filtering on NA doesn't work
* BUG: freq of freq - "num_rows" duplicate name
* low priority: can hidden cols be key cols? shouldn't matter
* check the column names using https://github.com/duckdb/duckdb/blob/master/examples/python/duckdb-python.py#L76
* need lazy eval in case someone opens a deeply nested sheet, all source sheet queries shouldn't be run
* arrow key navigation
* shortcuts for opening study-metadata, sample-metadata
* doc-sidebar
* links to previous sheets
  * list of previous sheets should not change on sort
  * browser history can still change
* some kind of pagination

* filter based on regex
* changing order of cols
  * are these preserved when sharing sheet?
* remember last active cell in table
* insert values
* col dtypes
* histogram bars
* column summary info
* left,right hover-on-scroll div
* column hover - buttons and [shortcut], and "help"
* all "help" should link to command palette

* re-write frontend in Elm in July?
* get as far as possible can with server-side

display message if server responds with 400 code on POST request
example of message - "pivot column needs to have less than X values"

check for SQL injection

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
* on pressing and holding just Shift, it shows which aggregation functions are applied to which columns

# big things
make this run in browser with pyiodide and wasm

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

duckdb-wasm (READ thoroughly)
https://duckdb.org/2021/10/29/duckdb-wasm.html

use this for fully-client side vow?
