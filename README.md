where was i
* implement QOL changes

pre-release checklist
* [FH] histogram bars
* [FM] tooltip on sort sign?
* [FM] button to open new sheet with filters
* [FM] add buttons for toggling width of column with shortcut tooltips
* [FM] column hover - buttons and [shortcut], and "help"
* [FM] freeze header row
* [FM] multi-line cols should remain multi-line after an operation
* [F] columns have the same width, to make them easier to see
* [F] cols should be first rendered with a default max-width with overflow set to hidden
* [F] on zoom, sheet window size shouldn't change
* [F] all "help" should link to command palette
* [F] links to previous sheet to have shorter names with tooltip
* [F] chrome loading issue? (going back after regex operation keeps the search bar open but out of focus)
* [F] mouse should hide itself
* [F] indicator of multiline col
* [F] don't scroll cell into view if mouse hovers over it

* [BH] show total num rows and other useful info
* [BH] some kind of pagination
* [BH] download as csv/parquet, load table into pandas
* [BH] show multiple datasets on homepage
* [B] col dtypes

todo
* refactor - move js logic from Alpine.bind into Alpine.data
* BUG: filtering on NA doesn't work
* BUG: freq of freq - "num_rows" duplicate name
* low priority: can hidden cols be key cols? shouldn't matter
* check the column names using https://github.com/duckdb/duckdb/blob/master/examples/python/duckdb-python.py#L76
* need lazy eval in case someone opens a deeply nested sheet, all source sheet queries shouldn't be run
* links to previous sheets
  * list of previous sheets should not change on sort
  * browser history can still change

* changing order of cols
  * are these preserved when sharing sheet?

* insert values
* column summary info
* left,right hover-on-scroll div

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

# how will these be tackled?
column types
* if we make all of them string, how will we plot floats
* NaN values
* operations on columns

interface for excluding columns

# few interesting things
RestrictedPython package
https://stackoverflow.com/questions/63160370/how-can-i-accept-and-run-users-code-securely-on-my-web-app

there's an easy way to check the column names in duckdb, and their types
https://github.com/duckdb/duckdb/blob/master/examples/python/duckdb-python.py#L76

duckdb-wasm (READ thoroughly)
https://duckdb.org/2021/10/29/duckdb-wasm.html

use this for fully-client side vow?
