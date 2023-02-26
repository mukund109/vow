where was i
* add more buttons (regex search 2 buttons - enter and escape)

add warning for vimium users

Lighthouse audit revealed several optimizations

design of column dropdown
* Select column
* Histogram of this column
* Histogram of all selected columns

bug
* apostrophe in "display_name" of demo datasets throws off sql syntax

* explore
  * row numbers (duckdb row_numbers())
  * make histogram cover entire row in freq table (was having trouble getting the css right, the color of the td element was taking precedence over color of tr)
slightly bigger challenges
  * versioning javascript files
  * if html is cached for a long time, how will html/js updates happen on client side?
  * wrapped_col_indices implementation seems brittle, need a straightforward way of initializing browser state from the server, same with key_cols implementation
    * col should be wrapped even if js is not loaded
  * use $persist and $scroll provided by alpinejs
  * way to embed table in html
  * write some blogs with embedded table

make tables immutable? and cache the f out of them
pre-release checklist
* [FH] display large numbers nicely
* [FM] tooltip on sort sign?
* [FM] button to open new table with filters
* [FM] add buttons for toggling width of column with shortcut tooltips
* [FM] column hover - buttons and [shortcut], and "help"
* [FM] multi-line cols should remain multi-line after an operation
* [F] on zoom, table window size shouldn't change
* [F] all "help" should link to command palette
* [F] links to previous table to have shorter names with tooltip
* [F] chrome loading issue? (going back after regex operation keeps the search bar open but out of focus)
* [F] mouse should hide itself
* [F] indicator of multi-line col
* [F] load table into pandas
* refactor - write tests
* refactor performOp functions (some take arguments when they should be using self.<attribute>)
* refactor, communicate with backend using actions rather than operations (e.g. openCell instead of facetOperation)

* [BH] implement Cache-control
* [B] show useful info like col-types
* [B] download as parquet
* [B] col dtypes

todo
* refactor - move js logic from Alpine.bind into Alpine.data
* BUG: filtering on NA doesn't work
* BUG: freq of freq - "num_rows" duplicate name
* low priority: can hidden cols be key cols? shouldn't matter
* check the column names using https://github.com/duckdb/duckdb/blob/master/examples/python/duckdb-python.py#L76
* need lazy eval in case someone opens a deeply nested table, all source table queries shouldn't be run
* links to previous tables
  * list of previous tables should not change on sort
  * browser history can still change

* changing order of cols
  * are these preserved when sharing table?

* insert values
* column summary info
* left,right hover-on-scroll div

* get as far as possible can with server-side

display message if server responds with 400 code on POST request
example of message - "pivot column needs to have less than X values"

check for SQL injection

need to think of a better way in which JS code can construct operations

use color to indicate the kind of table

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

## persisting changes
This is a complex issue
  * Some table attributes are used for re-creating the table e.g.  view, query_params, dbtype
  * Other table attributes inform the look of the table e.g. wrapped_col_indices, desc
* the `source` attribute is used to re-create the lineage
  * there's still no explicit way to find which operation was used to transform older table into current one. This info is implicitly stored in desc

There are also in-memory tables created at the start of the application. Should they be persisted? No
Its ok if the master and about table cannot be re-created from storage
Only need to make sure that the uid for these tables is mapped to their in-memory objects for lookup

For now, doing the following:
Persist all table attributes, including visual ones
Don't persist in-memory tables (i.e. tables where dbtype = 'memory')

It may lead to breaking changes in the future
* when attribute name changes e.g. `desc` -> `description`
* or when attribute value changes e.g. `gta[fil]` -> `filter`
may need to migrate older database to new one, or start from scratch

may have to change the way I'm persisting these
Instead of storing UID -> query, source
I should store UID -> operation, source
That way I'll only have to worry about keeping operations backward compatible, and making sure that the source table implements that operation
