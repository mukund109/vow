where was i
* "Enter" on freq sheet doesn't work if its sorted
* use `encodeURIComoponent` to encode query strings and path names

need to think of a better way in which JS code can construct operations

use color to indicate the kind of sheet

operations todo
* shift-F - enter * filter by exact match
* pivot
* sort
* insert

# potential bugs
rename columns
* problem - if shift-F is done twice, both columns are called `count_star()`

frontend-interactivity
* how would column shifting work?
* hiding columns

# potential features
* add a button for assigning to curators after filtering

# how will these be tackled?
column types
* if we make all of them string, how will we plot floats like age
* NaN values
* operations on columns
* passing long exact matches by url

interface for excluding columns

polly-wide updates

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
