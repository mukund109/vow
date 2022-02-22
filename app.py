import flask
from flask import request
import time
import random
import pandas as pd
import duckdb
from typing import Dict, List, Union
from pypika import Table, Query
from pypika.enums import Order
from pypika.queries import QueryBuilder, Selectable
from pypika.functions import Count

# create a flask application
app = flask.Flask(__name__)

# set the template directory
app.template_folder = "templates"

df = pd.read_csv("new-voter-registrations.csv")


def _get_conn():
    # TODO: closing the connection?
    return duckdb.connect("vow.db", read_only=True)


def _table_to_template_kwargs(
    view: QueryBuilder, conn: duckdb.DuckDBPyConnection
) -> Dict[str, Union[list, List[tuple]]]:
    conn.execute(view.limit(20).get_sql())
    rows = conn.fetchall()
    columns = [col[0] for col in conn.description]
    return {"columns": columns, "rows": rows}


def _get_formatted_url(url):
    # not robust, ideally url should be parsed and reconstructed
    if url.endswith("/"):
        return url + "?"
    else:
        return url + "&"


# create a route for the application
@app.route("/")
def index():
    # global df
    operations = request.args.getlist("op", type=str)
    view = _initialize_view("gta")
    view = _run_ops(view, operations)
    # table_info = _df_to_dict(sheet)
    return flask.render_template(
        "table.html",
        msg="how are you feeling?",
        format_url=_get_formatted_url,
        **_table_to_template_kwargs(view, _get_conn()),
    )


def _df_to_dict(sheet):
    return {"columns": sheet.columns, "rows": sheet.to_dict(orient="records")}


# uri ---encodes--> original table x op x op
def _run_ops_df(df, operations):
    if not operations:
        return df
    op = operations[0]
    result = None

    if op == "col_info":
        result = df.describe()
    elif op.startswith("f:"):
        col_name = op.split(":")[1]
        result = df[col_name].value_counts()
    else:
        raise ValueError("Unsupported operation")

    if type(result) == pd.Series:
        result = result.reset_index()

    return _run_ops(result, operations[1:])


def _initialize_view(table: str) -> QueryBuilder:
    return Query.from_(table).select("*")


def _frequency(view: QueryBuilder, col_name: str) -> QueryBuilder:
    res = Query.from_(view).groupby(col_name).select(Count("*"), col_name)
    return res


def _sort(view: QueryBuilder, col_name: str, ascending: bool = True) -> QueryBuilder:
    order = Order.asc if ascending else Order.desc
    res = Query.from_(view).orderby(col_name, order=order).select("*")
    return res


def _run_ops(view: QueryBuilder, operations) -> QueryBuilder:
    if not operations:
        return view
    op = operations[0]

    # op = f:<column name>
    if op.startswith("f:"):
        col_name = "".join(op.split(":")[1:])
        res = _frequency(view, col_name)
    elif op.startswith("sa:"):
        col_name = "".join(op.split(":")[1:])
        res = _sort(view, col_name, True)
    elif op.startswith("sd:"):
        col_name = "".join(op.split(":")[1:])
        res = _sort(view, col_name, False)
    else:
        raise ValueError("Unsupported operation")

    print(res.get_sql())
    return _run_ops(res, operations[1:])


# create a post route for the application
@app.route("/clicked", methods=["POST", "GET"])
def post():
    if request.method == "GET":
        return flask.render_template("index.html", msg="yolo")
    # get the data from the form
    data = request.form
    print(data)
    # return the data
    return "Success! You entered: {}".format(data)


names = ["here", "there", "everywhere"]


@app.route("/search", methods=["GET"])
def search():
    time.sleep(random.randint(1, 2))
    query = str(request.args.get("q")).lower()
    results = []
    if query != "":
        results = [x for x in names if query in x]
    return flask.render_template("search.html", results=results)


if __name__ == "__main__":
    app.run(debug=True)
