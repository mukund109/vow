import flask
from flask import request
import time
import random
import pandas as pd
import urllib.parse

# create a flask application
app = flask.Flask(__name__)

# set the template directory
app.template_folder = "templates"

df = pd.read_csv("new-voter-registrations.csv")


def _df_to_dict(sheet):
    return {"columns": sheet.columns, "rows": sheet.to_dict(orient="records")}


def _get_formatted_url(url):
    # not robust, ideally url should be parsed and reconstructed
    if url.endswith("/"):
        return url + "?"
    else:
        return url + "&"


# create a route for the application
@app.route("/")
def index():
    global df
    operations = request.args.getlist("op", type=str)
    sheet = _run_ops(df, operations)
    table_info = _df_to_dict(sheet)
    return flask.render_template(
        "table.html",
        msg="how are you feeling?",
        format_url=_get_formatted_url,
        **table_info
    )


# uri ---encodes--> original table x op x op
def _run_ops(df, operations):
    if not operations:
        return df
    op = operations[0]
    result = None

    if op == "col_info":
        result = df.describe()
    elif op.startswith("f:"):
        col_idx = int(op.split(":")[1])
        col_name = df.columns[col_idx]
        result = df[col_name].value_counts()
    else:
        raise ValueError("Unsupported operation")

    if type(result) == pd.Series:
        result = result.reset_index()

    return _run_ops(result, operations[1:])


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
