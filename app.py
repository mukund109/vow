from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from urllib.parse import unquote
import time
import random
import pandas as pd
import duckdb
from typing import Dict, List, Optional, Union, Tuple
from pypika import Table, Query, Field
from pypika.enums import Order
from pypika.queries import QueryBuilder, Selectable
from pypika.functions import Count
from fastapi import Request
from pydantic import BaseModel
from fastapi.responses import RedirectResponse


def uniqueid():
    seed = random.getrandbits(32)
    while True:
        yield str(seed)
        seed += 1


unique_sequence = uniqueid()

# create a flask application
app = FastAPI()

sheets = dict()

# set the template directory
templates = Jinja2Templates(directory="templates")

df = pd.read_csv("new-voter-registrations.csv")


class Sheet:
    def __init__(self, view: QueryBuilder, typ: str, source: Optional["Sheet"]):
        # TODO: source is different for different types of sheets
        self.view = view
        self.typ = typ

        conn = _get_conn()
        conn.execute(view.limit(20).get_sql())
        self.rows = conn.fetchall()
        self.columns = [col[0] for col in conn.description]
        self.uid = next(unique_sequence)
        self.source = source

    def frequency(self, col_name: str) -> "Sheet":
        # can check if column name is in self.columns
        res = Query.from_(self.view).groupby(col_name).select(Count("*"), col_name)
        return FreqSheet(res, self)

    def sort(self, col_name: str, ascending: bool = True) -> "Sheet":
        order = Order.asc if ascending else Order.desc
        res = Query.from_(self.view).orderby(col_name, order=order).select("*")
        return Sheet(res, "base", self)

    def filter_exact(self, field: str, keyword: str) -> "Sheet":
        res = Query.from_(self.view).where(Field(field) == keyword).select("*")
        return Sheet(res, "base", self)

    def run_op(self, operation: Tuple[str, str]) -> "Sheet":
        if not operation:
            return self

        op, params = operation

        if op == "f":
            col_name = params
            res = self.frequency(col_name)
        elif op == "sa":
            col_name = params
            res = self.sort(col_name, True)
        elif op == "sd":
            col_name = params
            res = self.sort(col_name, False)
        elif op == "fil":
            field = params.split(":")[0]
            val = ":".join(params.split(":")[1:])
            res = self.filter_exact(field=field, keyword=val)
        else:
            raise ValueError("Unsupported operation")

        return res


class FreqSheet(Sheet):
    def __init__(self, view: QueryBuilder, source: "Sheet"):
        super().__init__(view, "freq", source)
        self.source = source

    def filter_exact(self, field: str, keyword: str) -> "Sheet":
        return self.source.filter_exact(field, keyword)


class Operation(BaseModel):
    operation: Tuple[str, str]


def _get_conn():
    # TODO: closing the connection?
    return duckdb.connect("vow.db", read_only=True)


# def _table_to_template_kwargs(
#     view: QueryBuilder, conn: duckdb.DuckDBPyConnection
# ) -> Dict[str, Union[list, List[tuple]]]:
#     conn.execute(view.limit(20).get_sql())
#     rows = conn.fetchall()
#     columns = [col[0] for col in conn.description]
#     return {"columns": columns, "rows": rows}


def _get_formatted_url(url):
    # not robust, ideally url should be parsed and reconstructed
    if url.endswith("/"):
        return url + "?"
    else:
        return url + "&"


def _get_prev_url(url):
    return "op".join(url.split("op")[:-1])


def _parse_ops(ops: str) -> List[Tuple[str, str]]:
    # '/f:<params>/fil:<params>' -> [('f','<params>'), ('fil','<params>')]
    print(ops)

    if ops == "/" or ops == "":
        return []
    quoted_ops = [op.split(":") for op in ops.split("/") if op != ""]
    print(quoted_ops)

    return [(op, unquote(params)) for op, params in quoted_ops]


# passing uid in the body might be semantically more sensible
@app.post("/sheets/{uid}")
def post_view(uid: str, operation: Operation):
    prev_sheet = sheets[uid]

    new_sheet = prev_sheet.run_op(operation.operation)

    sheets[new_sheet.uid] = new_sheet

    return {"new_sheet": new_sheet.uid, "yolo": "Success"}


@app.get("/")
def index():
    # redirect to initial view
    if "gta" not in sheets:
        sheets["gta"] = Sheet(_initialize_view("gta"), "base", None)

    return RedirectResponse(url="/gta")


@app.get("/{uid}")
def get_sheet_by_uid(request: Request, uid: str):

    sheet = sheets[uid]

    return templates.TemplateResponse(
        "table.html",
        dict(
            request=request,
            msg="how are you feeling?",
            sheet=sheet,
        ),
    )


# @app.get("{ops:path}")
# def index(request: Request, ops: str):
#     # global df
#     operations = _parse_ops(ops)

#     print(operations)
#     view = _initialize_view("gta")
#     view, sheet_type = _run_ops(view, operations)
#     # table_info = _df_to_dict(sheet)
#     return templates.TemplateResponse(
#         "table.html",
#         dict(
#             request=request,
#             msg="how are you feeling?",
#             sheet_type=sheet_type,
#             **_table_to_template_kwargs(view, _get_conn()),
#         ),
#     )


# @app.get("/{table}/{ops:path}")
# def table_view(table: str, ops: str):
#     print(ops)
#     operations = [] if ops is None else _parse_ops(ops)
#     return "Table: {}\nOperations: {}".format(table, operations)


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


def _filter_exact(view: QueryBuilder, field: str, keyword: str) -> QueryBuilder:
    return Query.from_(view).where(Field(field) == keyword).select("*")


def _run_ops(
    view: QueryBuilder, operations: List[Tuple[str, str]], sheet_type: str = "base"
) -> Tuple[QueryBuilder, str]:
    if not operations:
        return view, sheet_type
    op, params = operations[0]

    # op = f:<column name>
    if op == "f":
        col_name = params
        res = _frequency(view, col_name)
        sheet_type = "freq"
    elif op == "sa":
        col_name = params
        res = _sort(view, col_name, True)
    elif op == "sd":
        col_name = params
        res = _sort(view, col_name, False)
    elif op == "fil":
        field = params.split(":")[0]
        val = ":".join(params.split(":")[1:])
        res = _filter_exact(view, field=field, keyword=val)
    else:
        raise ValueError("Unsupported operation")

    print(res.get_sql())
    return _run_ops(res, operations[1:], sheet_type)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, reload=True)
