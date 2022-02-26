from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
import random
import duckdb
from typing import Dict, List, Optional, Union, Tuple
from pypika import Table, Query, Field
from pypika.enums import Order
from pypika.queries import QueryBuilder
from pypika.functions import Count
from fastapi import Request
from pydantic import BaseModel
from fastapi.responses import RedirectResponse


# create a flask application
app = FastAPI()
# set the template directory
templates = Jinja2Templates(directory="templates")


def uniqueid():
    seed = random.getrandbits(32)
    while True:
        yield str(seed)
        seed += 1


unique_sequence = uniqueid()

sheets = dict()


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


def _initialize_view(table: str) -> QueryBuilder:
    return Query.from_(table).select("*")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, reload=True)
