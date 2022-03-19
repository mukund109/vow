from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
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
from utils import fetch_data


# create a flask application
app = FastAPI()
# set the template directory
templates = Jinja2Templates(directory="templates")

fetch_data()


def uniqueid():
    seed = random.getrandbits(32)
    while True:
        yield str(seed)
        seed += 1


unique_sequence = uniqueid()

sheets: Dict[str, "Sheet"] = dict()


class Sheet:
    def __init__(self, view: QueryBuilder, source: Optional["Sheet"]):
        # TODO: source is different for different types of sheets
        self.view = view

        conn = _get_conn()
        try:
            conn.execute(view.limit(40).get_sql())
        except RuntimeError as e:
            print(self.view.limit(40).get_sql())
            raise e
        self.rows = conn.fetchall()
        self.columns = [col[0] for col in conn.description]
        self.uid = next(unique_sequence)
        self.source = source

        self.orderbys = [(field.name, order) for field, order in self.view._orderbys]

    def frequency(self, cols: List[str]) -> "FreqSheet":
        # can check if column name is in self.columns
        res = (
            Query.from_(self.view)
            .groupby(*cols)
            .select(Count("*").as_("row_count"), *cols)
            .orderby(Count("*"), order=Order.desc)
        )
        return FreqSheet(res, key_cols=cols, source=self)

    def sort(self, col_name: str, ascending: bool = True) -> "Sheet":
        order = Order.asc if ascending else Order.desc
        res = Query.from_(self.view).orderby(col_name, order=order).select("*")
        if isinstance(self, FreqSheet):
            return FreqSheet(res, self.key_cols, self.source)
        return Sheet(res, self.source)

    def filter_exact(self, field: str, keyword: Optional[str]) -> "Sheet":
        if keyword is None:
            res = Query.from_(self.view).where(Field(field).isnull()).select("*")
        else:
            res = Query.from_(self.view).where(Field(field) == keyword).select("*")
        return Sheet(res, self)

    @property
    def typ(self):
        if type(self) == FreqSheet:
            return "freq"
        else:
            return "base"

    def run_op(
        self, operation: Union["Operation", "FreqOperation", "FilterOperation"]
    ) -> "Sheet":

        if isinstance(operation, FreqOperation):
            res = self.frequency(operation.cols)
            return res

        if isinstance(operation, FilterOperation):
            col = operation.col
            keyword = operation.keyword
            res = self.filter_exact(field=col, keyword=keyword)
            return res

        op = operation.operation_type
        params = operation.params

        if op == "sa":
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
    def __init__(self, view: QueryBuilder, key_cols: List[str], source: "Sheet"):
        super().__init__(view, source)
        self.source = source
        self.key_cols = key_cols

    def filter_exact(self, field: str, keyword: str) -> "Sheet":
        return self.source.filter_exact(field, keyword)


class FreqOperation(BaseModel):
    operation_type: str = "f"
    cols: List[str]


class FilterOperation(BaseModel):
    operation_type: str = "fil"
    col: str
    keyword: Optional[str]


class Operation(BaseModel):
    operation_type: str
    params: str


def _get_conn():
    # TODO: closing the connection?
    conn = duckdb.connect("vow.db", read_only=True)
    conn.execute("PRAGMA default_null_order='NULLS LAST'")
    return conn


def _initialize_view(table: str) -> QueryBuilder:
    return Query.from_(table).select("*")


if "gta" not in sheets:
    sheets["gta"] = Sheet(_initialize_view("test_2"), None)


@app.get("/")
def index():
    # redirect to initial view
    return RedirectResponse(url="/gta")


# passing uid in the body might be semantically more sensible
@app.post("/sheets/{uid}")
def post_view(uid: str, operation: Union[Operation, FreqOperation, FilterOperation]):
    prev_sheet = sheets[uid]

    new_sheet = prev_sheet.run_op(operation)

    sheets[new_sheet.uid] = new_sheet

    return {"new_sheet": new_sheet.uid, "yolo": "Success"}


@app.get("/{uid}")
def get_sheet_by_uid(request: Request, uid: str):

    sheet = sheets[uid]

    return templates.TemplateResponse(
        "table.html",
        dict(
            request=request,
            msg="vow",
            sheet=sheet,
        ),
    )


app.mount("/js/", StaticFiles(directory="js"), name="javascript")
app.mount("/", StaticFiles(directory="static"), name="site")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
