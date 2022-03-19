from fastapi import FastAPI, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import random
import duckdb
from typing import Dict, List, Optional, Union, Tuple
from pypika import Table, Query, Field, Case
from pypika.enums import Order
from pypika.queries import QueryBuilder
from pypika.functions import Count, First, Max
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


def _run_query(view: QueryBuilder, max_rows=40) -> Tuple[List, List]:
    conn = _get_conn()
    sql_query = view.limit(max_rows).get_sql()
    try:
        conn.execute(sql_query)
    except RuntimeError as e:
        print(sql_query)
        raise e
    rows = conn.fetchall()
    columns = [col[0] for col in conn.description]
    return rows, columns


class Sheet:
    def __init__(self, view: QueryBuilder, source: Optional["Sheet"]):
        # TODO: source is different for different types of sheets
        self.view = view
        self.rows, self.columns = _run_query(self.view)
        self.uid = next(unique_sequence)
        self.source = source

        self.orderbys = [(field.name, order) for field, order in self.view._orderbys]

    def frequency(self, cols: List[str]) -> "FreqSheet":
        # can check if column name is in self.columns
        res = (
            Query.from_(self.view)
            .groupby(*cols)
            .select(
                *cols,
                Count("*").as_("num_rows"),
            )
            .orderby(Count("*"), order=Order.desc)
        )
        return FreqSheet(res, key_cols=cols, source=self)

    def sort(self, col_name: str, ascending: bool = True) -> "Sheet":
        order = Order.asc if ascending else Order.desc
        res = Query.from_(self.view).orderby(col_name, order=order).select("*")
        if isinstance(self, FreqSheet):
            return FreqSheet(res, self.key_cols, self.source)
        return Sheet(res, self.source)

    def filter_exact(self, filters: List[Tuple[str, Optional[str]]]) -> "Sheet":
        res = Query.from_(self.view)
        for field, keyword in filters:
            if keyword is None:
                res = res.where(Field(field).isnull())
            else:
                res = res.where(Field(field) == keyword)
        res = res.select("*")
        return Sheet(res, self)

    def pivot(self, key_cols: List[str], pivot_col: str, agg_col: str):
        """
        aggs: (field, aggfunction)
        """
        col_limit = 35
        temp = Query.from_(self.view)
        temp = temp.select(pivot_col).distinct()
        rows, cols = _run_query(temp, max_rows=col_limit + 1)
        assert len(cols) == 1
        print(len(rows), rows)
        if len(rows) > col_limit:
            raise HTTPException(
                status_code=400,
                detail=f"The pivot column needs to have less than {col_limit} unique values",
            )
        pivot_vals = [row[0] for row in rows]

        # handling NULL in pivot_vals
        cases = [
            Max(Case().when(Field(pivot_col) == val, Field(agg_col))).as_(
                ("NaN" if val is None else val)
            )
            for val in pivot_vals
        ]
        res = Query.from_(self.view).groupby(*key_cols).select(*key_cols, *cases)
        return Sheet(res, self)

    @property
    def typ(self):
        if type(self) == FreqSheet:
            return "freq"
        else:
            return "base"

    def run_op(
        self,
        operation: Union[
            "Operation", "FreqOperation", "FilterOperation", "PivotOperation"
        ],
    ) -> "Sheet":

        if isinstance(operation, FreqOperation):
            res = self.frequency(operation.cols)
            return res

        if isinstance(operation, FilterOperation):
            filters = operation.filters
            res = self.filter_exact(filters=filters)
            return res

        if isinstance(operation, PivotOperation):
            return self.pivot(
                operation.key_cols, operation.pivot_col, operation.agg_col
            )

        op = operation.operation_type
        params = operation.params

        if op == "sa":
            col_name = params
            res = self.sort(col_name, True)
        elif op == "sd":
            col_name = params
            res = self.sort(col_name, False)
        else:
            raise ValueError("Unsupported operation")

        return res


class FreqSheet(Sheet):
    def __init__(self, view: QueryBuilder, key_cols: List[str], source: "Sheet"):
        super().__init__(view, source)
        self.source = source
        self.key_cols = key_cols

    def filter_exact(self, filters: List[Tuple[str, Optional[str]]]) -> "Sheet":
        return self.source.filter_exact(filters)

    @property
    def key_col_idx(self) -> List[int]:
        return [self.columns.index(key_col) for key_col in self.key_cols]


class FreqOperation(BaseModel):
    operation_type: str = "f"
    cols: List[str]


class FilterOperation(BaseModel):
    operation_type: str = "fil"
    filters: List[Tuple[str, Optional[str]]]


class PivotOperation(BaseModel):
    operation_type: str = "pivot"
    key_cols: List[str]
    pivot_col: str
    agg_col: str


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
def post_view(
    uid: str,
    operation: Union[Operation, FreqOperation, FilterOperation, PivotOperation],
):
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
