from fastapi import FastAPI, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import random
import duckdb
from typing import Dict, List, Optional, Union, Tuple, Literal
from pypika import Table, Query, Field, Case, Criterion
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

    try:
        rows = conn.fetchall()
    except RuntimeError as e:
        if e.args[0] == "no open result set":
            return [], []
        else:
            raise e

    columns = [col[0] for col in conn.description]
    return rows, columns


class Sheet:
    def __init__(
        self,
        view: QueryBuilder,
        source: Optional["Sheet"],
        desc: Optional[str] = None,
    ):
        # TODO: source is different for different types of sheets
        self.view = view
        self.rows, self.columns = _run_query(self.view)
        self.uid = next(unique_sequence)
        self.source = source

        # short description of operation performed on source to get this sheet
        self.desc = desc

        self.orderbys = {
            field.name: (order == Order.asc)
            for field, order in self.view._orderbys
        }

    @property
    def lineage(self) -> List["Sheet"]:
        """
        List of parent sheets + this sheet
        """
        if self.source is None:
            return [self]
        return self.source.lineage + [self]

    def __str__(self):
        if self.source is None and self.desc is None:
            return f"{self.uid}"
        if self.source is not None and self.desc is None:
            return f"{self.source}[{self.uid}]"
        if self.source is None and self.desc is not None:
            return f"{self.desc}"
        else:
            return f"{self.source}[{self.desc}]"

    def frequency(self, cols: List[str]) -> "FreqSheet":
        # can check if column name is in self.columns
        res = (
            Query.from_(self.view)
            .groupby(*cols)
            .select(
                *cols,
                Count("*").as_("num_rows"),
            )
        )
        # Instead of making the `orderby` clause part of the previous query
        # I'm putting the clause in a new query below
        # By doing I can use "num_rows" as the field to sort on, and can access
        # it from self.orderbys
        # TODO: make a test case for this
        res = (
            Query.from_(res).select("*").orderby("num_rows", order=Order.desc)
        )
        return FreqSheet(res, key_cols=cols, source=self, desc="freq")

    def sort(self, col_name: str, ascending: bool = True) -> "Sheet":
        order = Order.asc if ascending else Order.desc
        res = Query.from_(self.view).orderby(col_name, order=order).select("*")
        if isinstance(self, FreqSheet):
            return FreqSheet(res, self.key_cols, self.source)
        return Sheet(res, self.source)

    def _filter_exact(
        self,
        view,
        filters: List[Tuple[str, Optional[str]]],
        cols_to_return: Optional[List[str]],
    ) -> QueryBuilder:
        res = Query.from_(view)
        for field, keyword in filters:
            if keyword is None:
                res = res.where(Field(field).isnull())
            else:
                res = res.where(Field(field) == keyword)

        if cols_to_return is None:
            return res.select("*")

        res = res.select(*[Field(col) for col in cols_to_return])
        return res

    def filter_exact(
        self,
        filters: List[Tuple[str, Optional[str]]],
        cols_to_return: Optional[List[str]],
    ) -> "Sheet":
        res = self._filter_exact(self.view, filters, cols_to_return)

        return Sheet(res, self, desc="fil")

    def _filter_except(
        self,
        view,
        filters: List[Tuple[str, Optional[str]]],
        cols_to_return: Optional[List[str]],
    ) -> QueryBuilder:
        """
        Filters out all values except those that match `filters`
        This is like combining the filters with an OR condition
        """
        criterion = Criterion.any(
            [Field(field) == keyword for field, keyword in filters]
        )
        res = Query.from_(view).where(criterion)

        if cols_to_return is None:
            return res.select("*")

        return res.select(*[Field(col) for col in cols_to_return])

    def filter_except(
        self,
        filters: List[Tuple[str, Optional[str]]],
        cols_to_return: Optional[List[str]],
    ) -> "Sheet":
        """
        if cols_to_return is None, then return all columns
        """
        res = self._filter_except(self.view, filters, cols_to_return)
        return Sheet(res, self, desc="fil2")

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
                detail=(
                    "The pivot column needs to have less than"
                    f" {col_limit} unique values"
                ),
            )
        pivot_vals = [row[0] for row in rows]

        # handling NULL in pivot_vals
        cases = [
            Max(Case().when(Field(pivot_col) == val, Field(agg_col))).as_(
                ("NaN" if val is None else val)
            )
            for val in pivot_vals
        ]
        res = (
            Query.from_(self.view).groupby(*key_cols).select(*key_cols, *cases)
        )
        return Sheet(res, self, desc="piv")

    @property
    def typ(self):
        if type(self) == FreqSheet:
            return "freq"
        else:
            return "base"

    def run_op(
        self,
        operation: Union[
            "Operation",
            "FreqOperation",
            "FilterOperation",
            "PivotOperation",
            "FacetOperation",
        ],
    ) -> "Sheet":

        if isinstance(operation, FreqOperation):
            res = self.frequency(operation.cols)
            return res

        if isinstance(operation, FilterOperation):
            filters = operation.filters
            cols_to_return = operation.columns_to_return

            if operation.criterion == "all":
                res = self.filter_exact(
                    filters=filters, cols_to_return=cols_to_return
                )
            else:
                res = self.filter_except(
                    filters=filters, cols_to_return=cols_to_return
                )
            return res

        if isinstance(operation, PivotOperation):
            return self.pivot(
                operation.key_cols, operation.pivot_col, operation.agg_col
            )

        if isinstance(operation, FacetOperation):
            return self.facet_search(filters=operation.facets)

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
    def __init__(
        self,
        view: QueryBuilder,
        key_cols: List[str],
        source: "Sheet",
        **kwargs,
    ):
        super().__init__(view, source, **kwargs)
        self.source: Sheet = source
        self.key_cols = key_cols

    def check_for_key_cols(self, cols: List[str]):
        """
        checks whether `cols` contains all the self.key_cols
        """
        for col in self.key_cols:
            if col not in cols:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot hide key columns: {self.key_cols}",
                )

    def facet_search(
        self, filters: List[Tuple[str, Optional[str]]]
    ) -> "Sheet":
        """
        A facet search is just the behaviour that happens
        when you press 'Enter' on a row (facet) of a frequency sheet
        """
        return self.source.filter_exact(filters, cols_to_return=None)

    def filter_exact(
        self,
        filters: List[Tuple[str, Optional[str]]],
        cols_to_return: Optional[List[str]],
    ) -> "Sheet":
        """
        If a filter op is performed on a FreqSheet, it returns another
        FreqSheet with the same `source` and `key_cols`
        """

        self.check_for_key_cols(cols_to_return)
        res = self._filter_exact(self.view, filters, cols_to_return)
        return FreqSheet(
            res, key_cols=self.key_cols, source=self.source, desc="ffil"
        )

    def filter_except(
        self,
        filters: List[Tuple[str, Optional[str]]],
        cols_to_return: Optional[List[str]],
    ) -> "Sheet":
        """
        same as docs of `FreqSheet.filter_exact`
        """

        self.check_for_key_cols(cols_to_return)
        res = self._filter_except(self.view, filters, cols_to_return)
        return FreqSheet(
            res, key_cols=self.key_cols, source=self.source, desc="ffil"
        )

    @property
    def key_col_indices(self) -> List[int]:
        return [self.columns.index(key_col) for key_col in self.key_cols]


class FreqOperation(BaseModel):
    # WARNING: pydantic isn't pattern matching on the value of
    # `operation_type`. Instead its matching on the attributes
    # `operation_type` and `cols`
    # TODO: convert operation type to literal?
    operation_type: str = "f"
    cols: List[str]


class FilterOperation(BaseModel):
    operation_type: str = "fil"
    filters: List[Tuple[str, Optional[str]]]
    columns_to_return: Optional[List[str]] = None
    criterion: Literal["any", "all"] = "all"


class FacetOperation(BaseModel):
    operation_type: str = "fac"
    facets: List[Tuple[str, Optional[str]]]


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
    sheets["gta"] = Sheet(_initialize_view("test_2"), None, desc="gta")


@app.get("/")
def index():
    # redirect to initial view
    return RedirectResponse(url="/gta")


# passing uid in the body might be semantically more sensible
@app.post("/sheets/{uid}")
def post_view(
    uid: str,
    operation: Union[
        Operation,
        FreqOperation,
        FilterOperation,
        PivotOperation,
        FacetOperation,
    ],
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
app.mount("/static/", StaticFiles(directory="static"), name="site")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
