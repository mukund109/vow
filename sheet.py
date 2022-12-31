from functools import lru_cache
import random
from typing import Callable, List, Optional, Union, Tuple, Literal
from duckdb import DuckDBPyConnection
from fastapi import HTTPException
from pypika.queries import QueryBuilder
from pypika.functions import Count, Max
from pypika.enums import Order
from pypika import (
    Query,
    Field,
    Case,
    Criterion,
    CustomFunction,
    Parameter,
)
from pydantic import BaseModel


class FreqOperation(BaseModel):
    # WARNING: pydantic isn't pattern matching on the value of
    # `operation_type`. Instead its matching on the attributes
    # `operation_type` and `cols`
    # TODO: convert operation type to literal?
    # TODO: separate sort operation
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


class RegexSearchOperation(BaseModel):
    operation_type: str = "search"
    col: str
    regex: str
    columns_to_return: Optional[List[str]] = None


class Operation(BaseModel):
    operation_type: str
    params: str


OperationsType = Union[
    Operation,
    RegexSearchOperation,
    PivotOperation,
    FacetOperation,
    FilterOperation,
    FreqOperation,
]

regexp_matches = CustomFunction("regexp_matches", ["string", "regex"])


def _uniqueid():
    seed = random.getrandbits(32)
    while True:
        yield str(seed)
        seed += 1


_unique_sequence = _uniqueid()


def _execute_query(
    conn: DuckDBPyConnection,
    view: QueryBuilder,
    query_params: Optional[List[str]] = None,
) -> Tuple[List, List]:
    sql_query = view.get_sql()

    if query_params is None:
        query_params = []

    try:
        conn.execute(sql_query, query_params)
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
        query_params: Optional[List[str]] = None,  # for parameterized queries
        desc: Optional[str] = None,
        get_db_connection: Optional[Callable[[], DuckDBPyConnection]] = None,
    ):
        """
        Need to provide a database connection when instantiating.
        If not provided, will attempt to use the connection from the
        source sheet and throw an error if that doesn't exist
        """
        self.view = view

        self.query_params = query_params or []
        self.uid = next(_unique_sequence)
        self.source = source

        # short description of operation performed on source to get this sheet
        self.desc = desc

        self.orderbys = {
            field.name: (order == Order.asc)
            for field, order in self.view._orderbys
        }

        self.get_db_connection = self._infer_db(get_db_connection)

    def __len__(self):

        view = Query.from_(self.view).select(
            Count("*").as_("num_rows"),
        )
        rows, _ = _execute_query(
            self.get_db_connection(),
            view,
            query_params=self.all_query_params(),
        )
        first_row = rows[0]
        return first_row[0]

    def _infer_db(
        self, get_db_connection: Optional[Callable[[], DuckDBPyConnection]]
    ) -> Callable[[], DuckDBPyConnection]:
        if get_db_connection is not None:
            return get_db_connection
        if self.source is not None:
            return self.source.get_db_connection
        raise ValueError("No database connection provided")

    @lru_cache
    def __getitem_cached__(self, slice_rep):
        s = slice(*slice_rep[1])

        view = self.view[s]

        if not isinstance(view, QueryBuilder):
            raise Exception(f"view has unexpected type {type(view)}")
        rows, columns = _execute_query(
            self.get_db_connection(),
            view,
            query_params=self.all_query_params(),
        )
        self.columns = columns
        return rows, columns

    def __getitem__(self, s):
        # slice_rep is a hashable version of s
        # which makes it compatible with lru_cache
        slice_rep = s.__reduce__()

        return self.__getitem_cached__(slice_rep)

    def __hash__(self):
        return hash(self.uid)

    @property
    def lineage(self) -> List["Sheet"]:
        """
        List of parent sheets + this sheet
        """
        if self.source is None:
            return [self]
        return self.source.lineage + [self]

    @property
    def parent(self) -> Optional["Sheet"]:
        """
        Returns parent sheet if it exists otherwise returns self
        """
        lineage = self.lineage
        if len(lineage) <= 1:
            return self
        return lineage[-2]

    def all_query_params(self) -> List[str]:
        if self.source is None:
            return self.query_params
        return self.source.all_query_params() + self.query_params

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
        return Sheet(res, self.source, query_params=self.query_params)

    def _filter_exact(
        self,
        view,
        filters: List[Tuple[str, Optional[str]]],
        cols_to_return: Optional[List[str]],
    ) -> QueryBuilder:
        qry = Query.from_(view)
        for field, keyword in filters:
            if keyword is None:
                qry = qry.where(Field(field).isnull())
            else:
                qry = qry.where(Field(field) == keyword)

        if cols_to_return is None:
            return qry.select("*")

        qry = qry.select(*[Field(col) for col in cols_to_return])
        return qry

    def filter_exact(
        self,
        filters: List[Tuple[str, Optional[str]]],
        cols_to_return: Optional[List[str]],
    ) -> "Sheet":
        qry = self._filter_exact(self.view, filters, cols_to_return)

        return Sheet(qry, self, desc="fil")

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

    def _filter_regex(
        self,
        view,
        column: str,
        regex: str,
        cols_to_return: Optional[List[str]],
    ) -> QueryBuilder:
        qry = Query.from_(view).where(
            regexp_matches(Field(column), Parameter("?"))
        )

        if cols_to_return is None:
            qry = qry.select("*")
        else:
            qry = qry.select(*[Field(col) for col in cols_to_return])

        return qry

    def filter_regex(
        self, column: str, regex: str, cols_to_return: Optional[List[str]]
    ) -> "Sheet":
        qry = self._filter_regex(self.view, column, regex, cols_to_return)
        return Sheet(qry, self, query_params=[regex], desc="search")

    def pivot(self, key_cols: List[str], pivot_col: str, agg_col: str):
        """
        aggs: (field, aggfunction)
        """
        col_limit = 35
        temp = Query.from_(self.view)
        temp = temp.select(pivot_col).distinct()
        rows, cols = _execute_query(
            self.get_db_connection(), temp[: col_limit + 1]
        )
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
        operation: OperationsType,
    ) -> "Sheet":

        if isinstance(operation, FreqOperation):
            return self.frequency(operation.cols)

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

        if isinstance(operation, RegexSearchOperation):
            return self.filter_regex(
                column=operation.col,
                regex=operation.regex,
                cols_to_return=operation.columns_to_return,
            )

        if isinstance(operation, Operation):
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

        raise ValueError(f"Unsupported operation for sheet type f{type(self)}")


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

        if cols_to_return is not None:
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

        if cols_to_return is not None:
            self.check_for_key_cols(cols_to_return)
        res = self._filter_except(self.view, filters, cols_to_return)
        return FreqSheet(
            res, key_cols=self.key_cols, source=self.source, desc="ffil"
        )

    def filter_regex(
        self, column: str, regex: str, *args, **kwargs
    ) -> "FreqSheet":
        res = self._filter_regex(self.view, column, regex, *args, **kwargs)
        return FreqSheet(
            res,
            key_cols=self.key_cols,
            source=self.source,
            query_params=[regex],
            desc="fsearch",
        )

    def sort(self, col_name: str, ascending: bool = True) -> "FreqSheet":
        order = Order.asc if ascending else Order.desc
        res = Query.from_(self.view).orderby(col_name, order=order).select("*")
        return FreqSheet(
            res, self.key_cols, self.source, query_params=self.query_params
        )

    @property
    def key_col_indices(self) -> List[int]:
        return [self.columns.index(key_col) for key_col in self.key_cols]

    def run_op(
        self,
        operation: OperationsType,
    ) -> "Sheet":
        if isinstance(operation, FacetOperation):
            return self.facet_search(operation.facets)

        return super().run_op(operation)