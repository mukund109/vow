import io
import csv
import json
import pickle
import hashlib
from dataclasses import asdict, dataclass, field, fields
from functools import lru_cache
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
    Tuple,
    Literal,
    Self,
)
from duckdb import DuckDBPyConnection
import duckdb
from fastapi import HTTPException
from pypika.queries import QueryBuilder
from pypika.queries import Column as QueryColumn
from pypika.functions import Cast, Count, Max
from pypika.enums import Order
from pypika import (
    Query,
    Field,
    Case,
    Criterion,
    CustomFunction,
    Parameter,
    analytics,
)
from pydantic import BaseModel
from enum import StrEnum


def load_demo_datasets():
    with open("demo_datasets.json", "r") as f:
        return json.load(f)


DBType = Literal["memory", "disk"]

class ColType(StrEnum):
    BIGINT = "BIGINT"
    BOOLEAN = "BOOLEAN"
    BLOB = "BLOB"
    DATE = "DATE"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    HUGEINT = "HUGEINT"
    INTEGER = "INTEGER"
    REAL = "REAL"
    SMALLINT = "SMALLINT"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    TINYINT = "TINYINT"
    UBIGINT = "UBIGINT"
    UINTEGER = "UINTEGER"
    USMALLINT = "USMALLINT"
    UTINYINT = "UTINYINT"
    UUID = "UUID"
    VARCHAR = "VARCHAR"

def get_conn() -> DuckDBPyConnection:
    conn = duckdb.connect("vow.db", read_only=True)
    conn.execute("PRAGMA default_null_order='NULLS LAST'")
    return conn


_conn_memory = duckdb.connect(":memory:")
_conn_memory.execute("PRAGMA default_null_order='NULLS LAST'")


def get_in_memory_conn() -> DuckDBPyConnection:
    return _conn_memory.cursor()


class Store:
    # TODO: for aliases/names, don't store copies
    # store a pointer to uid?
    def __init__(self):
        self.in_memory_db: Dict[str, Any] = {}
        self.db: Dict[str, bytes] = {}

    def put_in_memory(self, key: str, obj: Any):
        self.in_memory_db[key] = obj

    def put(self, key: str, obj: bytes):
        self.db[key] = obj

    def get(self, key: str) -> Any:
        if key in self.in_memory_db:
            return self.in_memory_db[key]

        return self.db[key]

    def __repr__(self) -> str:
        return (
            f"In-memory: {self.in_memory_db.keys()}\nOthers: {self.db.keys()}"
        )


table_store = Store()


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


class OpenOperation(BaseModel):
    operation_type: str = "open"
    rowid: int


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
    OpenOperation,
    FilterOperation,
    FreqOperation,
]

regexp_matches = CustomFunction("regexp_matches", ["string", "regex"])

@dataclass(frozen=True)
class Column:
    name: str
    type: ColType


def _get_schema_for_view(
    conn: DuckDBPyConnection,
    view: QueryBuilder,
    query_params: Optional[List[str]] = None,
) -> List[Column]:

    sql_query = f"DESCRIBE {view.get_sql()}"

    if query_params is None:
        query_params = []

    try:
        conn.execute(sql_query, query_params)
    except Exception as e:
        print(sql_query)
        raise e

    try:
        rows = conn.fetchall()
    except RuntimeError as e:
        if e.args[0] == "no open result set":
            return []
        else:
            raise e

    schema: List[Column] = [Column(row[0], row[1]) for row in rows]
    return schema


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
    except Exception as e:
        print(sql_query)
        raise e

    columns = [col[0] for col in conn.description]
    try:
        rows = conn.fetchall()

    except RuntimeError as e:
        if e.args[0] == "no open result set":
            return [], []
        else:
            raise e

    return rows, columns


def _execute_query_csv_stream(
    conn: DuckDBPyConnection,
    view: QueryBuilder,
    query_params: Optional[List[str]] = None,
) -> Iterator[str]:
    sql_query = view.get_sql()

    if query_params is None:
        query_params = []

    try:
        conn.execute(sql_query, query_params)
    except RuntimeError as e:
        print(sql_query)
        raise e

    buffer = io.StringIO()
    csv_writer = csv.writer(buffer)

    def _row_to_csv_str(seq):
        csv_writer.writerow(seq)
        value = buffer.getvalue().strip("\r\n")
        buffer.seek(0)
        buffer.truncate(0)
        return value + "\n"

    yield _row_to_csv_str((col_info[0] for col_info in conn.description))

    while True:
        try:
            row = conn.fetchone()
            if row is None:
                continue
            yield _row_to_csv_str(row)
        except RuntimeError as e:
            if e.args[0].startswith(
                "Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result"
            ):
                break
            raise e

@dataclass(kw_only=True, eq=False)
class Table:
    uid: str = field(init=False)
    view: QueryBuilder
    source: Optional["Table"] = field(repr=False)
    query_params: List[str] = field(default_factory=list)
    name: Optional[str] = None
    desc: Optional[str] = None
    dbtype: Optional[DBType] = None
    columns: List[Column] = field(init=False)
    wrapped_col_indices: List[int] = field(default_factory=list)

    def __post_init__(self):
        self.query_params = self.query_params or []

        source_uid = self.source.uid if self.source else ""
        query_params_str = ",".join(self.query_params)
        query_str = self.view.get_sql()
        hash_str = source_uid + query_params_str + query_str
        self.uid = hashlib.md5(hash_str.encode("utf-8")).hexdigest()[:15]

        self.orderbys = {
            field.name: (order == Order.asc)
            for field, order in self.view._orderbys
        }

        # Try to infer dbtype from source if not provided
        if self.dbtype is None:
            if self.source is None:
                raise ValueError(f"Unable to infer dbtype for {self}")
            elif self.source is not None:
                self.dbtype = self.source.dbtype

        self.get_db_connection = (
            get_conn if self.dbtype == "disk" else get_in_memory_conn
        )

        self.columns = _get_schema_for_view(
            self.get_db_connection(),
            self.view,
            query_params=self.all_query_params(),
        )
        # TODO: refactor: don't do IO in table constructor
        self.persist()

    def _persist(self, key):
        if self.dbtype == "memory":
            table_store.put_in_memory(key, self)

        data = asdict(self)

        # exclude fields that have init=False, e.g. columns
        # these are initialized in __post_init__
        for field in fields(self):
            if not field.init:
                del data[field.name]

        data.pop("source")
        data["source_uid"] = self.source.uid if self.source else None
        record = {"class": self.__class__.__name__, "data": data}
        table_store.put(key, pickle.dumps(record))

    def persist(self):
        self._persist(key=self.uid)
        if self.name is not None:
            self._persist(key=self.name)

    @classmethod
    def load(cls, uid: str) -> "Table":
        obj = table_store.get(uid)
        if isinstance(obj, Table):
            return obj
        record = pickle.loads(table_store.get(uid))
        class_ = globals()[record["class"]]
        data = record["data"]
        source_uid = data.pop("source_uid")
        source = None if source_uid is None else Table.load(source_uid)
        data["source"] = source
        return class_(**data)

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

    # Note: this depends on implementation of __hash__
    @lru_cache
    def __getitem_cached__(self, slice_rep):
        s = slice(*slice_rep[1])

        limit, offset = s.stop - s.start, s.start
        # pypika seems to have a different understanding of
        # the start and stop attributes of a slice
        view = self.view[offset:limit]

        if not isinstance(view, QueryBuilder):
            raise Exception(f"view has unexpected type {type(view)}")
        rows, columns = _execute_query(
            self.get_db_connection(),
            view,
            query_params=self.all_query_params(),
        )
        return rows, columns

    def __getitem__(self, s):
        # slice_rep is a hashable version of s
        # which makes it compatible with lru_cache
        slice_rep = s.__reduce__()

        return self.__getitem_cached__(slice_rep)

    def __hash__(self):
        return hash(self.uid)

    @property
    def lineage(self) -> List["Table"]:
        """
        List of parent table + this table
        """
        if self.source is None:
            return [self]
        return self.source.lineage + [self]

    @property
    def parent(self) -> Optional["Table"]:
        """
        Returns parent table if it exists otherwise returns self
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
        match (self.source, self.name, self.desc):
            case (None, None, None):
                return "unk"
            case (_, str(n), None):
                return n
            case (_, _, str(d)):
                return d
        return "unk"

    def frequency(self, cols: List[str]) -> "FreqTable":
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
        num_rows = QueryColumn("num_rows")
        percentage = (
            100 * Cast(num_rows, "REAL") / analytics.Sum(num_rows).over()
        )
        percentage = percentage.as_("percentage")
        res = (
            Query.from_(res)
            .select(
                "*",
                percentage,
            )
            .orderby("num_rows", order=Order.desc)
        )
        return FreqTable(view=res, key_cols=cols, source=self, desc="freq")

    def sort(self, col_name: str, ascending: bool = True) -> "Table":
        order = Order.asc if ascending else Order.desc
        res = Query.from_(self.view).orderby(col_name, order=order).select("*")
        return Table(
            view=res,
            source=self.source,
            desc=self.desc,
            query_params=self.query_params,
            dbtype=self.dbtype,
        )

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
    ) -> "Table":
        qry = self._filter_exact(self.view, filters, cols_to_return)

        return Table(view=qry, source=self, desc="fil")

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
    ) -> "Table":
        """
        if cols_to_return is None, then return all columns
        """
        res = self._filter_except(self.view, filters, cols_to_return)
        return Table(view=res, source=self, desc="fil2")

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
    ) -> "Table":
        qry = self._filter_regex(self.view, column, regex, cols_to_return)
        return Table(
            view=qry, source=self, query_params=[regex], desc="search"
        )

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
        return Table(view=res, source=self, desc="piv")

    @property
    def typ(self):
        if type(self) == FreqTable:
            return "freq"
        else:
            return "base"

    def run_op(
        self,
        operation: OperationsType,
    ) -> "Table":

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
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported operation",
                )
            return res

        raise HTTPException(
            status_code=400,
            detail=f"Unsupported operation",
        )

    def iter_csv(self):
        return _execute_query_csv_stream(
            self.get_db_connection(), self.view, self.all_query_params()
        )


@dataclass(kw_only=True, eq=False)
class FreqTable(Table):
    key_cols: List[str]

    def __post_init__(self):
        super().__post_init__()
        col_names = [c.name for c in self.columns]
        self.wrapped_col_indices = (
            [col_names.index("percentage")]
            if "percentage" in col_names
            else []
        )

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
    ) -> "Table":
        """
        A facet search is just the behaviour that happens
        when you press 'Enter' on a row (facet) of a frequency table
        """
        if self.source is None:
            raise ValueError("source cannot be None for freq table")
        return self.source.filter_exact(filters, cols_to_return=None)

    def filter_exact(
        self,
        filters: List[Tuple[str, Optional[str]]],
        cols_to_return: Optional[List[str]],
    ) -> "Table":
        """
        If a filter op is performed on a FreqTable, it returns another
        FreqTable with the same `source` and `key_cols`
        """

        if cols_to_return is not None:
            self.check_for_key_cols(cols_to_return)
        res = self._filter_exact(self.view, filters, cols_to_return)
        return FreqTable(
            view=res, key_cols=self.key_cols, source=self.source, desc="ffil"
        )

    def filter_except(
        self,
        filters: List[Tuple[str, Optional[str]]],
        cols_to_return: Optional[List[str]],
    ) -> "Table":
        """
        same as docs of `FreqTable.filter_exact`
        """

        if cols_to_return is not None:
            self.check_for_key_cols(cols_to_return)
        res = self._filter_except(self.view, filters, cols_to_return)
        return FreqTable(
            view=res, key_cols=self.key_cols, source=self.source, desc="ffil"
        )

    def filter_regex(
        self, column: str, regex: str, *args, **kwargs
    ) -> "FreqTable":
        res = self._filter_regex(self.view, column, regex, *args, **kwargs)
        return FreqTable(
            view=res,
            key_cols=self.key_cols,
            source=self.source,
            query_params=[regex],
            desc="fsearch",
        )

    def sort(self, col_name: str, ascending: bool = True) -> "FreqTable":
        order = Order.asc if ascending else Order.desc
        res = Query.from_(self.view).orderby(col_name, order=order).select("*")
        return FreqTable(
            view=res,
            key_cols=self.key_cols,
            source=self.source,
            query_params=self.query_params,
        )

    @property
    def key_col_indices(self) -> List[int]:
        col_names = [c.name for c in self.columns]
        return [col_names.index(key_col) for key_col in self.key_cols]

    def run_op(
        self,
        operation: OperationsType,
    ) -> "Table":
        if isinstance(operation, FacetOperation):
            return self.facet_search(operation.facets)

        return super().run_op(operation)


@dataclass(kw_only=True, eq=False)
class MemoryTable(Table):
    cols: List[str] = field(hash=False)
    rows: List = field(hash=False)

    @classmethod
    def from_records(cls, name: str, cols: List[str], rows: List, **kwargs):
        conn = get_in_memory_conn()
        col_str = ", ".join([f"{col} VARCHAR" for col in cols])
        # insert rows into db
        conn.execute(f"CREATE TABLE {name} ({col_str});")
        num_cols = len(cols)
        conn.executemany(
            f"INSERT INTO {name} VALUES ({','.join(['?'] * num_cols)});", rows
        )

        return cls(
            name=name,
            cols=cols,
            rows=rows,
            view=Query.from_(name).select("*"),
            source=None,
            desc=name,
            dbtype="memory",
            **kwargs,
        )


@dataclass(kw_only=True, eq=False)
class TableOfTables(MemoryTable):
    table_names: List[str]

    def run_op(self, operation: OperationsType) -> "Table":

        if isinstance(operation, OpenOperation):
            table_name = self.table_names[operation.rowid]
            return Table(
                view=Query.from_(table_name).select("*"),
                source=self,
                name=table_name,
                dbtype="disk",
            )

        return super().run_op(operation)


class MarkdownTable(MemoryTable):
    @classmethod
    def from_markdown_str(cls, name: str, text: str) -> Self:
        return cls.from_records(
            name=name, cols=["md"], rows=[(text,)]
        )


demo_datasets = load_demo_datasets()
main_table = TableOfTables.from_records(
    name="main",
    cols=["name", "details", "date"],
    rows=[
        (dataset["display_name"], dataset["details"], dataset["date"])
        for dataset in demo_datasets
    ],
    table_names=[dataset["table_name"] for dataset in demo_datasets],
    wrapped_col_indices=[1],
)

about_table = MarkdownTable.from_markdown_str(
    name="about",
    text="""Tablehub is a tool for sharing and exploring tables

The UX is heavily inspired by the amazing terminal-tool [Visidata](https://www.visidata.org)

#### Keybindings

The keybindings are likely to change to mantain parity with Visidata

##### Navigation
- `j` `k` `l` `h` for scrolling
- `ArrowDown` `ArrowUp` `ArrowRight` `ArrowLeft` for scrolling
- `N`: Open next set of paginated rows
- `P`: Open previous set of paginated rows
- `v`: adjust column width
- `q`: go to last table (browser back button)
- `p`: go to next table (browser forward button)
- `G`: Jump to last row
- `g`: Jump to top row

##### Frequency

- `!` Toggle key column
- `F` Frequency of current column
- `f` Frequency of key columns

##### Filtering
- `,` Filter by value in current cell
- `-` Hide/Unhide columns
- `"` Materialize filters

##### Search
- `|` Search by regex on active column

##### Sorting
- `[` Sort ascending
- `]` Sort descending

##### Aggregation
- `+` Toggle aggregate column
- `W` Pivot on current column (needs one key and one aggregate column)


#### Note for Vimium users

You will need to disable **Vimium** for [Tablehub.io](/tables/about) if you want to make use of the keybindings implemented here.

Navigation experience for you will be slower since vimium disables browsers' backward-forward cache. The only way to get around this is to delete/disable the Vimium plugin entirely.
    """,
)
