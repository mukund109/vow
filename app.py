from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import duckdb
from typing import Dict, Union
from pypika import (
    Query,
)
from pypika.queries import QueryBuilder
from fastapi import Request
from fastapi.responses import RedirectResponse
from utils import fetch_sample_database
from sheet import (
    Sheet,
    Operation,
    FreqOperation,
    FilterOperation,
    FacetOperation,
    PivotOperation,
    RegexSearchOperation,
)

# create a flask application
app = FastAPI()
# set the template directory
templates = Jinja2Templates(directory="templates")

fetch_sample_database()


def get_conn():
    conn = duckdb.connect("vow.db", read_only=True)
    conn.execute("PRAGMA default_null_order='NULLS LAST'")
    return conn


# core data structure (mapping from sheet uid to Sheet)
sheets: Dict[str, Sheet] = dict()


def _initialize_view(table: str) -> QueryBuilder:
    return Query.from_(table).select("*")


if "gta" not in sheets:
    sheets["gta"] = Sheet(
        _initialize_view("test_2"),
        None,
        desc="gta",
        get_db_connection=get_conn,
    )


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
        RegexSearchOperation,
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
