from typing import Dict, Union
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi import Request
from fastapi.responses import RedirectResponse
from fastapi.responses import StreamingResponse
import duckdb
from pypika import (
    Query,
)
from pypika.queries import QueryBuilder
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
from pydantic import NonNegativeInt
from fastapi.exceptions import HTTPException

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
    starting_sheet = Sheet(
        _initialize_view("test_2"),
        None,
        desc="gta",
        get_db_connection=get_conn,
    )
    sheets[starting_sheet.uid] = starting_sheet


@app.get("/")
def index():
    # redirect to initial view
    return RedirectResponse(url=f"/{starting_sheet.uid}")


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


_MAX_NUM_ROWS = 25


@app.get("/{uid}")
def get_sheet_by_uid(request: Request, uid: str, page: NonNegativeInt = 0):

    if uid not in sheets:
        raise HTTPException(status_code=404, detail="Sheet not found")

    sheet = sheets[uid]
    rows, columns = sheet[page * _MAX_NUM_ROWS : (page + 1) * _MAX_NUM_ROWS]
    num_rows = len(sheet)

    page_info = {
        "has_prev_page": page > 0,
        "has_next_page": (page + 1) * _MAX_NUM_ROWS < num_rows,
        "page": page,
    }

    return templates.TemplateResponse(
        "table.html",
        dict(
            request=request,
            msg="vow",
            rows=rows,
            columns=columns,
            sheet=sheet,
            num_rows=num_rows,
            **page_info,
        ),
    )


@app.get("/downloads/{uid}")
def download_sheet(uid: str, file_type: str = "csv"):
    if file_type != "csv":
        raise HTTPException(status_code=404, detail="File type not supported")

    if uid not in sheets:
        raise HTTPException(status_code=404, detail="Sheet not found")

    sheet = sheets[uid]
    filename_without_ext = "_".join([str(s) for s in sheet.lineage])
    filename = (filename_without_ext or "download") or ".csv"

    return StreamingResponse(
        sheet.iter_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


app.mount("/js/", StaticFiles(directory="js"), name="javascript")
app.mount("/static/", StaticFiles(directory="static"), name="site")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
