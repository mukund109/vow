import time
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from fastapi.responses import StreamingResponse
from fastapi.responses import HTMLResponse
from utils import fetch_sample_database
from table import Table, OperationsType
from pydantic import NonNegativeInt
from fastapi.exceptions import HTTPException
from view import html_page

# create a flask application
app = FastAPI()
# set the template directory

fetch_sample_database()


@app.get("/")
def index():
    # redirect to initial view
    return RedirectResponse(url=f"/tables/main")


# passing uid in the body might be semantically more sensible
@app.post("/tables/{uid}")
def post_view(
    uid: str,
    operation: OperationsType,
):
    prev_table = Table.load(uid)

    new_table = prev_table.run_op(operation)

    return {"new_table": new_table.uid, "yolo": "Success"}


@app.get("/tables/{uid}")
def table_by_uid(uid: str, page: NonNegativeInt = 0):

    try:
        table = Table.load(uid)
    except KeyError:
        raise HTTPException(status_code=404, detail="Table not found")

    return HTMLResponse(content=html_page(table, page=page), status_code=200)


@app.get("/about", response_class=HTMLResponse)
def about():
    return RedirectResponse(url=f"/tables/about")


@app.get("/downloads/{uid}")
def download_table(uid: str, file_type: str = "csv"):
    if file_type != "csv":
        raise HTTPException(status_code=404, detail="File type not supported")

    try:
        table = Table.load(uid)
    except KeyError:
        raise HTTPException(status_code=404, detail="Table not found")

    filename_without_ext = "_".join([str(s) for s in table.lineage])
    filename = (filename_without_ext or "download") or ".csv"

    return StreamingResponse(
        table.iter_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


app.mount("/js/", StaticFiles(directory="js"), name="javascript")
app.mount("/static/", StaticFiles(directory="static"), name="site")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
