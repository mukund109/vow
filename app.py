from typing import Dict
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from fastapi.responses import StreamingResponse
from fastapi.responses import HTMLResponse
from utils import fetch_sample_database
from sheet import Sheet, AboutSheet, MasterSheet, OperationsType
from pydantic import NonNegativeInt
from fastapi.exceptions import HTTPException
from view import html_page

# create a flask application
app = FastAPI()
# set the template directory
templates = Jinja2Templates(directory="templates")

fetch_sample_database()


# core data structure (mapping from sheet uid to Sheet)
sheets: Dict[str, Sheet] = dict()

master_sheet = MasterSheet()
sheets["master"] = master_sheet
sheets[master_sheet.uid] = master_sheet

about_sheet = AboutSheet()
sheets["about"] = about_sheet
sheets[about_sheet.uid] = about_sheet


@app.get("/")
def index():
    # redirect to initial view
    return RedirectResponse(url=f"/sheets/{master_sheet.uid}")


# passing uid in the body might be semantically more sensible
@app.post("/sheets/{uid}")
def post_view(
    uid: str,
    operation: OperationsType,
):
    prev_sheet = sheets[uid]

    new_sheet = prev_sheet.run_op(operation)

    sheets[new_sheet.uid] = new_sheet

    return {"new_sheet": new_sheet.uid, "yolo": "Success"}


@app.get("/sheets/{uid}")
def sheet_by_uid(uid: str, page: NonNegativeInt = 0):

    if uid not in sheets:
        raise HTTPException(status_code=404, detail="Sheet not found")

    sheet = sheets[uid]
    return HTMLResponse(content=html_page(sheet, page=page), status_code=200)


@app.get("/about", response_class=HTMLResponse)
def about():
    return RedirectResponse(url=f"/sheets/about")


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
