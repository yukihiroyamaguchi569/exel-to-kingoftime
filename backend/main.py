"""FastAPI application: API routes + static file serving."""
from __future__ import annotations
import io
import sys
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

import pandas as pd

import session_store
from models import PipelineRequest
from pipeline import run_pipeline

# ── app setup ─────────────────────────────────────────────────────────────────

app = FastAPI(title="Excel→CSV Pipeline")

UPLOAD_DIR = Path(__file__).parent.parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

PREVIEW_ROWS = 50


# ── utility ───────────────────────────────────────────────────────────────────

def _df_to_preview(df: pd.DataFrame) -> dict:
    preview = df.head(PREVIEW_ROWS).fillna("").astype(str)
    return {
        "columns": list(df.columns),
        "rows": preview.values.tolist(),
        "total_rows": len(df),
    }


# ── routes ────────────────────────────────────────────────────────────────────

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload an Excel file. Returns file_id, sheet list, and preview of first sheet."""
    contents = await file.read()
    suffix = Path(file.filename or "upload.xlsx").suffix.lower()
    dest = UPLOAD_DIR / f"{__import__('uuid').uuid4()}{suffix}"
    dest.write_bytes(contents)

    if suffix in (".xlsx", ".xls", ".xlsm", ".xlsb", ".ods"):
        xl = pd.ExcelFile(dest, engine="openpyxl")
        sheets = xl.sheet_names
        first_sheet = sheets[0]
        df = xl.parse(first_sheet)
    else:
        raise HTTPException(status_code=400, detail="Unsupported file type. Please upload an Excel file.")

    file_id = session_store.new_session(df, sheets, dest, first_sheet)
    return {
        "file_id": file_id,
        "sheets": sheets,
        "selected_sheet": first_sheet,
        **_df_to_preview(df),
    }


@app.post("/upload-join-file")
async def upload_join_file(file: UploadFile = File(...), sheet_name: str = Form(default="")):
    """Upload a secondary file for left_join."""
    contents = await file.read()
    suffix = Path(file.filename or "join.xlsx").suffix.lower()
    dest = UPLOAD_DIR / f"{__import__('uuid').uuid4()}{suffix}"
    dest.write_bytes(contents)

    xl = pd.ExcelFile(dest, engine="openpyxl")
    sheets = xl.sheet_names
    selected = sheet_name if sheet_name in sheets else sheets[0]
    df = xl.parse(selected)

    file_id = session_store.new_session(df, sheets, dest, selected)
    return {
        "file_id": file_id,
        "sheets": sheets,
        "selected_sheet": selected,
        "columns": list(df.columns),
    }


@app.post("/select-sheet")
async def select_sheet(file_id: str = Form(...), sheet_name: str = Form(...)):
    """Switch the active sheet for an already-uploaded file."""
    df = session_store.switch_sheet(file_id, sheet_name)
    if df is None:
        raise HTTPException(status_code=404, detail="Session not found")
    return {
        "file_id": file_id,
        "selected_sheet": sheet_name,
        **_df_to_preview(df),
    }


@app.post("/preview")
async def preview(req: PipelineRequest):
    """Run pipeline up to preview_up_to index (inclusive) and return preview."""
    df = session_store.get_df(req.file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Session not found")

    steps = req.steps
    if req.preview_up_to is not None:
        steps = steps[: req.preview_up_to + 1]

    try:
        result = run_pipeline(df.copy(), steps)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return _df_to_preview(result)


@app.post("/run")
async def run(req: PipelineRequest):
    """Execute full pipeline and return CSV as a streaming download."""
    df = session_store.get_df(req.file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Session not found")

    try:
        result = run_pipeline(df.copy(), req.steps)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    buf = io.StringIO()
    result.to_csv(buf, index=False, header=False)
    buf.seek(0)

    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=output.csv"},
    )


@app.get("/session/{file_id}/columns")
async def get_columns(file_id: str):
    df = session_store.get_df(file_id)
    if df is None:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"columns": list(df.columns)}


@app.delete("/session/{file_id}")
async def delete_session(file_id: str):
    ok = session_store.delete_session(file_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"deleted": file_id}


# ── static files (frontend) ───────────────────────────────────────────────────
# Mount last so API routes take precedence

if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
