"""FastAPI application: API routes + static file serving."""
from __future__ import annotations
import io
import re
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

UPLOAD_DIR = Path("/tmp/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

PREVIEW_ROWS = 50


# ── utility ───────────────────────────────────────────────────────────────────

def validate_shift_data(df: pd.DataFrame) -> list[dict]:
    """Validate a shift table DataFrame and return a list of check results."""
    checks = []

    # 1. 空チェック
    if len(df) == 0:
        checks.append({"type": "error", "label": "データ行", "message": "シートにデータ行がありません"})
        return checks  # 以降のチェックは意味がないので早期リターン
    if len(df.columns) == 0:
        checks.append({"type": "error", "label": "列", "message": "シートに列がありません"})
        return checks

    # 2. 列数チェック（28〜32列が期待値）
    n_cols = len(df.columns)
    if 28 <= n_cols <= 32:
        checks.append({"type": "ok", "label": "列数", "message": f"{n_cols}列（問題なし）"})
    else:
        checks.append({"type": "warning", "label": "列数", "message": f"{n_cols}列（シフト表は28〜32列が期待値です）"})

    # 3. 列ヘッダー日付チェック（2列目以降）
    header_cols = list(df.columns)[1:]
    date_count = 0
    for col in header_cols:
        col_str = str(col).strip()
        # 1〜31の整数チェック
        try:
            val = int(float(col_str))
            if 1 <= val <= 31:
                date_count += 1
                continue
        except (ValueError, TypeError):
            pass
        # YYYY/MM/DD形式チェック
        if re.match(r'^\d{4}/\d{1,2}/\d{1,2}$', col_str):
            date_count += 1

    total_header = len(header_cols)
    if total_header > 0:
        ratio = date_count / total_header
        if ratio >= 0.5:
            checks.append({"type": "ok", "label": "列ヘッダー", "message": f"日付らしい値を確認（{date_count}/{total_header}列）"})
        else:
            checks.append({"type": "warning", "label": "列ヘッダー", "message": f"日付らしい列が少ない（{date_count}/{total_header}列）"})
    else:
        checks.append({"type": "warning", "label": "列ヘッダー", "message": "2列目以降の列がありません"})

    # 4. 最初の列（職員番号列）チェック
    first_col = df.iloc[:, 0]
    total_cells = len(first_col)

    # 空セル数
    empty_count = first_col.isna().sum() + (first_col.astype(str).str.strip() == '').sum()
    if empty_count > 0:
        checks.append({"type": "warning", "label": "職員番号列", "message": f"空セルが{int(empty_count)}件あります"})
    else:
        checks.append({"type": "ok", "label": "職員番号列", "message": "空セルなし"})

    # 2〜4桁の数字（職員番号）の割合
    num_match = first_col.astype(str).str.strip().str.match(r'^\d{2,4}$').sum()
    num_ratio = num_match / total_cells if total_cells > 0 else 0
    if num_ratio >= 0.9:
        checks.append({"type": "ok", "label": "職員番号列の値", "message": f"2〜4桁の値を確認（{int(num_match)}/{total_cells}件）"})
    else:
        checks.append({"type": "warning", "label": "職員番号列の値", "message": f"2〜4桁の値が少ない（{int(num_match)}/{total_cells}件）"})

    return checks


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
    try:
        dest.write_bytes(contents)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"ファイル保存エラー: {e}")

    if suffix not in (".xlsx", ".xls", ".xlsm", ".xlsb", ".ods"):
        raise HTTPException(status_code=400, detail="Unsupported file type. Please upload an Excel file.")

    try:
        engine = "xlrd" if suffix == ".xls" else "openpyxl"
        xl = pd.ExcelFile(dest, engine=engine)
        sheets = xl.sheet_names
        first_sheet = sheets[0]
        df = xl.parse(first_sheet)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Excelファイルの読み込みに失敗しました: {e}")

    file_id = session_store.new_session(df, sheets, dest, first_sheet)
    return {
        "file_id": file_id,
        "sheets": sheets,
        "selected_sheet": first_sheet,
        "checks": validate_shift_data(df),
        **_df_to_preview(df),
    }


@app.post("/upload-join-file")
async def upload_join_file(file: UploadFile = File(...), sheet_name: str = Form(default="")):
    """Upload a secondary file for left_join."""
    contents = await file.read()
    suffix = Path(file.filename or "join.xlsx").suffix.lower()
    dest = UPLOAD_DIR / f"{__import__('uuid').uuid4()}{suffix}"
    try:
        dest.write_bytes(contents)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"ファイル保存エラー: {e}")

    try:
        engine = "xlrd" if suffix == ".xls" else "openpyxl"
        xl = pd.ExcelFile(dest, engine=engine)
        sheets = xl.sheet_names
        selected = sheet_name if sheet_name in sheets else sheets[0]
        df = xl.parse(selected)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Excelファイルの読み込みに失敗しました: {e}")

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
        "checks": validate_shift_data(df),
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
