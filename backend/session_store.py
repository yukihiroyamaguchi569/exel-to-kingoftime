"""In-memory session store: maps file_id -> (DataFrame, sheet_names, file_path)."""
from __future__ import annotations
import uuid
from pathlib import Path
from typing import Optional
import pandas as pd

# { file_id: { "df": DataFrame, "sheets": list[str], "path": Path, "sheet_name": str } }
_store: dict[str, dict] = {}


def new_session(df: pd.DataFrame, sheets: list[str], path: Path, sheet_name: str) -> str:
    file_id = str(uuid.uuid4())
    _store[file_id] = {
        "df": df,
        "sheets": sheets,
        "path": path,
        "sheet_name": sheet_name,
    }
    return file_id


def get_df(file_id: str) -> Optional[pd.DataFrame]:
    entry = _store.get(file_id)
    return entry["df"] if entry else None


def get_sheets(file_id: str) -> Optional[list[str]]:
    entry = _store.get(file_id)
    return entry["sheets"] if entry else None


def get_path(file_id: str) -> Optional[Path]:
    entry = _store.get(file_id)
    return entry["path"] if entry else None


def switch_sheet(file_id: str, sheet_name: str) -> Optional[pd.DataFrame]:
    entry = _store.get(file_id)
    if not entry:
        return None
    path = entry["path"]
    df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
    entry["df"] = df
    entry["sheet_name"] = sheet_name
    return df


def delete_session(file_id: str) -> bool:
    if file_id in _store:
        del _store[file_id]
        return True
    return False


def list_sessions() -> list[str]:
    return list(_store.keys())
