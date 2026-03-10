"""Pure transformation functions: (df, step) -> df."""
from __future__ import annotations
import pandas as pd
from models import (
    DeleteRowsStep,
    DeleteColumnsStep,
    UnpivotStep,
    LeftJoinStep,
    FilterRowsStep,
    ReorderColumnsStep,
    PipelineStep,
)
import session_store


def apply_step(df: pd.DataFrame, step: PipelineStep) -> pd.DataFrame:
    if isinstance(step, DeleteRowsStep):
        return _delete_rows(df, step)
    elif isinstance(step, DeleteColumnsStep):
        return _delete_columns(df, step)
    elif isinstance(step, UnpivotStep):
        return _unpivot(df, step)
    elif isinstance(step, LeftJoinStep):
        return _left_join(df, step)
    elif isinstance(step, FilterRowsStep):
        return _filter_rows(df, step)
    elif isinstance(step, ReorderColumnsStep):
        return _reorder_columns(df, step)
    raise ValueError(f"Unknown step type: {step.type}")


def run_pipeline(df: pd.DataFrame, steps: list[PipelineStep]) -> pd.DataFrame:
    for step in steps:
        df = apply_step(df, step)
    return df


# ── individual operations ─────────────────────────────────────────────────────

def _delete_rows(df: pd.DataFrame, step: DeleteRowsStep) -> pd.DataFrame:
    if step.mode == "index_range":
        start = step.start if step.start is not None else 0
        end = step.end if step.end is not None else len(df)
        indices = list(range(start, min(end, len(df))))
        return df.drop(index=df.index[indices]).reset_index(drop=True)
    else:
        # condition mode
        col = step.column
        op = step.operator
        val = step.value
        if not col or op is None:
            return df
        mask = _build_mask(df, col, op, val)
        return df[~mask].reset_index(drop=True)


def _delete_columns(df: pd.DataFrame, step: DeleteColumnsStep) -> pd.DataFrame:
    cols = [c for c in step.columns if c in df.columns]
    return df.drop(columns=cols)


def _unpivot(df: pd.DataFrame, step: UnpivotStep) -> pd.DataFrame:
    id_vars = [c for c in step.id_vars if c in df.columns]
    value_vars = [c for c in step.value_vars if c in df.columns] or None
    return df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name=step.var_name or "variable",
        value_name=step.value_name or "value",
    )


def _left_join(df: pd.DataFrame, step: LeftJoinStep) -> pd.DataFrame:
    if not step.join_file_id:
        return df
    right_df = session_store.get_df(step.join_file_id)
    if right_df is None:
        raise ValueError(f"Join file not found: {step.join_file_id}")
    if not step.left_on or not step.right_on:
        raise ValueError("left_on and right_on must be specified for left_join")
    if step.select_cols:
        keep = [step.right_on] + [c for c in step.select_cols if c in right_df.columns and c != step.right_on]
        right_df = right_df[keep]
    result = df.merge(right_df, left_on=step.left_on, right_on=step.right_on, how="left")
    if step.right_on != step.left_on and step.right_on in result.columns:
        result = result.drop(columns=[step.right_on])
    return result


def _filter_rows(df: pd.DataFrame, step: FilterRowsStep) -> pd.DataFrame:
    col = step.column
    op = step.operator
    val = step.value
    if not col or not op:
        return df
    mask = _build_mask(df, col, op, val)
    return df[mask].reset_index(drop=True)


def _reorder_columns(df: pd.DataFrame, step: ReorderColumnsStep) -> pd.DataFrame:
    order = [c for c in step.order if c in df.columns]
    remaining = [c for c in df.columns if c not in order]
    return df[order + remaining]


# ── helpers ───────────────────────────────────────────────────────────────────

def _build_mask(df: pd.DataFrame, col: str, op: str, val: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)

    series = df[col]

    if op == "is_null":
        return series.isna()
    if op == "is_not_null":
        return series.notna()
    if op == "contains":
        return series.astype(str).str.contains(val, na=False)
    if op == "not_contains":
        return ~series.astype(str).str.contains(val, na=False)
    if op == "starts_with":
        return series.astype(str).str.startswith(val, na=False)
    if op == "ends_with":
        return series.astype(str).str.endswith(val, na=False)

    # numeric / comparable operators
    try:
        numeric_val = float(val) if val else None
    except (ValueError, TypeError):
        numeric_val = None

    def _coerce(s: pd.Series, v):
        try:
            return s.astype(float), float(v)
        except Exception:
            return s.astype(str), str(v)

    if op == "==":
        if numeric_val is not None:
            s, v = _coerce(series, numeric_val)
            return s == v
        return series.astype(str) == str(val)
    if op == "!=":
        if numeric_val is not None:
            s, v = _coerce(series, numeric_val)
            return s != v
        return series.astype(str) != str(val)
    if op == ">":
        s, v = _coerce(series, numeric_val if numeric_val is not None else val)
        return s > v
    if op == "<":
        s, v = _coerce(series, numeric_val if numeric_val is not None else val)
        return s < v
    if op == ">=":
        s, v = _coerce(series, numeric_val if numeric_val is not None else val)
        return s >= v
    if op == "<=":
        s, v = _coerce(series, numeric_val if numeric_val is not None else val)
        return s <= v

    return pd.Series([False] * len(df), index=df.index)
