from __future__ import annotations
from typing import Annotated, Literal, Optional, Union
from pydantic import BaseModel, Field


class DeleteRowsStep(BaseModel):
    type: Literal["delete_rows"] = "delete_rows"
    mode: Literal["index_range", "condition"] = "index_range"
    # index_range mode
    start: Optional[int] = None
    end: Optional[int] = None
    # condition mode
    column: Optional[str] = None
    operator: Optional[str] = None
    value: Optional[str] = None


class DeleteColumnsStep(BaseModel):
    type: Literal["delete_columns"] = "delete_columns"
    columns: list[str] = Field(default_factory=list)


class UnpivotStep(BaseModel):
    type: Literal["unpivot"] = "unpivot"
    id_vars: list[str] = Field(default_factory=list)
    value_vars: list[str] = Field(default_factory=list)
    var_name: str = "variable"
    value_name: str = "value"


class LeftJoinStep(BaseModel):
    type: Literal["left_join"] = "left_join"
    join_file_id: str = ""
    left_on: str = ""
    right_on: str = ""
    select_cols: list[str] = Field(default_factory=list)


class PrependYearMonthStep(BaseModel):
    type: Literal["prepend_yearmonth"] = "prepend_yearmonth"
    column: str = ""
    year: int = 2024
    month: int = 1


class ZeroPadStep(BaseModel):
    type: Literal["zero_pad"] = "zero_pad"
    column: str = ""
    width: int = 2


class FilterRowsStep(BaseModel):
    type: Literal["filter_rows"] = "filter_rows"
    column: str = ""
    operator: str = "=="
    value: str = ""


class ReorderColumnsStep(BaseModel):
    type: Literal["reorder_columns"] = "reorder_columns"
    order: list[str] = Field(default_factory=list)


PipelineStep = Annotated[
    Union[
        DeleteRowsStep,
        DeleteColumnsStep,
        UnpivotStep,
        LeftJoinStep,
        PrependYearMonthStep,
        ZeroPadStep,
        FilterRowsStep,
        ReorderColumnsStep,
    ],
    Field(discriminator="type"),
]


class PipelineRequest(BaseModel):
    file_id: str
    sheet_name: Optional[str] = None
    steps: list[PipelineStep] = Field(default_factory=list)
    preview_up_to: Optional[int] = None  # run steps[0..preview_up_to] inclusive
