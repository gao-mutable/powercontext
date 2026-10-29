# Copyright (c) 2026 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Row-level CSV Sources with exact record identity and text projection.

每个数据行被采集为一个独立的 ``CSVRowSource``（CAPTURED），
其 identity 由 document_id 与主键值共同决定，重排后保持稳定。
"""

from __future__ import annotations

import csv
import io
import json
from urllib.parse import quote

from pydantic import BaseModel

from powercontext.limits import MAX_SOURCE_ID_LENGTH
from powercontext.sources import AdapterSourceDefinition, TextEvidence
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY

CSV_ROW_SOURCE_NAME = "csv-row"


class CSVDocumentInput(BaseModel):
    """完整的离线 CSV 字节内容与记录识别信息。"""

    document_id: str
    primary_key: str
    content: bytes


class CSVRowInput(BaseModel):
    """一行数据按原表头顺序保留的字段值。"""

    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]


class CSVRowSource(Source):
    """已采集的一行 CSV 记录。"""

    source_id: str
    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]


def csv_row_source_id(document_id: str, primary_key_value: str, /) -> str:
    """构造 ``csv:{document_id}:{primary_key_value}`` 形式的稳定 identity。"""
    if not document_id or document_id != document_id.strip():
        raise ValueError("document_id must be non-empty without surrounding whitespace")
    if not primary_key_value or not primary_key_value.strip():
        raise ValueError("primary_key_value must not be blank")
    identity = f"csv:{quote(document_id, safe='')}:{quote(primary_key_value, safe='')}"
    if len(identity) > MAX_SOURCE_ID_LENGTH:
        raise ValueError("source_id exceeds the 256 character limit")
    return identity

    # TODO(你来写)：

def parse_csv_document(document: CSVDocumentInput, /) -> tuple[CSVRowInput, ...]:
    """解析整份 CSV；任何格式/校验错误都以 ValueError 原子失败。"""
    if not document.document_id or document.document_id != document.document_id.strip():
        raise ValueError("document_id must be non-empty without surrounding whitespace")
    if not document.primary_key or not document.primary_key.strip():
        raise ValueError("primary_key must not be blank")
    try:
        text = document.content.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise ValueError("CSV content must be valid UTF-8") from error
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError("CSV document must contain a header record") from None
    if not header:
        raise ValueError("CSV document must contain a header record")
    seen_names = set()
    for name in header:
        if not name.strip():
            raise ValueError("header fields must not be blank")
        if name in seen_names:
            raise ValueError("header fields must be unique")
        seen_names.add(name)
    if document.primary_key not in header:
        raise ValueError("primary_key must name an existing column")
    primary_key_index = header.index(document.primary_key)
    columns = tuple(header)
    rows: list[CSVRowInput] = []
    seen_keys: set[str] = set()
    try:
        for record in reader:
            if not record:
                continue  # 完全空的物理行，跳过
            if len(record) != len(header):
                raise ValueError("record width must match the header width")
            primary_key_value = record[primary_key_index]
            if not primary_key_value.strip():
                raise ValueError("primary key values must not be blank")
            if primary_key_value in seen_keys:
                raise ValueError("primary key values must be unique")
            seen_keys.add(primary_key_value)
            rows.append(
                CSVRowInput(
                    document_id=document.document_id,
                    primary_key=document.primary_key,
                    primary_key_value=primary_key_value,
                    columns=columns,
                    values=tuple(record),
                )
            )
    except csv.Error as error:
        raise ValueError(str(error)) from error

    return tuple(rows)

class CSVRowTextEvidenceProjection:
    """把一行记录投影成标准 text-evidence 文本证据。"""

    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = CSVRowSource
    output_class = TextEvidence

    def project(self, source: CSVRowSource, /) -> TextEvidence:
        mapping = dict(zip(source.columns, source.values))
        return TextEvidence(
            source_type="csv-row",
            source_id=source.source_id,
            content=json.dumps(mapping, ensure_ascii=False, separators=(",", ":")),
            metadata={
                "document_id": source.document_id,
                "primary_key": source.primary_key,
                "primary_key_value": source.primary_key_value,
                "columns": list(source.columns),
                "row": mapping,
            },
        )

class CSVRowSourceAdapter:
    """CSVRowInput -> CSVRowSource 的解析与回读。"""

    input_class = CSVRowInput
    name = CSV_ROW_SOURCE_NAME
    source_class = CSVRowSource

    async def resolve(self, value: CSVRowInput, /) -> CSVRowSource:
        source_id = csv_row_source_id(value.document_id, value.primary_key_value)
        return CSVRowSource(
            name=source_id,
            source_id=source_id,
            materialization=SourceMaterialization.CAPTURED,
            document_id=value.document_id,
            primary_key=value.primary_key,
            primary_key_value=value.primary_key_value,
            columns=value.columns,
            values=value.values,
        )

    async def read(self, source: CSVRowSource, /) -> CSVRowInput:
        return CSVRowInput(
            document_id=source.document_id,
            primary_key=source.primary_key,
            primary_key_value=source.primary_key_value,
            columns=source.columns,
            values=source.values,
        )
CSV_ROW_SOURCE_DEFINITION = AdapterSourceDefinition(
    CSVRowSourceAdapter(),
    projections=(CSVRowTextEvidenceProjection(),),
)
