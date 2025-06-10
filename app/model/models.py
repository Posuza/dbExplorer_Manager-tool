from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import uuid

class ConnectionParams(BaseModel):
    server: str
    database: str
    user: str
    password: str
    port: int
    db_type: str = "mysql"  # Make sure this field exists and has a proper default

class SessionInfo(BaseModel):
    session_id: str = str(uuid.uuid4())
    connection_params: ConnectionParams
    
class TableInfo(BaseModel):
    database: str
    table: str
    
class UpdateRecordRequest(BaseModel):
    column: str  
    value: Any  

class UpdateRecordResponse(BaseModel):
    success: bool
    message: str

class TableRecordCount(BaseModel):
    table: str
    count: int

class ColumnInfo(BaseModel):
    name: str
    type: str 
    # Add other column properties as needed

class TableColumns(BaseModel):
    columns: List[ColumnInfo]

class PaginatedRecords(BaseModel):
    table: str
    page: int
    page_size: int
    total_count: int
    total_pages: int
    records: List[Dict[str, Any]]

class SelectedColumnsRecords(BaseModel):
    table: str
    columns: List[str]
    page: int
    page_size: int
    total_count: int
    total_pages: int
    records: List[Dict[str, Any]]

class ConnectionResponse(BaseModel):
    session_id: str
    tables: List[TableInfo]
    db_type: Optional[str] = None  # Add this line
    database: Optional[str] = None  # Add this line if not already present

# Add new CRUD models
class CreateRecordRequest(BaseModel):
    data: Dict[str, Any]  # Column name -> value mapping

class CreateRecordResponse(BaseModel):
    success: bool
    message: str
    record_id: Optional[str] = None

class DeleteRecordResponse(BaseModel):
    success: bool
    message: str
    deleted_count: int

class SingleRecord(BaseModel):
    record: Dict[str, Any]
    table: str
    record_id: str

class PreviewRecords(BaseModel):
    records: List[Dict[str, Any]]
    table: str
    count: int = 10

class SessionsList(BaseModel):
    active_sessions: List[str]