from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
import logging
import math

from ..db.database import get_db_connection
from ..db.redis_caches import (get_cache, set_cache,
                             invalidate_table_cache,  
                             cache_table_records, get_cached_table_records,
                             cache_table_count, get_cached_table_count,
                             cache_table_columns, get_cached_table_columns,
                             cache_table_preview, get_cached_table_preview)
from ..model.models import (TableInfo, UpdateRecordRequest, 
                           UpdateRecordResponse, TableRecordCount, TableColumns,
                           PaginatedRecords, SelectedColumnsRecords, 
                           CreateRecordRequest, CreateRecordResponse,
                           DeleteRecordResponse, SingleRecord)

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/api/tables",
    tags=["tables"],
    responses={404: {"description": "Not found"}},
)

# ===============================
# TABLE INFORMATION ROUTES
# ===============================

@router.get("", response_model=List[TableInfo])
async def get_tables(request: Request):
    """Get list of all tables"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    cache_key = f"{session_id}:all_tables"
    cached_data = get_cache(cache_key)
    if cached_data:
        return cached_data
    
    with get_db_connection(session_id) as db:
        tables = db.get_all_tables()
    
    set_cache(cache_key, tables, expire=3600)
    return tables

@router.get("/{table_name}/columns", response_model=TableColumns)
async def get_table_columns(table_name: str, request: Request):
    """Get all columns in a table"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    # Fix: Use session_id and table_name instead of pre-built cache_key
    cached_data = get_cached_table_columns(session_id, table_name)
    if cached_data:
        return {"columns": [{"name": col, "type": "unknown"} for col in cached_data]}
    
    with get_db_connection(session_id) as db:
        columns = db.get_table_columns(table_name)
    
    # Transform column names to column objects
    if columns and isinstance(columns[0], str):
        # If columns are returned as strings, convert to dict format
        column_dicts = [{"name": col, "type": "unknown"} for col in columns]
        result = {"columns": column_dicts}
    else:
        # If columns are already in dict format
        result = {"columns": columns}
    
    # Cache the raw column names (strings), not the formatted result
    cache_table_columns(session_id, table_name, columns, expire=3600)
    return result

@router.get("/{table_name}/count", response_model=TableRecordCount)
async def get_table_count(table_name: str, request: Request):
    """Get total record count for a table"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    cached_count = get_cached_table_count(session_id, table_name)
    if cached_count is not None:
        return {"table": table_name, "count": cached_count}
    
    with get_db_connection(session_id) as db:
        count = db.get_table_record_count(table_name)
    
    cache_table_count(session_id, table_name, count)
    return {"table": table_name, "count": count}

@router.get("/{table_name}/preview", response_model=List[Dict[str, Any]])
async def get_table_preview(table_name: str, request: Request):
    """Get the first 10 records from a table for preview"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    cached_data = get_cached_table_preview(session_id, table_name)
    if cached_data:
        return cached_data
    
    with get_db_connection(session_id) as db:
        records = db.get_first_10_records(table_name)
        
    cache_table_preview(session_id, table_name, records or [])
    return records or []

# ===============================
# RECORD READ OPERATIONS
# ===============================

@router.get("/{table_name}/records", response_model=PaginatedRecords)
async def get_table_records(
    table_name: str,
    request: Request,
    page: int = Query(1, ge=1), 
    page_size: int = Query(50, ge=1, le=1000)
):
    """Get paginated records from a table"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    cached_data = get_cached_table_records(session_id, table_name, page, page_size)
    if cached_data:
        return {**cached_data, "table": table_name, "page": page, "page_size": page_size}
    
    with get_db_connection(session_id) as db:
        result = db.get_paginated_records(table_name, page, page_size)
        
    result["table"] = table_name
    result["page"] = page
    result["page_size"] = page_size
    
    cache_table_records(session_id, table_name, page, page_size, result)
    return result

@router.get("/{table_name}/records/selected", response_model=SelectedColumnsRecords)
async def get_selected_columns_records(
    table_name: str,
    request: Request,
    columns: List[str] = Query(..., description="List of column names to select"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=1000)
):
    """Get records with only selected columns"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    try:
        with get_db_connection(session_id) as db:
            columns_str = ', '.join([f'`{col}`' for col in columns])
            
            count_cursor = db.conn.cursor()
            count_query = f"SELECT COUNT(*) FROM `{table_name}`"
            count_cursor.execute(count_query)
            total_count = count_cursor.fetchone()[0]
            count_cursor.close()
            
            total_pages = math.ceil(total_count / page_size)
            offset = (page - 1) * page_size
            
            cursor = db.conn.cursor(dictionary=True) if hasattr(db.conn, 'cursor') else db.conn.cursor()
            records_query = f"SELECT {columns_str} FROM `{table_name}` LIMIT %s OFFSET %s"
            cursor.execute(records_query, (page_size, offset))
            records = cursor.fetchall()
            cursor.close()
        
        return {
            "table": table_name,
            "columns": columns,
            "page": page,
            "page_size": page_size,
            "total_count": total_count,
            "total_pages": total_pages,
            "records": records or []
        }
        
    except Exception as e:
        logger.error(f"Error getting selected columns for table {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving selected columns: {str(e)}")

@router.get("/{table_name}/records/{record_id}", response_model=SingleRecord)
async def get_single_record(table_name: str, record_id: str, request: Request):
    """Get a single record by ID for editing"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    try:
        with get_db_connection(session_id) as db:
            primary_key = db._get_primary_key_or_first_column(table_name)
            
            cursor = db.conn.cursor(dictionary=True) if hasattr(db.conn, 'cursor') else db.conn.cursor()
            query = f"SELECT * FROM `{table_name}` WHERE `{primary_key}` = %s"
            cursor.execute(query, (record_id,))
            record = cursor.fetchone()
            cursor.close()
            
            if not record:
                raise HTTPException(status_code=404, detail="Record not found")
                
            return {
                "record": record,
                "table": table_name,
                "record_id": record_id
            }
            
    except Exception as e:
        logger.error(f"Error getting record {record_id} from {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving record: {str(e)}")

# ===============================
# RECORD WRITE OPERATIONS
# ===============================

@router.post("/{table_name}/records", response_model=CreateRecordResponse)
async def create_table_record(table_name: str, record_data: CreateRecordRequest, request: Request):
    """Create a new record in the table"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    try:
        with get_db_connection(session_id) as db:
            record_id = db.create_record(table_name, record_data.data)
            
        if record_id:
            invalidate_table_cache(session_id, table_name)
            
            return {
                "success": True,
                "message": f"Successfully created new record in {table_name}",
                "record_id": str(record_id)  # Convert to string
            }
        else:
            return {
                "success": False,
                "message": "Failed to create record",
                "record_id": None
            }
            
    except Exception as e:
        logger.error(f"Error creating record in {table_name}: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False, 
                "message": f"Error creating record: {str(e)}",
                "record_id": None
            }
        )

@router.post("/{table_name}/records/{record_id}", response_model=UpdateRecordResponse)
async def update_table_record(table_name: str, record_id: str, 
                            update_request: UpdateRecordRequest, request: Request):
    """Update a specific record in the table"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    try:
        with get_db_connection(session_id) as db:
            # ✅ Use correct field names from model
            success = db.update_record(table_name, record_id, 
                                     update_request.column, update_request.value)
        
        if success:
            invalidate_table_cache(session_id, table_name)
            
            return {
                "success": True,
                "message": f"Successfully updated record {record_id}"
            }
        else:
            return {
                "success": False,
                "message": f"Failed to update record {record_id}"
            }
            
    except Exception as e:
        logger.error(f"Error updating record {record_id} in {table_name}: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error updating record: {str(e)}"}
        )

# ===============================
# RECORD DELETE OPERATIONS
# ===============================

@router.delete("/{table_name}/records/{record_id}", response_model=DeleteRecordResponse)
async def delete_single_record(table_name: str, record_id: str, request: Request):
    """Delete a single record from the table"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    try:
        with get_db_connection(session_id) as db:
            success = db.delete_record(table_name, record_id)
            
        if success:
            invalidate_table_cache(session_id, table_name)
            
            return {
                "success": True,
                "message": f"Successfully deleted record {record_id}",
                "deleted_count": 1
            }
        else:
            return {
                "success": False,
                "message": f"Failed to delete record {record_id}"
            }
            
    except Exception as e:
        logger.error(f"Error deleting record {record_id} from {table_name}: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error deleting record: {str(e)}"}
        )

@router.delete("/{table_name}/records", response_model=DeleteRecordResponse)
async def bulk_delete_records(table_name: str, request: Request):
    """Delete multiple records (bulk delete)"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    try:
        body = await request.json()
        record_ids = body.get("record_ids", [])
        
        if not record_ids:
            return {
                "success": False,
                "message": "No record IDs provided",
                "deleted_count": 0
            }
        
        with get_db_connection(session_id) as db:
            if hasattr(db, 'bulk_delete_records'):
                deleted_count = db.bulk_delete_records(table_name, record_ids)
            else:
                deleted_count = 0
                for record_id in record_ids:
                    if db.delete_record(table_name, record_id):
                        deleted_count += 1
        
        if deleted_count > 0:
            invalidate_table_cache(session_id, table_name)
            
        return {
            "success": True,
            "message": f"Successfully deleted {deleted_count} record(s)",
            "deleted_count": deleted_count
        }
        
    except Exception as e:
        logger.error(f"Error bulk deleting records from {table_name}: {e}")
        return {
            "success": False,
            "message": f"Error deleting records: {str(e)}",
            "deleted_count": 0
        }

@router.get("/{table_name}/schema")
async def get_table_schema(table_name: str, request: Request):
    """Get detailed table schema for creating/editing records"""
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID required")
    
    try:
        with get_db_connection(session_id) as db:
            cursor = db.conn.cursor(dictionary=True)
            
            # Use DESCRIBE instead of INFORMATION_SCHEMA (simpler and works without database name)
            cursor.execute(f"DESCRIBE `{table_name}`")
            describe_result = cursor.fetchall()
            cursor.close()
            
            # Transform DESCRIBE output to match expected format
            schema_columns = []
            for col in describe_result:
                field_name = col['Field']
                field_type = col['Type']
                is_nullable = col['Null'] == 'YES'
                default_value = col['Default']
                is_primary_key = 'PRI' in (col['Key'] or '')
                extra = col['Extra'] or ''
                is_auto_increment = 'auto_increment' in extra.lower()
                
                # Extract base data type (e.g., 'varchar(255)' -> 'varchar')
                base_type = field_type.split('(')[0].lower()
                
                # Extract max_length for varchar/char types
                max_length = None
                if '(' in field_type and ')' in field_type:
                    try:
                        length_str = field_type.split('(')[1].split(')')[0].split(',')[0]
                        max_length = int(length_str)
                    except (ValueError, IndexError):
                        pass
                
                column_info = {
                    "name": field_name,
                    "type": base_type,
                    "full_type": field_type,
                    "nullable": is_nullable,
                    "default": default_value,
                    "is_primary_key": is_primary_key,
                    "is_auto_increment": is_auto_increment,
                    "max_length": max_length,
                    "precision": None,
                    "scale": None,
                    "required": not is_nullable and default_value is None and not is_auto_increment
                }
                schema_columns.append(column_info)
            
            return {
                "table_name": table_name,
                "columns": schema_columns
            }
            
    except Exception as e:
        logger.error(f"Error getting schema for {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting table schema: {str(e)}")