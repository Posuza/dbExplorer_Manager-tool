from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from typing import List
import logging
import uuid

from ..db.database import get_db_connection
from ..db.redis_caches import (store_connection_info, get_connection_info, 
                              delete_connection_info, list_active_connections,
                              clear_all_session_cache)
from ..model.models import ConnectionParams, ConnectionResponse, TableInfo

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/api",
    tags=["connection"],
    responses={404: {"description": "Not found"}},
)

@router.post("/connect", response_model=ConnectionResponse)
async def connect_to_db(params: ConnectionParams):
    """Connect to database and create session"""
    try:
        db_type = getattr(params, "db_type", "mysql")
        
        # Import the appropriate connector based on database type
        if db_type == "mysql":
            from ..db.dbDriver.mysqlConnector import MySQLConnection as DBConnection
        elif db_type == "postgresql":
            from ..db.dbDriver.postgreSqlConnector import PostgreSQLConnection as DBConnection
        elif db_type == "oracle":
            from ..db.dbDriver.oracleConnector import OracleConnection as DBConnection
        elif db_type == "sqlite":
            from ..db.dbDriver.sqliteConnector import SQLiteConnection as DBConnection
        elif db_type == "mongodb":
            from ..db.dbDriver.mongoDbConnector import MongoDBConnection as DBConnection
        else:
            from ..db.dbDriver.mssqlConnector import MSSQLConnection as DBConnection
        
        db = DBConnection()
        
        try:
            conn = db.connect(
                server=params.server,
                database=params.database,
                user=params.user,
                password=params.password,
                port=params.port
            )
            
            if not conn:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Failed to connect to {db_type} database"
                )
            
            # Create session
            session_id = str(uuid.uuid4())
            connection_params = {
                "server": params.server,
                "database": params.database,
                "user": params.user,
                "password": params.password,
                "port": params.port,
                "db_type": db_type
            }
            
            # Store connection info in Redis with 1 hour expiration
            store_connection_info(session_id, connection_params, expire=3600)
            
            # Get tables
            tables = db.get_all_tables()
            
            logger.info(f"Successfully connected to {db_type} database {params.database}")
            
            # Create response with cookie
            response = JSONResponse(content={
                "session_id": session_id,
                "tables": tables,
                "db_type": db_type,
                "database": params.database
            })
            
            # Set session cookie
            response.set_cookie(
                key="session_id",
                value=session_id,
                httponly=True,
                secure=False,
                samesite="lax",
                max_age=3600
            )
            
            return response
            
        except Exception as db_error:
            logger.error(f"Database connection error for {db_type}: {db_error}")
            raise HTTPException(
                status_code=400,
                detail=f"Database connection failed: {str(db_error)}"
            )
        finally:
            if 'db' in locals() and hasattr(db, 'close'):
                try:
                    db.close()
                except Exception as close_error:
                    logger.warning(f"Error closing database connection: {close_error}")
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in connect_to_db: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@router.get("/reconnect/{session_id}", response_model=ConnectionResponse)
async def reconnect_to_db(session_id: str):
    """Reconnect using existing session"""
    try:
        connection_info = get_connection_info(session_id)
        if not connection_info:
            raise HTTPException(status_code=404, detail="Session not found or expired")
        
        # Test connection
        with get_db_connection(session_id) as db:
            tables = db.get_all_tables()
        
        logger.info(f"Successfully reconnected session {session_id}")
        
        return {
            "session_id": session_id,
            "tables": tables,
            "db_type": connection_info.get("db_type"),
            "database": connection_info.get("database")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reconnecting session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Reconnection failed: {str(e)}")

@router.post("/disconnect/{session_id}")
async def disconnect_db(session_id: str):
    """Disconnect from database and remove session"""
    try:
        # Call cache clearing function instead of implementing it here
        from .cache import clear_all_cache
        cache_result = await clear_all_cache(session_id)
        
        # Only handle connection deletion here
        connection_deleted = delete_connection_info(session_id)
        
        return {
            "message": "Successfully disconnected from database",
            "session_id": session_id,
            "cache_entries_cleared": cache_result.get("cache_entries_cleared", 0)
        }
    except Exception as e:
        logger.error(f"Error disconnecting session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error disconnecting: {str(e)}")

@router.get("/connection-info/{session_id}")
async def get_connection_info_route(session_id: str):
    """Get connection information for a session"""
    try:
        connection_info = get_connection_info(session_id)
        if not connection_info:
            raise HTTPException(status_code=404, detail="Session not found or expired")
        
        # Don't return sensitive info like password
        safe_info = {
            "session_id": session_id,
            "server": connection_info.get("server"),
            "database": connection_info.get("database"),
            "user": connection_info.get("user"),
            "port": connection_info.get("port"),
            "db_type": connection_info.get("db_type")
        }
        
        return safe_info
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting connection info for session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving connection info: {str(e)}")

@router.get("/sessions")
async def get_active_sessions():
    """Get list of all active sessions"""
    try:
        sessions = list_active_connections()
        return {"active_sessions": sessions}
    except Exception as e:
        logger.error(f"Error getting active sessions: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting sessions: {str(e)}")

@router.delete("/sessions/{session_id}")
async def force_disconnect_session(session_id: str):
    """Force disconnect a specific session (admin function)"""
    try:
        # Clear cache and connection info
        cleared_count = clear_all_session_cache(session_id)
        connection_deleted = delete_connection_info(session_id)
        
        return {
            "message": f"Force disconnected session {session_id}",
            "cache_entries_cleared": cleared_count,
            "connection_deleted": connection_deleted
        }
        
    except Exception as e:
        logger.error(f"Error force disconnecting session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error force disconnecting: {str(e)}")