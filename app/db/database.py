import logging
from contextlib import contextmanager
from fastapi import HTTPException
from .redis_caches import get_connection_info

logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection(session_id: str):
    """Get a database connection from session ID"""
    connection_info = get_connection_info(session_id)
    
    if not connection_info:
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    
    db_type = connection_info.get("db_type", "mssql")
    
    # Import the appropriate connector based on database type
    if db_type == "mysql":
        from .dbDriver.mysqlConnector import MySQLConnection
        db_class = MySQLConnection
    elif db_type == "postgresql":
        from .dbDriver.postgreSqlConnector import PostgreSQLConnection
        db_class = PostgreSQLConnection
    elif db_type == "oracle":
        from .dbDriver.oracleConnector import OracleConnection
        db_class = OracleConnection
    elif db_type == "sqlite":
        from .dbDriver.sqliteConnector import SQLiteConnection
        db_class = SQLiteConnection
    elif db_type == "mongodb":
        from .dbDriver.mongodbConnector import MongoDBConnection
        db_class = MongoDBConnection
    else:
        # Default to MSSQL
        from .dbDriver.mssqlConnector import MSSQLConnection
        db_class = MSSQLConnection
    
    # Create database connection
    db = db_class()
    
    try:
        # Connect to database
        conn = db.connect(
            server=connection_info["server"],
            database=connection_info["database"],
            user=connection_info["user"],
            password=connection_info["password"],
            port=connection_info["port"]
        )
        
        if not conn:
            raise HTTPException(status_code=500, detail="Failed to connect to database")
        
        yield db
        
    finally:
        # Close connection
        if hasattr(db, 'close'):
            db.close()