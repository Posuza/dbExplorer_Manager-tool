import redis
import json
import logging
import math  # Add this import
import os
from typing import Any, Optional, Dict, List
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    password=REDIS_PASSWORD,
    decode_responses=True
)

def set_cache(key: str, value: Any, expire: int = 3600) -> bool:
    """Set a value in Redis cache with expiration"""
    try:
        if isinstance(value, (dict, list)):
            redis_client.setex(key, expire, json.dumps(value, default=str))
        else:
            redis_client.setex(key, expire, str(value))
        return True
    except Exception as e:
        logger.error(f"Error setting cache for key {key}: {e}")
        return False

def get_cache(key: str) -> Optional[Any]:
    """Get data from Redis cache with error recovery"""
    try:
        value = redis_client.get(key)
        if value:
            try:
                return json.loads(value)
            except json.JSONDecodeError as json_error:
                logger.warning(f"Corrupted cache data for key {key}, deleting: {json_error}")
                redis_client.delete(key)  # ✅ Auto-cleanup corrupted data
                return None
        return None
    except redis.RedisError as e:
        logger.error(f"Redis error getting key {key}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting cache key {key}: {e}")
        return None

def delete_cache(key: str) -> bool:
    """Delete a specific cache key"""
    try:
        result = redis_client.delete(key)
        if result:
            logger.debug(f"Deleted cache key: {key}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error deleting cache key {key}: {e}")
        return False

def clear_all_session_cache(session_id: str) -> int:
    """Clear all cache entries for a specific session"""
    try:
        patterns = [
            f"connection:{session_id}",
            f"{session_id}:*"
        ]
        
        total_cleared = 0
        for pattern in patterns:
            keys = redis_client.keys(pattern)
            if keys:
                deleted = redis_client.delete(*keys)
                total_cleared += deleted
                
        logger.info(f"Cleared {total_cleared} cache entries for session {session_id}")
        return total_cleared
        
    except Exception as e:
        logger.error(f"Error clearing cache for session {session_id}: {e}")
        return 0

def clear_table_cache(table_name: str) -> int:
    """
    Clear all cache entries related to a specific table
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        int: Number of cache entries cleared
    """
    try:
        pattern = f"table:{table_name}:*"
        keys = redis_client.keys(pattern)
        if keys:
            deleted = redis_client.delete(*keys)
            logger.info(f"Cleared {deleted} cache entries for table {table_name}")
            return deleted
        return 0
    except Exception as e:
        logger.error(f"Error clearing cache for table {table_name}: {e}")
        return 0

def cache_table_data(table_name: str, page: int, page_size: int, data: Dict[str, Any]) -> bool:
    """
    Cache paginated table data
    
    Args:
        table_name (str): Name of the table
        page (int): Page number
        page_size (int): Page size
        data (Dict): Data to cache
        
    Returns:
        bool: True if cached successfully, False otherwise
    """
    key = f"table:{table_name}:page:{page}:size:{page_size}"
    return set_cache(key, data)

def get_cached_table_data(table_name: str, page: int, page_size: int) -> Optional[Dict[str, Any]]:
    """
    Get cached paginated table data
    
    Args:
        table_name (str): Name of the table
        page (int): Page number
        page_size (int): Page size
        
    Returns:
        Optional[Dict]: Cached data or None if not found
    """
    key = f"table:{table_name}:page:{page}:size:{page_size}"
    return get_cache(key)

def store_db_info(session_id: str, db_info: dict, ttl: int = 3600):
    """
    Store database information in Redis with a time-to-live (TTL)
    
    Args:
        session_id (str): Unique session identifier
        db_info (dict): Database information to store
        ttl (int): Time-to-live in seconds (default: 1 hour)
    """
    try:
        redis_client.set(f"dbinfo:{session_id}", json.dumps(db_info), ex=ttl)
        logger.info(f"Stored DB info for session {session_id}")
    except Exception as e:
        logger.error(f"Error storing DB info for session {session_id}: {e}")

def delete_db_info(session_id: str):
    """
    Delete database information from Redis
    
    Args:
        session_id (str): Unique session identifier
    """
    try:
        redis_client.delete(f"dbinfo:{session_id}")
        logger.info(f"Deleted DB info for session {session_id}")
    except Exception as e:
        logger.error(f"Error deleting DB info for session {session_id}: {e}")

# New functions for connection info management
def store_connection_info(session_id: str, connection_params: Dict[str, Any], expire: int = 3600) -> bool:
    """
    Store database connection information in Redis
    
    Args:
        session_id (str): Unique session identifier
        connection_params (Dict): Connection parameters
        expire (int): Expiration time in seconds (default: 1 hour)
        
    Returns:
        bool: True if stored successfully, False otherwise
    """
    # Mask password in logs
    log_params = {**connection_params}
    if 'password' in log_params:
        log_params['password'] = '********'
    
    logger.info(f"Storing connection info for session {session_id}: {log_params}")
    
    key = f"connection:{session_id}"
    return set_cache(key, connection_params, expire)

def get_connection_info(session_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve database connection information from Redis
    
    Args:
        session_id (str): Unique session identifier
        
    Returns:
        Optional[Dict]: Connection parameters or None if not found
    """
    key = f"connection:{session_id}"
    return get_cache(key)

def delete_connection_info(session_id: str) -> bool:
    """Delete connection info from Redis cache"""
    try:
        redis_key = f"connection:{session_id}"
        result = redis_client.delete(redis_key)
        logger.info(f"Deleted connection info for session {session_id}")
        return bool(result)
    except Exception as e:
        logger.error(f"Error deleting connection info: {e}")
        return False

def list_active_connections() -> List[str]:
    """
    List all active database connections
    
    Returns:
        List[str]: List of active session IDs
    """
    try:
        keys = redis_client.keys("connection:*")
        return [key.replace("connection:", "") for key in keys]
    except Exception as e:
        logger.error(f"Error listing active connections: {e}")
        return []

def cleanup_expired_sessions() -> int:
    """Clean up cache for expired sessions"""
    try:
        # Get all connection keys
        connection_keys = redis_client.keys("connection:*")
        cleaned_sessions = 0
        
        for conn_key in connection_keys:
            # Check if connection info still exists
            if not redis_client.exists(conn_key):
                # Extract session_id from key
                session_id = conn_key.replace("connection:", "")
                
                # Clean up all cache for this expired session
                cleared = delete_all_session_cache(session_id)
                if cleared > 0:
                    cleaned_sessions += 1
                    logger.info(f"Cleaned up expired session {session_id}: {cleared} cache entries")
        
        return cleaned_sessions
        
    except Exception as e:
        logger.error(f"Error cleaning up expired sessions: {e}")
        return 0

def cache_table_columns(session_id: str, table_name: str, columns: List[str], expire: int = 3600) -> bool:
    """Cache table columns"""
    key = f"{session_id}:table:{table_name}:columns"
    return set_cache(key, columns, expire)

def get_cached_table_columns(session_id: str, table_name: str) -> Optional[List[str]]:
    """Get cached table columns"""
    key = f"{session_id}:table:{table_name}:columns"
    return get_cache(key)

def cache_table_records(session_id: str, table_name: str, page: int, page_size: int, 
                       records_data: Dict[str, Any], expire: int = 1800) -> bool:
    """Cache paginated table records (30 min expiry for records)"""
    key = f"{session_id}:table:{table_name}:records:page:{page}:size:{page_size}"
    return set_cache(key, records_data, expire)

def get_cached_table_records(session_id: str, table_name: str, page: int, page_size: int) -> Optional[Dict[str, Any]]:
    """Get cached table records"""
    key = f"{session_id}:table:{table_name}:records:page:{page}:size:{page_size}"
    return get_cache(key)

def cache_table_count(session_id: str, table_name: str, count: int, expire: int = 1800) -> bool:
    """Cache table record count"""
    key = f"{session_id}:table:{table_name}:count"
    return set_cache(key, count, expire)

def get_cached_table_count(session_id: str, table_name: str) -> Optional[int]:
    """Get cached table record count"""
    key = f"{session_id}:table:{table_name}:count"
    return get_cache(key)

def cache_selected_columns_records(session_id: str, table_name: str, columns: List[str], 
                                 page: int, page_size: int, records_data: Dict[str, Any], 
                                 expire: int = 1800) -> bool:
    """Cache records with selected columns"""
    columns_hash = hash(tuple(sorted(columns)))
    key = f"{session_id}:table:{table_name}:selected:{columns_hash}:page:{page}:size:{page_size}"
    return set_cache(key, records_data, expire)

def get_cached_selected_columns_records(session_id: str, table_name: str, columns: List[str], 
                                      page: int, page_size: int) -> Optional[Dict[str, Any]]:
    """Get cached records with selected columns"""
    columns_hash = hash(tuple(sorted(columns)))
    key = f"{session_id}:table:{table_name}:selected:{columns_hash}:page:{page}:size:{page_size}"
    return get_cache(key)

def invalidate_table_cache(session_id: str, table_name: str) -> int:
    """Invalidate all cache entries for a specific table"""
    try:
        patterns = [
            f"{session_id}:table:{table_name}:*",
            f"{session_id}:table:{table_name}:records:*",
            f"{session_id}:table:{table_name}:selected:*",
            f"{session_id}:table:{table_name}:count",
            f"{session_id}:table:{table_name}:columns",
        ]
        
        total_cleared = 0
        for pattern in patterns:
            keys = redis_client.keys(pattern)
            if keys:
                deleted = redis_client.delete(*keys)
                total_cleared += deleted
                
        logger.info(f"Invalidated {total_cleared} cache entries for table {table_name}")
        return total_cleared
        
    except Exception as e:
        logger.error(f"Error invalidating cache for table {table_name}: {e}")
        return 0

def cache_single_record(session_id: str, table_name: str, record_id: str, 
                       record_data: Dict[str, Any], expire: int = 1800) -> bool:
    """Cache single record data"""
    key = f"{session_id}:table:{table_name}:record:{record_id}"
    return set_cache(key, record_data, expire)

def get_cached_single_record(session_id: str, table_name: str, record_id: str) -> Optional[Dict[str, Any]]:
    """Get cached single record"""
    key = f"{session_id}:table:{table_name}:record:{record_id}"
    return get_cache(key)

def invalidate_record_cache(session_id: str, table_name: str, record_id: str) -> bool:
    """Invalidate cache for a specific record"""
    key = f"{session_id}:table:{table_name}:record:{record_id}"
    return delete_cache(key)

def close():
    """Close Redis connection pool"""
    try:
        redis_client.close()
        logger.info("Redis connection pool closed")
    except Exception as e:
        logger.error(f"Error closing Redis connection: {e}")

def flush_all_cache() -> bool:
    """Emergency function to flush ALL Redis cache (use with caution!)"""
    try:
        redis_client.flushdb()
        logger.warning("Flushed ALL cache from Redis database")
        return True
    except Exception as e:
        logger.error(f"Error flushing all cache: {e}")
        return False

def cache_table_preview(session_id: str, table_name: str, preview_data: List[Dict[str, Any]], expire: int = 300) -> bool:
    """Cache table preview data (5 min expiry for previews)"""
    key = f"{session_id}:table:{table_name}:preview"
    return set_cache(key, preview_data, expire)

def get_cached_table_preview(session_id: str, table_name: str) -> Optional[List[Dict[str, Any]]]:
    """Get cached table preview"""
    key = f"{session_id}:table:{table_name}:preview"
    return get_cache(key)