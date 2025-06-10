from fastapi import APIRouter, HTTPException, Request
from typing import Dict, Any
import logging

from ..db.redis_caches import (get_connection_info, clear_all_session_cache, 
                              invalidate_table_cache, redis_client)

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/api/cache",
    tags=["cache"],
    responses={404: {"description": "Not found"}},
)

@router.get("/status/{session_id}")
async def get_cache_status(session_id: str):
    """Get detailed cache statistics for a session"""
    try:
        # Check if session exists
        connection_info = get_connection_info(session_id)
        if not connection_info:
            raise HTTPException(status_code=404, detail="Session not found or expired")
        
        # Define specific patterns for better breakdown
        patterns = {
            "Connection Info": f"connection:{session_id}",
            "Table Lists": f"{session_id}:all_tables",
            "Table Counts": f"{session_id}:table:*:count",
            "Table Columns": f"{session_id}:table:*:columns", 
            "Table Records": f"{session_id}:table:*:records:*",
            "Selected Columns": f"{session_id}:table:*:selected:*",
            "Single Records": f"{session_id}:table:*:record:*",
            "Table Previews": f"{session_id}:table:*:preview",
            "Other Session Data": f"{session_id}:*"
        }
        
        total_keys = 0
        cache_breakdown = {}
        counted_keys = set()
        
        for category, pattern in patterns.items():
            try:
                keys = redis_client.keys(pattern)
                new_keys = [k for k in keys if k not in counted_keys]
                
                if new_keys:
                    cache_breakdown[category] = len(new_keys)
                    counted_keys.update(new_keys)
                    total_keys += len(new_keys)
            except Exception as pattern_error:
                logger.warning(f"Error processing pattern {pattern}: {pattern_error}")
                continue
        
        # Get Redis server info
        try:
            redis_info = redis_client.info()
            redis_stats = {
                "used_memory": redis_info.get("used_memory_human", "Unknown"),
                "connected_clients": redis_info.get("connected_clients", 0),
                "total_commands_processed": redis_info.get("total_commands_processed", 0),
                "keyspace_hits": redis_info.get("keyspace_hits", 0),
                "keyspace_misses": redis_info.get("keyspace_misses", 0),
            }
            
            # Calculate hit rate
            hits = redis_stats["keyspace_hits"]
            misses = redis_stats["keyspace_misses"]
            total_requests = hits + misses
            hit_rate = round((hits / total_requests * 100), 2) if total_requests > 0 else 0
            redis_stats["hit_rate_percentage"] = hit_rate
            
        except Exception as redis_error:
            logger.warning(f"Could not get Redis info: {redis_error}")
            redis_stats = {}
        
        return {
            "session_id": session_id,
            "total_cache_entries": total_keys,
            "cache_breakdown": cache_breakdown,
            "redis_info": redis_stats
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting cache status: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving cache status: {str(e)}")

@router.delete("/clear-all/{session_id}")
async def clear_all_cache(session_id: str):
    """Clear all cache entries for a specific session"""
    try:
        # Check if session exists
        connection_info = get_connection_info(session_id)
        if not connection_info:
            raise HTTPException(status_code=404, detail="Session not found or expired")
        
        # Clear all cache for this session
        cleared_count = clear_all_session_cache(session_id)
        
        logger.info(f"Cleared {cleared_count} cache entries for session {session_id}")
        
        return {
            "success": True,
            "message": "Successfully cleared all cache entries",
            "session_id": session_id,
            "cache_entries_cleared": cleared_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error clearing cache for session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error clearing cache: {str(e)}")

@router.delete("/clear/{session_id}/table/{table_name}")
async def clear_table_cache(session_id: str, table_name: str):
    """Clear cache for a specific table"""
    try:
        # Check if session exists
        connection_info = get_connection_info(session_id)
        if not connection_info:
            raise HTTPException(status_code=404, detail="Session not found or expired")
        
        # Clear table-specific cache
        count = invalidate_table_cache(session_id, table_name)
        
        logger.info(f"Cleared {count} cache entries for table {table_name} in session {session_id}")
        
        return {
            "success": True, 
            "message": f"Cache cleared for table {table_name}",
            "session_id": session_id,
            "table_name": table_name,
            "cleared_entries": count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error clearing cache for table {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error clearing cache: {str(e)}")

@router.get("/health")
async def cache_health_check():
    """Check Redis cache health"""
    try:
        # Test basic Redis operations
        test_key = "health_check_test"
        redis_client.set(test_key, "test_value", ex=10)
        value = redis_client.get(test_key)
        redis_client.delete(test_key)
        
        if value != "test_value":
            raise Exception("Redis read/write test failed")
        
        # Get Redis info
        info = redis_client.info()
        
        return {
            "status": "healthy",
            "redis_connected": True,
            "used_memory": info.get("used_memory_human", "Unknown"),
            "total_connections_received": info.get("total_connections_received", 0),
            "keyspace_hits": info.get("keyspace_hits", 0),
            "keyspace_misses": info.get("keyspace_misses", 0),
            "hit_rate_percentage": round((info.get("keyspace_hits", 0) / max(info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0), 1)) * 100, 2)
        }
        
    except Exception as e:
        logger.error(f"Cache health check failed: {e}")
        return {
            "status": "unhealthy",
            "redis_connected": False,
            "error": str(e)
        }

@router.get("/stats")
async def get_cache_stats():
    """Get overall Redis cache statistics"""
    try:
        info = redis_client.info()
        
        return {
            "redis_version": info.get("redis_version", "Unknown"),
            "used_memory": info.get("used_memory_human", "Unknown"),
            "used_memory_peak": info.get("used_memory_peak_human", "Unknown"),
            "connected_clients": info.get("connected_clients", 0),
            "total_connections_received": info.get("total_connections_received", 0),
            "total_commands_processed": info.get("total_commands_processed", 0),
            "keyspace_hits": info.get("keyspace_hits", 0),
            "keyspace_misses": info.get("keyspace_misses", 0),
            "expired_keys": info.get("expired_keys", 0),
            "evicted_keys": info.get("evicted_keys", 0),
        }
        
    except Exception as e:
        logger.error(f"Error getting cache stats: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving cache stats: {str(e)}")