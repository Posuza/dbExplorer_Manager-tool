import pymssql
import logging
import math  # Add this import
from typing import List, Dict, Any, Optional, Union

# Configure logging
logger = logging.getLogger(__name__)

class MSSQLConnection:
    def __init__(self):
        self.conn = None
        
    def connect(self, server: str, database: str, user: str, password: str, port: int = 1433) -> Union[pymssql.Connection, None]:
        """Connect to Microsoft SQL Server database"""
        try:
            # Try to connect with standard format
            attempt = {
                "server": server,
                "port": port,
                "database": database,
                "user": user,
                "password": password
            }
            
            # Mask password in logs
            log_attempt = {**attempt}
            if 'password' in log_attempt:
                log_attempt['password'] = '********'
            logger.info(f"Connection attempt with parameters: {log_attempt}")
            
            self.conn = pymssql.connect(**attempt)
            logger.info(f"Successfully connected to SQL Server")
            return self.conn
            
        except pymssql.OperationalError as e:
            logger.error(f"Operational error connecting to SQL Server: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error connecting to database: {e}")
            return None

    def get_all_tables(self) -> List[Dict[str, str]]:
        """Get all table names with their database names"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return []
            
            cursor = self.conn.cursor()
            # Query to get all tables with their database names
            query = """
                SELECT 
                    TABLE_CATALOG AS DatabaseName,
                    TABLE_NAME AS TableName
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_CATALOG, TABLE_NAME
            """
            cursor.execute(query)
            
            # Return a list of dictionaries with database and table names
            tables = []
            for row in cursor.fetchall():
                tables.append({
                    'database': row[0],
                    'table': row[1]
                })
            
            cursor.close()
            
            logger.info(f"Found {len(tables)} tables")
            return tables
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
    
    def get_table_record_count(self, table_name: str) -> int:
        """Get the total number of records in a specific table"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return 0
            
            # Validate table name to prevent SQL injection
            self._validate_table_exists(table_name)
            
            cursor = self.conn.cursor()
            # Query to get the count of records in the table
            query = f"SELECT COUNT(*) FROM [{table_name}]"
            
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"Found {count} records in table {table_name}")
            return count
        except Exception as e:
            logger.error(f"Error getting record count for table {table_name}: {e}")
            return 0
    
    def get_first_10_records(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """Get the first 10 records from a specific table"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return None
            
            # Validate table name to prevent SQL injection
            self._validate_table_exists(table_name)
            
            cursor = self.conn.cursor(as_dict=True)
            # Query to get the first 10 records from the table
            query = f"SELECT TOP 10 * FROM [{table_name}]"
            
            cursor.execute(query)
            records = cursor.fetchall()
            cursor.close()
            
            logger.info(f"Retrieved {len(records)} records from table {table_name}")
            return records
        except Exception as e:
            logger.error(f"Error getting records from table {table_name}: {e}")
            return None
            
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get all column names for a specific table"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return []
            
            cursor = self.conn.cursor()
            # Query to get all column names for the table
            query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """
            
            cursor.execute(query, (table_name,))
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Found {len(columns)} columns in table {table_name}")
            return columns
        except Exception as e:
            logger.error(f"Error getting columns for table {table_name}: {e}")
            return []
    
    def _get_primary_key_or_first_column(self, table_name: str) -> str:
        """Get the primary key column or the first column of a table for ordering"""
        try:
            cursor = self.conn.cursor()
            
            # Try to get primary key column first
            query = """
                SELECT column_name
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1
                AND table_name = %s
            """
            cursor.execute(query, (table_name,))
            result = cursor.fetchone()
            
            if result:
                primary_key = result[0]
                cursor.close()
                return primary_key
            
            # If no primary key, get the first column
            query = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """
            cursor.execute(query, (table_name,))
            result = cursor.fetchone()
            
            if result:
                first_column = result[0]
                cursor.close()
                return first_column
            
            # If all else fails, return a default column name that might exist
            cursor.close()
            return "ID"  # Common default primary key name
            
        except Exception as e:
            logger.error(f"Error getting primary key for table {table_name}: {e}")
            return "ID"  # Fallback to a common primary key name
    
    def get_paginated_records(self, table_name: str, page: int = 1, page_size: int = 50, columns: List[str] = None) -> Dict[str, Any]:
        """Get paginated records from a specific table, optionally with specific columns"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return {"records": [], "total_count": 0, "total_pages": 0}
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            # Get total count for pagination info
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
            total_count = cursor.fetchone()[0]
            
            # Calculate pagination values
            total_pages = math.ceil(total_count / page_size)
            
            # Get paginated records with cursor as dict
            cursor = self.conn.cursor(as_dict=True)
            
            # Try to determine SQL Server version to use appropriate pagination
            try:
                cursor.execute("SELECT SERVERPROPERTY('ProductVersion')")
                version_str = cursor.fetchone()[0]
                major_version = int(version_str.split('.')[0])
            except Exception:
                # If version check fails, assume older version
                major_version = 0
                logger.warning("Could not determine SQL Server version, assuming older version")
            
            # If columns are specified, validate them and use only valid ones
            if columns:
                valid_columns = self._validate_columns(table_name, columns)
                columns_sql = ", ".join([f"[{col}]" for col in valid_columns]) if valid_columns else "*"
            else:
                columns_sql = "*"
            
            # Try different pagination methods based on SQL Server version and capabilities
            records = None
            
            # Method 1: Modern pagination with OFFSET/FETCH (SQL Server 2012+)
            if major_version >= 11:
                try:
                    query = f"""
                        SELECT {columns_sql} FROM [{table_name}]
                        ORDER BY (SELECT NULL)
                        OFFSET {(page - 1) * page_size} ROWS 
                        FETCH NEXT {page_size} ROWS ONLY
                    """
                    cursor.execute(query)
                    records = cursor.fetchall()
                    logger.info("Used OFFSET/FETCH pagination")
                except Exception as e:
                    logger.warning(f"OFFSET/FETCH pagination failed: {e}")
                    records = None
            
            # Method 2: ROW_NUMBER() pagination (SQL Server 2005+)
            if records is None:
                try:
                    query = f"""
                        WITH PagedData AS (
                            SELECT {columns_sql}, 
                                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
                            FROM [{table_name}]
                        )
                        SELECT {columns_sql} FROM PagedData
                        WHERE RowNum BETWEEN {(page - 1) * page_size + 1} AND {page * page_size}
                    """
                    cursor.execute(query)
                    records = cursor.fetchall()
                    logger.info("Used ROW_NUMBER() pagination")
                except Exception as e:
                    logger.warning(f"ROW_NUMBER() pagination failed: {e}")
                    records = None
            
            # Method 3: Basic TOP method (most compatible)
            if records is None:
                try:
                    if page == 1:
                        query = f"SELECT TOP {page_size} {columns_sql} FROM [{table_name}]"
                    else:
                        # For pages beyond the first, we need a more complex query
                        # Get a column to use for ordering
                        order_column = self._get_primary_key_or_first_column(table_name)
                        
                        query = f"""
                            SELECT TOP {page_size} {columns_sql}
                            FROM [{table_name}]
                            WHERE [{order_column}] NOT IN (
                                SELECT TOP {(page - 1) * page_size} [{order_column}]
                                FROM [{table_name}]
                                ORDER BY [{order_column}]
                            )
                            ORDER BY [{order_column}]
                        """
                    
                    cursor.execute(query)
                    records = cursor.fetchall()
                    logger.info("Used TOP method pagination")
                except Exception as e:
                    logger.error(f"All pagination methods failed: {e}")
                    records = []
            
            cursor.close()
            
            logger.info(f"Retrieved {len(records) if records else 0} records from table {table_name} (page {page}, page_size {page_size})")
            
            return {
                "records": records or [],
                "total_count": total_count,
                "total_pages": total_pages
            }
        except Exception as e:
            logger.error(f"Error getting paginated records from table {table_name}: {e}")
            return {"records": [], "total_count": 0, "total_pages": 0}
    
    def _validate_table_exists(self, table_name: str) -> bool:
        """Validate that a table exists to prevent SQL injection"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'
        """, (table_name,))
        
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        
        if not exists:
            logger.error(f"Table '{table_name}' not found")
            raise ValueError(f"Table '{table_name}' not found")
        
        return exists
    
    def _validate_columns(self, table_name: str, columns: List[str]) -> List[str]:
        """Validate that columns exist in the table"""
        valid_columns = []
        cursor = self.conn.cursor()
        
        for col in columns:
            cursor.execute("""
                SELECT COUNT(1) FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = ? AND COLUMN_NAME = ?
            """, (table_name, col))
            if cursor.fetchone()[0] > 0:
                valid_columns.append(col)
        
        cursor.close()
        return valid_columns

    def update_record(self, table_name: str, record_id: str, column_name: str, new_value: Any) -> bool:
        """Update a specific field in a record"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return False
            
            # Validate table and column exist
            self._validate_table_exists(table_name)
            valid_columns = self._validate_columns(table_name, [column_name])
            
            if not valid_columns:
                logger.error(f"Column '{column_name}' not found in table '{table_name}'")
                raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")
            
            # Get primary key column for the WHERE clause
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            cursor = self.conn.cursor()
            
            # Prepare and execute the update query - use ? for MSSQL
            query = f"UPDATE [{table_name}] SET [{column_name}] = ? WHERE [{primary_key}] = ?"
            cursor.execute(query, (new_value, record_id))
            
            # Commit the changes
            self.conn.commit()
            
            # Check if any rows were affected
            rows_affected = cursor.rowcount
            cursor.close()
            
            if rows_affected == 0:
                logger.warning(f"No records updated in table {table_name} with ID {record_id}")
                return False
            
            logger.info(f"Successfully updated column {column_name} for record {record_id} in table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating record in table {table_name}: {e}")
            # Rollback in case of error
            if self.conn:
                self.conn.rollback()
            raise e
            
    def get_table_records(self, table_name: str, page: int = 1, page_size: int = 50) -> List[Dict[str, Any]]:
        """Get table records - wrapper for get_paginated_records"""
        try:
            result = self.get_paginated_records(table_name, page, page_size)
            return result.get("records", [])
        except Exception as e:
            logger.error(f"Error getting table records: {e}")
            return []
    
    def get_table_records_with_columns(self, table_name: str, columns: List[str], page: int = 1, page_size: int = 50) -> List[Dict[str, Any]]:
        """Get table records with specific columns"""
        try:
            result = self.get_paginated_records(table_name, page, page_size, columns)
            return result.get("records", [])
        except Exception as e:
            logger.error(f"Error getting table records with columns: {e}")
            return []
    
    def create_record(self, table_name: str, data: Dict[str, Any]) -> Optional[str]:
        """Insert a new record into the table"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return None
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            # Validate columns exist
            valid_columns = self._validate_columns(table_name, list(data.keys()))
            if not valid_columns:
                raise ValueError("No valid columns provided")
            
            # Filter data to only include valid columns
            filtered_data = {col: data[col] for col in valid_columns if col in data}
            
            cursor = self.conn.cursor()
            
            # Prepare INSERT statement
            columns = list(filtered_data.keys())
            placeholders = ", ".join(["?"] * len(columns))
            column_names = ", ".join([f"[{col}]" for col in columns])
            
            query = f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})"
            values = list(filtered_data.values())
            
            cursor.execute(query, values)
            self.conn.commit()
            
            # Get the inserted record ID
            cursor.execute("SELECT @@IDENTITY")
            record_id = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"Successfully created record with ID {record_id} in table {table_name}")
            return str(record_id)
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"Error creating record in table {table_name}: {e}")
            raise e
    
    def delete_record(self, table_name: str, record_id: str) -> bool:
        """Delete a record from the table"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return False
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            # Get primary key column
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            cursor = self.conn.cursor()
            query = f"DELETE FROM [{table_name}] WHERE [{primary_key}] = ?"
            cursor.execute(query, (record_id,))
            self.conn.commit()
            
            rows_affected = cursor.rowcount
            cursor.close()
            
            if rows_affected > 0:
                logger.info(f"Successfully deleted record {record_id} from table {table_name}")
                return True
            else:
                logger.warning(f"No record found with ID {record_id} in table {table_name}")
                return False
                
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"Error deleting record from table {table_name}: {e}")
            raise e
    
    def bulk_delete_records(self, table_name: str, record_ids: List[str]) -> int:
        """Delete multiple records from the table"""
        try:
            if not self.conn or not record_ids:
                return 0
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            # Get primary key column
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            cursor = self.conn.cursor()
            
            # Create placeholders for IN clause
            placeholders = ", ".join(["?"] * len(record_ids))
            delete_query = f"DELETE FROM [{table_name}] WHERE [{primary_key}] IN ({placeholders})"
            
            cursor.execute(delete_query, record_ids)
            self.conn.commit()
            
            deleted_count = cursor.rowcount
            cursor.close()
            
            logger.info(f"Bulk deleted {deleted_count} records from table {table_name}")
            return deleted_count
            
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"Error bulk deleting records from table {table_name}: {e}")
            return 0
    
    def get_record_by_id(self, table_name: str, record_id: str) -> Optional[Dict[str, Any]]:
        """Get a single record by ID"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return None
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            # Get primary key column
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            cursor = self.conn.cursor(as_dict=True)
            query = f"SELECT * FROM [{table_name}] WHERE [{primary_key}] = ?"
            cursor.execute(query, (record_id,))
            
            record = cursor.fetchone()
            cursor.close()
            
            return record
            
        except Exception as e:
            logger.error(f"Error getting record from table {table_name}: {e}")
            return None
    
    def close(self):
        """Close the connection"""
        if self.conn:
            self.conn.close()
            logger.info("Connection closed")
            self.conn = None

