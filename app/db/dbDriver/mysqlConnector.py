import mysql.connector
import logging
import math  # Add this import
from typing import List, Dict, Any, Optional, Union

logger = logging.getLogger(__name__)

class MySQLConnection:
    def __init__(self):
        self.conn = None

    def connect(self, server: str, database: str, user: str, password: str, port: int = 3306) -> Union[mysql.connector.connection.MySQLConnection, None]:
        try:
            logger.info(f"Connecting to MySQL database {database} on {server}:{port}")
            
            # Create connection parameters dictionary
            connection_params = {
                "host": server,
                "port": int(port),  # Make sure port is an integer
                "database": database,
                "user": user
            }
            
            # Only add password if it's not empty
            if password:
                connection_params["password"] = password
                
            # Log connection attempt with masked password for debugging
            log_params = connection_params.copy()
            if 'password' in log_params:
                log_params['password'] = '********'
            logger.info(f"MySQL connection attempt with parameters: {log_params}")
            
            self.conn = mysql.connector.connect(**connection_params)
            logger.info("Successfully connected to MySQL")
            return self.conn
        except Exception as e:
            logger.error(f"Error connecting to MySQL: {e}")
            return None

    def get_all_tables(self) -> List[Dict[str, str]]:
        try:
            if not self.conn:
                logger.error("No active connection")
                return []
            cursor = self.conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [{'database': self.conn.database, 'table': row[0]} for row in cursor.fetchall()]
            cursor.close()
            logger.info(f"Found {len(tables)} tables")
            return tables
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []

    def get_table_record_count(self, table_name: str) -> int:
        try:
            if not self.conn:
                logger.error("No active connection")
                return 0
            self._validate_table_exists(table_name)
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            count = cursor.fetchone()[0]
            cursor.close()
            logger.info(f"Found {count} records in table {table_name}")
            return count
        except Exception as e:
            logger.error(f"Error getting record count for table {table_name}: {e}")
            return 0

    def get_first_10_records(self, table_name: str) -> List[Dict[str, Any]]:
        """Get first 10 records for preview"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return []
        
            self._validate_table_exists(table_name)
        
            cursor = self.conn.cursor(dictionary=True)
            query = f"SELECT * FROM `{table_name}` LIMIT 10"
            cursor.execute(query)
        
            records = cursor.fetchall()
            cursor.close()
        
            return records or []
        
        except Exception as e:
            logger.error(f"Error getting preview records from table {table_name}: {e}")
            return []

    def get_table_columns(self, table_name: str) -> List[str]:
        try:
            if not self.conn:
                logger.error("No active connection")
                return []
            cursor = self.conn.cursor()
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            logger.info(f"Found {len(columns)} columns in table {table_name}")
            return columns
        except Exception as e:
            logger.error(f"Error getting columns for table {table_name}: {e}")
            return []

    def _get_primary_key_or_first_column(self, table_name: str) -> str:
        try:
            cursor = self.conn.cursor()
            # Try to get primary key
            cursor.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_KEY = 'PRI'
                LIMIT 1
            """, (self.conn.database, table_name))
            result = cursor.fetchone()
            if result:
                cursor.close()
                return result[0]
            # Fallback to first column
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            result = cursor.fetchone()
            cursor.close()
            if result:
                return result[0]
            return "id"
        except Exception as e:
            logger.error(f"Error getting primary key for table {table_name}: {e}")
            return "id"

    def get_paginated_records(self, table_name: str, page: int = 1, page_size: int = 50, columns: List[str] = None) -> Dict[str, Any]:
        try:
            if not self.conn:
                logger.error("No active connection")
                return {"records": [], "total_count": 0, "total_pages": 0}
            self._validate_table_exists(table_name)
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            total_count = cursor.fetchone()[0]
            total_pages = math.ceil(total_count / page_size)
            if columns:
                valid_columns = self._validate_columns(table_name, columns)
                columns_sql = ", ".join([f"`{col}`" for col in valid_columns]) if valid_columns else "*"
            else:
                columns_sql = "*"
            offset = (page - 1) * page_size
            cursor = self.conn.cursor(dictionary=True)
            query = f"SELECT {columns_sql} FROM `{table_name}` LIMIT %s OFFSET %s"
            cursor.execute(query, (page_size, offset))
            records = cursor.fetchall()
            cursor.close()
            logger.info(f"Retrieved {len(records)} records from table {table_name} (page {page}, page_size {page_size})")
            return {
                "records": records or [],
                "total_count": total_count,
                "total_pages": total_pages
            }
        except Exception as e:
            logger.error(f"Error getting paginated records from table {table_name}: {e}")
            return {"records": [], "total_count": 0, "total_pages": 0}

    def _validate_table_exists(self, table_name: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """, (self.conn.database, table_name))
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        if not exists:
            logger.error(f"Table '{table_name}' not found")
            raise ValueError(f"Table '{table_name}' not found")
        return exists

    def _validate_columns(self, table_name: str, columns: List[str]) -> List[str]:
        valid_columns = []
        cursor = self.conn.cursor()
        for col in columns:
            cursor.execute("""
                SELECT COUNT(1) FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
            """, (self.conn.database, table_name, col))
            if cursor.fetchone()[0] > 0:
                valid_columns.append(col)
        cursor.close()
        return valid_columns

    def update_record(self, table_name: str, record_id: str, column_name: str, new_value: Any) -> bool:
        try:
            if not self.conn:
                logger.error("No active connection")
                return False
            self._validate_table_exists(table_name)
            valid_columns = self._validate_columns(table_name, [column_name])
            if not valid_columns:
                logger.error(f"Column '{column_name}' not found in table '{table_name}'")
                raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")
            primary_key = self._get_primary_key_or_first_column(table_name)
            cursor = self.conn.cursor()
            query = f"UPDATE `{table_name}` SET `{column_name}` = %s WHERE `{primary_key}` = %s"
            cursor.execute(query, (new_value, record_id))
            self.conn.commit()
            rows_affected = cursor.rowcount
            cursor.close()
            if rows_affected == 0:
                logger.warning(f"No records updated in table {table_name} with ID {record_id}")
                return False
            logger.info(f"Successfully updated column {column_name} for record {record_id} in table {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error updating record in table {table_name}: {e}")
            if self.conn:
                self.conn.rollback()
            raise e

    # Ensure create_record returns proper type

    def create_record(self, table_name: str, data: Dict[str, Any]) -> Union[int, str, None]:
        """Create a new record in the specified table"""
        try:
            if not self.conn:
                logger.error("No database connection available")
                return None
            
            # Filter out None values and empty strings for non-required fields
            filtered_data = {k: v for k, v in data.items() if v is not None and v != ''}
            
            if not filtered_data:
                logger.error("No valid data provided for record creation")
                return None
            
            # Build INSERT query
            columns = list(filtered_data.keys())
            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join([f'`{col}`' for col in columns])
            
            query = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders})"
            
            cursor = self.conn.cursor()
            cursor.execute(query, list(filtered_data.values()))
            
            # Get the inserted record ID
            record_id = cursor.lastrowid
            
            self.conn.commit()
            cursor.close()
            
            logger.info(f"Successfully created record in {table_name} with ID: {record_id}")
            return record_id  # This returns an integer, which is correct
            
        except Exception as e:
            logger.error(f"Error creating record in {table_name}: {e}")
            if self.conn:
                self.conn.rollback()
            return None

    def delete_record(self, table_name: str, record_id: str) -> bool:
        """Delete a record from the table"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return False
            
            self._validate_table_exists(table_name)
            
            # Get primary key column
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            cursor = self.conn.cursor()
            query = f"DELETE FROM `{table_name}` WHERE `{primary_key}` = %s"
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
            logger.error(f"Error deleting record {record_id} from table {table_name}: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    def bulk_delete_records(self, table_name: str, record_ids: List[str]) -> int:
        """Delete multiple records from the table"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return 0
            
            self._validate_table_exists(table_name)
            
            # Get primary key column
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            # Create placeholders for IN clause
            placeholders = ', '.join(['%s'] * len(record_ids))
            
            cursor = self.conn.cursor()
            query = f"DELETE FROM `{table_name}` WHERE `{primary_key}` IN ({placeholders})"
            cursor.execute(query, record_ids)
            
            self.conn.commit()
            rows_affected = cursor.rowcount
            cursor.close()
            
            logger.info(f"Successfully deleted {rows_affected} records from table {table_name}")
            return rows_affected
            
        except Exception as e:
            logger.error(f"Error bulk deleting records from table {table_name}: {e}")
            if self.conn:
                self.conn.rollback()
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
            
            cursor = self.conn.cursor(dictionary=True)
            query = f"SELECT * FROM `{table_name}` WHERE `{primary_key}` = %s"
            cursor.execute(query, (record_id,))
            
            record = cursor.fetchone()
            cursor.close()
            
            return record
            
        except Exception as e:
            logger.error(f"Error getting record from table {table_name}: {e}")
            return None

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
        

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("MySQL connection closed")
            self.conn = None
