import psycopg2
import psycopg2.extras
import logging
import math  # Add this import
from typing import List, Dict, Any, Optional, Union

logger = logging.getLogger(__name__)

class PostgreSQLConnection:
    def __init__(self):
        self.conn = None
        self.db_name = None

    def connect(self, server: str, database: str, user: str, password: str, port: int = 5432) -> Union[psycopg2.extensions.connection, None]:
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host=server,
                port=port,
                dbname=database,
                user=user,
                password=password
            )
            self.db_name = database
            logger.info(f"Successfully connected to PostgreSQL database {database}")
            return self.conn
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            return None

    def get_all_tables(self) -> List[Dict[str, str]]:
        """Get all table names with their schema names"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return []
            
            cursor = self.conn.cursor()
            # Query to get all tables
            query = """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
            """
            cursor.execute(query)
            
            # Return a list of dictionaries with database and table names
            tables = []
            for row in cursor.fetchall():
                schema, table = row
                table_name = f"{schema}.{table}" if schema != 'public' else table
                tables.append({
                    'database': self.db_name,
                    'table': table_name
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
            
            # Handle schema.table format
            if '.' in table_name:
                schema, table = table_name.split('.')
                table_identifier = f'"{schema}"."{table}"'
            else:
                table_identifier = f'"{table_name}"'
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            cursor = self.conn.cursor()
            # Query to get the count of records in the table
            query = f"SELECT COUNT(*) FROM {table_identifier}"
            
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
            
            # Handle schema.table format
            if '.' in table_name:
                schema, table = table_name.split('.')
                table_identifier = f'"{schema}"."{table}"'
            else:
                table_identifier = f'"{table_name}"'
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Query to get the first 10 records from the table
            query = f"SELECT * FROM {table_identifier} LIMIT 10"
            
            cursor.execute(query)
            records = cursor.fetchall()
            # Convert records to dicts
            records = [dict(record) for record in records]
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
            
            # Handle schema.table format
            schema = 'public'
            table = table_name
            if '.' in table_name:
                schema, table = table_name.split('.')
            
            cursor = self.conn.cursor()
            # Query to get all column names for the table
            query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            
            cursor.execute(query, (schema, table))
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
            # Handle schema.table format
            schema = 'public'
            table = table_name
            if '.' in table_name:
                schema, table = table_name.split('.')
            
            cursor = self.conn.cursor()
            
            # Try to get primary key column first
            query = """
                SELECT a.attname
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                      AND a.attnum = ANY(i.indkey)
                WHERE  i.indrelid = %s::regclass
                AND    i.indisprimary
            """
            cursor.execute(query, (f'{schema}.{table}',))
            result = cursor.fetchone()
            
            if result:
                primary_key = result[0]
                cursor.close()
                return primary_key
            
            # If no primary key, get the first column
            query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                LIMIT 1
            """
            cursor.execute(query, (schema, table))
            result = cursor.fetchone()
            
            if result:
                first_column = result[0]
                cursor.close()
                return first_column
            
            # If all else fails, return a default column name that might exist
            cursor.close()
            return "id"  # Common default primary key name
            
        except Exception as e:
            logger.error(f"Error getting primary key for table {table_name}: {e}")
            return "id"  # Fallback to a common primary key name
    
    def get_paginated_records(self, table_name: str, page: int = 1, page_size: int = 50, columns: List[str] = None) -> Dict[str, Any]:
        """Get paginated records from a specific table, optionally with specific columns"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return {"records": [], "total_count": 0, "total_pages": 0}
            
            # Handle schema.table format
            if '.' in table_name:
                schema, table = table_name.split('.')
                table_identifier = f'"{schema}"."{table}"'
            else:
                table_identifier = f'"{table_name}"'
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            # Get total count for pagination info
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_identifier}")
            total_count = cursor.fetchone()[0]
            
            # Calculate pagination values
            total_pages = math.ceil(total_count / page_size)
            offset = (page - 1) * page_size
            
            # If columns are specified, validate them and use only valid ones
            if columns:
                valid_columns = self._validate_columns(table_name, columns)
                columns_sql = ", ".join([f'"{col}"' for col in valid_columns]) if valid_columns else "*"
            else:
                columns_sql = "*"
            
            # Get paginated records
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            query = f"SELECT {columns_sql} FROM {table_identifier} LIMIT %s OFFSET %s"
            cursor.execute(query, (page_size, offset))
            
            records = cursor.fetchall()
            # Convert records to dicts
            records = [dict(record) for record in records]
            cursor.close()
            
            logger.info(f"Retrieved {len(records)} records from table {table_name} (page {page}, page_size {page_size})")
            
            return {
                "records": records,
                "total_count": total_count,
                "total_pages": total_pages
            }
        except Exception as e:
            logger.error(f"Error getting paginated records from table {table_name}: {e}")
            return {"records": [], "total_count": 0, "total_pages": 0}
    
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
            
            # Handle schema.table format
            if '.' in table_name:
                schema, table = table_name.split('.')
                table_identifier = f'"{schema}"."{table}"'
            else:
                table_identifier = f'"{table_name}"'
            
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
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join([f'"{col}"' for col in columns])
            
            query = f"INSERT INTO {table_identifier} ({column_names}) VALUES ({placeholders}) RETURNING *"
            values = list(filtered_data.values())
            
            cursor.execute(query, values)
            self.conn.commit()
            
            # Get the inserted record
            record = cursor.fetchone()
            record_id = str(record[0]) if record else None
            cursor.close()
            
            logger.info(f"Successfully created record with ID {record_id} in table {table_name}")
            return record_id
            
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
            
            # Handle schema.table format
            if '.' in table_name:
                schema, table = table_name.split('.')
                table_identifier = f'"{schema}"."{table}"'
            else:
                table_identifier = f'"{table_name}"'
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            # Get primary key column
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            cursor = self.conn.cursor()
            query = f'DELETE FROM {table_identifier} WHERE "{primary_key}" = %s'
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
            
            # Handle schema.table format
            if '.' in table_name:
                schema, table = table_name.split('.')
                table_identifier = f'"{schema}"."{table}"'
            else:
                table_identifier = f'"{table_name}"'
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            # Get primary key column
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            cursor = self.conn.cursor()
            
            # Create placeholders for IN clause
            placeholders = ", ".join(["%s"] * len(record_ids))
            delete_query = f'DELETE FROM {table_identifier} WHERE "{primary_key}" IN ({placeholders})'
            
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
            
            # Handle schema.table format
            if '.' in table_name:
                schema, table = table_name.split('.')
                table_identifier = f'"{schema}"."{table}"'
            else:
                table_identifier = f'"{table_name}"'
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            # Get primary key column
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            query = f'SELECT * FROM {table_identifier} WHERE "{primary_key}" = %s'
            cursor.execute(query, (record_id,))
            
            record = cursor.fetchone()
            cursor.close()
            
            return dict(record) if record else None
            
        except Exception as e:
            logger.error(f"Error getting record from table {table_name}: {e}")
            return None
    
    def close(self):
        """Close the connection"""
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed")
            self.conn = None