import cx_Oracle
import logging
import math  # Add this import
from typing import List, Dict, Any, Optional, Union

logger = logging.getLogger(__name__)

class OracleConnection:
    def __init__(self):
        self.conn = None
        self.user = None

    def connect(self, server: str, database: str, user: str, password: str, port: int = 1521) -> Union[cx_Oracle.Connection, None]:
        """Connect to Oracle database"""
        try:
            # Oracle connection string: username/password@host:port/service_name
            dsn = cx_Oracle.makedsn(server, port, service_name=database)
            self.conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
            self.user = user.upper()  # Oracle typically uses uppercase for usernames
            logger.info(f"Successfully connected to Oracle database {database}")
            return self.conn
        except Exception as e:
            logger.error(f"Error connecting to Oracle: {e}")
            return None

    def get_all_tables(self) -> List[Dict[str, str]]:
        """Get all table names with their owner names"""
        try:
            if not self.conn:
                logger.error("No active connection")
                return []
            
            cursor = self.conn.cursor()
            # Query to get all tables accessible to the user
            query = """
                SELECT owner, table_name
                FROM all_tables
                WHERE owner = :owner
                OR table_name IN (
                    SELECT table_name FROM all_tab_privs 
                    WHERE grantee = :grantee AND privilege = 'SELECT'
                )
                ORDER BY owner, table_name
            """
            cursor.execute(query, owner=self.user, grantee=self.user)
            
            # Return a list of dictionaries with owner and table names
            tables = []
            for row in cursor.fetchall():
                owner, table = row
                table_name = f"{owner}.{table}" if owner != self.user else table
                tables.append({
                    'database': owner,
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
            
            # Handle owner.table format
            if '.' in table_name:
                owner, table = table_name.split('.')
                table_identifier = f'"{owner}"."{table}"'
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
            
            # Handle owner.table format
            if '.' in table_name:
                owner, table = table_name.split('.')
                table_identifier = f'"{owner}"."{table}"'
            else:
                table_identifier = f'"{table_name}"'
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            cursor = self.conn.cursor()
            # Query to get columns first for dict creation
            columns = self.get_table_columns(table_name)
            
            # Oracle syntax for limiting rows
            query = f"SELECT * FROM {table_identifier} WHERE ROWNUM <= 10"
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Convert rows to dictionaries
            records = []
            for row in rows:
                record = {}
                for i, col in enumerate(columns):
                    record[col] = row[i]
                records.append(record)
            
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
            
            # Handle owner.table format
            owner = self.user
            table = table_name
            if '.' in table_name:
                owner, table = table_name.split('.')
            
            cursor = self.conn.cursor()
            # Query to get all column names for the table
            query = """
                SELECT column_name
                FROM all_tab_columns
                WHERE owner = :owner AND table_name = :table_name
                ORDER BY column_id
            """
            
            cursor.execute(query, owner=owner, table_name=table)
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
            # Handle owner.table format
            owner = self.user
            table = table_name
            if '.' in table_name:
                owner, table = table_name.split('.')
            
            cursor = self.conn.cursor()
            
            # Try to get primary key column first
            query = """
                SELECT cols.column_name
                FROM all_constraints cons, all_cons_columns cols
                WHERE cons.constraint_type = 'P'
                AND cons.constraint_name = cols.constraint_name
                AND cons.owner = cols.owner
                AND cols.owner = :owner
                AND cols.table_name = :table_name
            """
            cursor.execute(query, owner=owner, table_name=table)
            result = cursor.fetchone()
            
            if result:
                primary_key = result[0]
                cursor.close()
                return primary_key
            
            # If no primary key, get the first column
            query = """
                SELECT column_name
                FROM all_tab_columns
                WHERE owner = :owner AND table_name = :table_name
                ORDER BY column_id
                FETCH FIRST 1 ROW ONLY
            """
            cursor.execute(query, owner=owner, table_name=table)
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
            
            # Handle owner.table format
            if '.' in table_name:
                owner, table = table_name.split('.')
                table_identifier = f'"{owner}"."{table}"'
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
            
            # If columns are specified, validate them and use only valid ones
            if columns:
                valid_columns = self._validate_columns(table_name, columns)
                columns_sql = ", ".join([f'"{col}"' for col in valid_columns]) if valid_columns else "*"
            else:
                columns_sql = "*"
            
            # Oracle pagination using ROWNUM or ROW_NUMBER() window function
            cursor = self.conn.cursor()
            
            # Modern Oracle versions support ROW_NUMBER() for better pagination
            try:
                query = f"""
                    SELECT {columns_sql} FROM (
                        SELECT t.*, ROW_NUMBER() OVER (ORDER BY NULL) AS rn
                        FROM {table_identifier} t
                    )
                    WHERE rn BETWEEN :start_row AND :end_row
                """
                start_row = (page - 1) * page_size + 1
                end_row = page * page_size
                cursor.execute(query, start_row=start_row, end_row=end_row)
            except:
                # Fallback for older Oracle versions
                query = f"""
                    SELECT {columns_sql} FROM (
                        SELECT t.*, ROWNUM rn
                        FROM {table_identifier} t
                        WHERE ROWNUM <= :end_row
                    )
                    WHERE rn >= :start_row
                """
                start_row = (page - 1) * page_size + 1
                end_row = page * page_size
                cursor.execute(query, start_row=start_row, end_row=end_row)
            
            # Get column names for dict creation
            all_columns = self.get_table_columns(table_name)
            if columns:
                all_columns = [col for col in all_columns if col in valid_columns]
            
            # Convert rows to dictionaries
            rows = cursor.fetchall()
            records = []
            for row in rows:
                record = {}
                for i, col in enumerate(all_columns):
                    record[col] = row[i]
                records.append(record)
            
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
    
    def _validate_table_exists(self, table_name: str) -> bool:
        """Validate that a table exists to prevent SQL injection"""
        # Handle owner.table format
        owner = self.user
        table = table_name
        if '.' in table_name:
            owner, table = table_name.split('.')
        
        cursor = self.conn.cursor()
        query = """
            SELECT COUNT(1) FROM all_tables 
            WHERE owner = :owner AND table_name = :table_name
        """
        cursor.execute(query, owner=owner, table_name=table)
        
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        
        if not exists:
            logger.error(f"Table '{table_name}' not found")
            raise ValueError(f"Table '{table_name}' not found")
        
        return exists
    
    def _validate_columns(self, table_name: str, columns: List[str]) -> List[str]:
        """Validate that columns exist in the table"""
        # Handle owner.table format
        owner = self.user
        table = table_name
        if '.' in table_name:
            owner, table = table_name.split('.')
        
        valid_columns = []
        cursor = self.conn.cursor()
        
        for col in columns:
            query = """
                SELECT COUNT(1) FROM all_tab_columns 
                WHERE owner = :owner AND table_name = :table_name AND column_name = :column_name
            """
            cursor.execute(query, owner=owner, table_name=table, column_name=col)
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
            
            # Handle owner.table format
            if '.' in table_name:
                owner, table = table_name.split('.')
                table_identifier = f'"{owner}"."{table}"'
            else:
                table_identifier = f'"{table_name}"'
            
            # Validate table and column exist
            self._validate_table_exists(table_name)
            valid_columns = self._validate_columns(table_name, [column_name])
            
            if not valid_columns:
                logger.error(f"Column '{column_name}' not found in table '{table_name}'")
                raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")
            
            # Get primary key column for the WHERE clause
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            cursor = self.conn.cursor()
            
            # Prepare and execute the update query
            query = f'UPDATE {table_identifier} SET "{column_name}" = :new_value WHERE "{primary_key}" = :record_id'
            cursor.execute(query, new_value=new_value, record_id=record_id)
            
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
            
            # Handle owner.table format
            if '.' in table_name:
                owner, table = table_name.split('.')
                table_identifier = f'"{owner}"."{table}"'
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
            placeholders = ", ".join([":{}".format(i+1) for i in range(len(columns))])
            column_names = ", ".join([f'"{col}"' for col in columns])
            
            query = f"INSERT INTO {table_identifier} ({column_names}) VALUES ({placeholders})"
            values = list(filtered_data.values())
            
            cursor.execute(query, values)
            self.conn.commit()
            
            # Get the inserted record ID (assuming first column is primary key)
            cursor.execute(f"SELECT MAX({columns[0]}) FROM {table_identifier}")
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
            
            # Handle owner.table format
            if '.' in table_name:
                owner, table = table_name.split('.')
                table_identifier = f'"{owner}"."{table}"'
            else:
                table_identifier = f'"{table_name}"'
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            # Get primary key column
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            cursor = self.conn.cursor()
            query = f'DELETE FROM {table_identifier} WHERE "{primary_key}" = :1'
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
            
            # Handle owner.table format
            if '.' in table_name:
                owner, table = table_name.split('.')
                table_identifier = f'"{owner}"."{table}"'
            else:
                table_identifier = f'"{table_name}"'
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            # Get primary key column
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            cursor = self.conn.cursor()
            
            # Oracle doesn't support as many placeholders, so delete in batches
            total_deleted = 0
            batch_size = 1000
            
            for i in range(0, len(record_ids), batch_size):
                batch = record_ids[i:i + batch_size]
                placeholders = ", ".join([f":p{j+1}" for j in range(len(batch))])
                delete_query = f'DELETE FROM {table_identifier} WHERE "{primary_key}" IN ({placeholders})'
                
                cursor.execute(delete_query, batch)
                total_deleted += cursor.rowcount
            
            self.conn.commit()
            cursor.close()
            
            logger.info(f"Bulk deleted {total_deleted} records from table {table_name}")
            return total_deleted
            
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
            
            # Handle owner.table format
            if '.' in table_name:
                owner, table = table_name.split('.')
                table_identifier = f'"{owner}"."{table}"'
            else:
                table_identifier = f'"{table_name}"'
            
            # Validate table exists
            self._validate_table_exists(table_name)
            
            # Get primary key column
            primary_key = self._get_primary_key_or_first_column(table_name)
            
            cursor = self.conn.cursor()
            query = f'SELECT * FROM {table_identifier} WHERE "{primary_key}" = :1'
            cursor.execute(query, (record_id,))
            
            row = cursor.fetchone()
            
            if row:
                # Get column names and create dictionary
                columns = self.get_table_columns(table_name)
                record = {}
                for i, col in enumerate(columns):
                    record[col] = row[i]
                cursor.close()
                return record
            
            cursor.close()
            return None
            
        except Exception as e:
            logger.error(f"Error getting record from table {table_name}: {e}")
            return None
    
    def close(self):
        """Close the connection"""
        if self.conn:
            self.conn.close()
            logger.info("Oracle connection closed")
            self.conn = None