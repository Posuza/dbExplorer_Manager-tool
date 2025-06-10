from pymongo import MongoClient
import logging
import math  # Add this import
from typing import List, Dict, Any, Optional, Union
from bson import ObjectId

logger = logging.getLogger(__name__)

class MongoDBConnection:
    def __init__(self):
        self.client = None
        self.db = None
        self.db_name = None

    def connect(self, server: str, database: str, user: str = None, password: str = None, port: int = 27017) -> Union[MongoClient, None]:
        """Connect to MongoDB database"""
        try:
            # Build connection string
            if user and password:
                connection_string = f"mongodb://{user}:{password}@{server}:{port}/{database}"
            else:
                connection_string = f"mongodb://{server}:{port}/{database}"
            
            # Connect to MongoDB
            self.client = MongoClient(connection_string)
            self.db = self.client[database]
            self.db_name = database
            
            # Test the connection by listing collections
            self.db.list_collection_names()
            
            logger.info(f"Successfully connected to MongoDB database {database}")
            return self.client
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            return None

    def get_all_tables(self) -> List[Dict[str, str]]:
        """Get all collections (tables) in the database"""
        try:
            if not self.db:
                logger.error("No active connection")
                return []
            
            # In MongoDB, collections are equivalent to tables
            collections = self.db.list_collection_names()
            
            # Return a list of dictionaries with database and collection names
            tables = []
            for collection in collections:
                tables.append({
                    'database': self.db_name,
                    'table': collection
                })
            
            logger.info(f"Found {len(tables)} collections")
            return tables
        except Exception as e:
            logger.error(f"Error getting collections: {e}")
            return []
    
    def get_table_record_count(self, table_name: str) -> int:
        """Get the total number of documents in a specific collection"""
        try:
            if not self.db:
                logger.error("No active connection")
                return 0
            
            # Validate collection exists
            self._validate_table_exists(table_name)
            
            # Count documents in the collection
            count = self.db[table_name].count_documents({})
            
            logger.info(f"Found {count} documents in collection {table_name}")
            return count
        except Exception as e:
            logger.error(f"Error getting document count for collection {table_name}: {e}")
            return 0
    
    def get_first_10_records(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """Get the first 10 documents from a specific collection"""
        try:
            if not self.db:
                logger.error("No active connection")
                return None
            
            # Validate collection exists
            self._validate_table_exists(table_name)
            
            # Get the first 10 documents from the collection
            cursor = self.db[table_name].find({}).limit(10)
            
            # Convert MongoDB documents to dictionaries
            records = []
            for doc in cursor:
                # Convert ObjectId to string
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                records.append(doc)
            
            logger.info(f"Retrieved {len(records)} documents from collection {table_name}")
            return records
        except Exception as e:
            logger.error(f"Error getting documents from collection {table_name}: {e}")
            return None
            
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get all fields (columns) for a specific collection"""
        try:
            if not self.db:
                logger.error("No active connection")
                return []
            
            # Validate collection exists
            self._validate_table_exists(table_name)
            
            # MongoDB doesn't have a fixed schema, so we need to analyze documents
            # to determine all possible fields
            
            # Get a sample document
            sample = self.db[table_name].find_one()
            
            if not sample:
                return ['_id']  # Default field if collection is empty
            
            # Extract all keys from the sample document
            columns = self._extract_all_keys(sample)
            
            logger.info(f"Found {len(columns)} fields in collection {table_name}")
            return columns
        except Exception as e:
            logger.error(f"Error getting fields for collection {table_name}: {e}")
            return []
    
    def _extract_all_keys(self, document: Dict[str, Any], prefix: str = '') -> List[str]:
        """Recursively extract all keys from a nested document"""
        keys = []
        for key, value in document.items():
            full_key = f"{prefix}.{key}" if prefix else key
            keys.append(full_key)
            
            # Handle nested documents
            if isinstance(value, dict):
                nested_keys = self._extract_all_keys(value, full_key)
                keys.extend(nested_keys)
        
        return keys
    
    def _get_primary_key_or_first_column(self, table_name: str) -> str:
        """Get the primary key field (always _id in MongoDB)"""
        return "_id"
    
    def get_paginated_records(self, table_name: str, page: int = 1, page_size: int = 50, columns: List[str] = None) -> Dict[str, Any]:
        """Get paginated documents from a specific collection, optionally with specific fields"""
        try:
            if not self.db:
                logger.error("No active connection")
                return {"records": [], "total_count": 0, "total_pages": 0}
            
            # Validate collection exists
            self._validate_table_exists(table_name)
            
            # Get total count for pagination info
            total_count = self.db[table_name].count_documents({})
            
            # Calculate pagination values
            total_pages = math.ceil(total_count / page_size)
            skip = (page - 1) * page_size
            
            # Prepare projection (fields to include)
            projection = None
            if columns:
                projection = {col: 1 for col in columns}
                # Always include _id for proper identification
                if '_id' not in projection:
                    projection['_id'] = 1
            
            # Get paginated documents
            cursor = self.db[table_name].find({}, projection).skip(skip).limit(page_size)
            
            # Convert MongoDB documents to dictionaries
            records = []
            for doc in cursor:
                # Convert ObjectId to string
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                records.append(doc)
            
            logger.info(f"Retrieved {len(records)} documents from collection {table_name} (page {page}, page_size {page_size})")
            
            return {
                "records": records,
                "total_count": total_count,
                "total_pages": total_pages
            }
        except Exception as e:
            logger.error(f"Error getting paginated documents from collection {table_name}: {e}")
            return {"records": [], "total_count": 0, "total_pages": 0}
    
    def _validate_table_exists(self, table_name: str) -> bool:
        """Validate that a collection exists"""
        if not self.db:
            logger.error("No active connection")
            return False
            
        collections = self.db.list_collection_names()
        exists = table_name in collections
        
        if not exists:
            logger.error(f"Collection '{table_name}' not found")
            raise ValueError(f"Collection '{table_name}' not found")
        
        return exists
    
    def _validate_columns(self, table_name: str, columns: List[str]) -> List[str]:
        """Validate field names (MongoDB has dynamic schema, so we accept all fields)"""
        # For MongoDB, we accept all fields since documents can have any fields
        return columns
    
    def update_record(self, table_name: str, record_id: str, column_name: str, new_value: Any) -> bool:
        """Update a specific field in a document"""
        try:
            if not self.db:
                logger.error("No active connection")
                return False
            
            # Validate collection exists
            self._validate_table_exists(table_name)
            
            # MongoDB uses _id as the primary key
            # Handle nested fields (fields with dots)
            if '.' in column_name:
                update_data = {column_name: new_value}
            else:
                update_data = {column_name: new_value}
            
            # Convert string ID to ObjectId if needed
            try:
                object_id = ObjectId(record_id)
            except:
                object_id = record_id
            
            result = self.db[table_name].update_one(
                {"_id": object_id},
                {"$set": update_data}
            )
            
            # Check if any documents were modified
            if result.modified_count == 0:
                logger.warning(f"No documents updated in collection {table_name} with ID {record_id}")
                return False
            
            logger.info(f"Successfully updated field {column_name} for document {record_id} in collection {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating document in collection {table_name}: {e}")
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
        """Insert a new document into the collection"""
        try:
            if not self.db:
                logger.error("No active connection")
                return None
            
            # Validate collection exists
            self._validate_table_exists(table_name)
            
            # Insert the document
            result = self.db[table_name].insert_one(data)
            
            # Return the inserted ID as string
            record_id = str(result.inserted_id)
            
            logger.info(f"Successfully created document with ID {record_id} in collection {table_name}")
            return record_id
            
        except Exception as e:
            logger.error(f"Error creating document in collection {table_name}: {e}")
            raise e
    
    def delete_record(self, table_name: str, record_id: str) -> bool:
        """Delete a document from the collection"""
        try:
            if not self.db:
                logger.error("No active connection")
                return False
            
            # Validate collection exists
            self._validate_table_exists(table_name)
            
            # Convert string ID to ObjectId if needed
            try:
                object_id = ObjectId(record_id)
            except:
                object_id = record_id
            
            result = self.db[table_name].delete_one({"_id": object_id})
            
            if result.deleted_count > 0:
                logger.info(f"Successfully deleted document {record_id} from collection {table_name}")
                return True
            else:
                logger.warning(f"No document found with ID {record_id} in collection {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting document from collection {table_name}: {e}")
            raise e
    
    def bulk_delete_records(self, table_name: str, record_ids: List[str]) -> int:
        """Delete multiple documents from the collection"""
        try:
            if not self.db or not record_ids:
                return 0
            
            # Validate collection exists
            self._validate_table_exists(table_name)
            
            # Convert string IDs to ObjectIds
            object_ids = []
            for record_id in record_ids:
                try:
                    object_ids.append(ObjectId(record_id))
                except:
                    object_ids.append(record_id)
            
            result = self.db[table_name].delete_many({"_id": {"$in": object_ids}})
            
            deleted_count = result.deleted_count
            logger.info(f"Bulk deleted {deleted_count} documents from collection {table_name}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error bulk deleting documents from collection {table_name}: {e}")
            return 0
    
    def get_record_by_id(self, table_name: str, record_id: str) -> Optional[Dict[str, Any]]:
        """Get a single document by ID"""
        try:
            if not self.db:
                logger.error("No active connection")
                return None
            
            # Validate collection exists
            self._validate_table_exists(table_name)
            
            # Convert string ID to ObjectId if needed
            try:
                object_id = ObjectId(record_id)
            except:
                object_id = record_id
            
            document = self.db[table_name].find_one({"_id": object_id})
            
            if document:
                # Convert ObjectId to string
                if '_id' in document:
                    document['_id'] = str(document['_id'])
                return document
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting document from collection {table_name}: {e}")
            return None
            
    def close(self):
        """Close the connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
            self.client = None
            self.db = None