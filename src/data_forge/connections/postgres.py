import sys
from typing import Any, List, Tuple

import psycopg2
from loguru import logger

logger.remove()  # Remove default handler
logger.add(sys.stdout, level="INFO", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}")


def connect_to_postgres(
    host, database, user, password, port=5432
) -> psycopg2.extensions.connection:
    """Connect to PostgreSQL database and return connection object.

    Args:
        host (str): Database host address.
        dbname (str): Database name.
        user (str): Database user.
        password (str): Database password.
        port (int, optional): Database port. Defaults to 5432.

    Returns:
        psycopg2.extensions.connection: PostgreSQL connection object.

    """
    logger.info("Connecting to PostgreSQL database")
    try:
        connection = psycopg2.connect(
            host=host, dbname=database, user=user, password=password, port=port
        )
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        raise


def parse_schema(schema_def: dict) -> str:
    """Parse schema definition and return SQL column definitions.
    Args:
        schema_def (dict): Schema definition.
    Returns:
        str: SQL column definitions."""
    columns = []
    for col in schema_def["columns"]:
        col_def = f"{col['name']} {col['type']}"

        if col.get("primary_key"):
            col_def += " PRIMARY KEY"
        elif not col.get("nullable", True):
            col_def += " NOT NULL"

        if col.get("default"):
            col_def += f" DEFAULT {col['default']}"

        columns.append(col_def)

    return ", ".join(columns)


def create_indexes(
    connection: psycopg2.extensions.connection,
    layer: str,
    table: str,
    indexes: List[dict],
) -> None:
    """Create indexes for the table.
    Args:
        connection (psycopg2.extensions.connection): PostgreSQL connection object.
        layer (str): Layer name.
        table (str): Table name.
        indexes (list[dict]): List of index definitions.
    """
    cursor = connection.cursor()
    try:
        for index in indexes:
            index_name = index["name"]
            columns = ", ".join(index["columns"])
            create_index_query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {layer}.{table} ({columns});"
            cursor.execute(create_index_query)
        connection.commit()
    finally:
        cursor.close()


def create_table(
    connection: psycopg2.extensions.connection, layer: str, table: str, schema: dict
) -> None:
    """Create a table in the PostgreSQL database if not exists.
    Args:
        connection (psycopg2.extensions.connection): PostgreSQL connection object.
        layer (str): Layer name.
        table (str): Table name.
        schema (dict): Schema definition with columns and optional indexes.

    """

    logger.info(f"Creating table {table} in layer {layer}")
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {layer};"
    columns = parse_schema(schema)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {layer}.{table} ({columns});"

    try:
        cursor = connection.cursor()
        cursor.execute(create_schema_query)
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        logger.info(f"Table {layer}.{table} created successfully")

        # Create indexes if defined
        if "indexes" in schema:
            create_indexes(connection, layer, table, schema["indexes"])

    except psycopg2.Error as e:
        logger.error(f"Error creating table {table}: {e}")
        raise


def insert_data(connection, layer: str, table: str, data: dict, schema: dict) -> None:
    """Insert data into the specified table.
    Args:
        connection (psycopg2.extensions.connection): PostgreSQL connection object.
        layer (str): Layer name.
        table (str): Table name.
        data (dict): Data to insert.
        schema (dict): Schema definition.
    """
    
    try:
        logger.info(f"Inserting data into table {table} in layer {layer}")
        
        # Get non-serial columns from schema
        non_serial_columns = [
            col["name"] for col in schema["columns"] 
            if col.get("type", "").upper() != "SERIAL"
        ]
        
        logger.info(f"Non-serial columns: {non_serial_columns}")
        
        # Filter data to only include columns that exist in schema
        filtered_data = {
            key: value for key, value in data.items() 
            if key in non_serial_columns
        }
        
        # Create ordered lists based on schema column order
        columns = []
        values = []
        
        for col_name in non_serial_columns:
            if col_name in filtered_data:
                columns.append(col_name)
                values.append(filtered_data[col_name])
        
        if not columns:
            raise ValueError("No valid columns found for insertion")
        
        # Create the INSERT query
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {layer}.{table} ({columns_str}) VALUES ({placeholders});"
        
        logger.info(f"Executing query: {query}")
        logger.info(f"With data: {tuple(values)}")
        logger.info(f"Columns: {columns}")
        
        # Execute the query
        with connection.cursor() as cursor:
            cursor.execute(query, tuple(values))
            connection.commit()
            
        logger.info(f"Data inserted successfully into {layer}.{table}")
        
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        connection.rollback()
        
        # Add more debug info
        logger.error(f"Data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
        logger.error(f"Schema columns: {[col['name'] for col in schema.get('columns', [])]}")
        
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise
