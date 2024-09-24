import os
import logging
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

# Set up logging for application monitoring and debugging.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Database:
    """
    A class to handle PostgreSQL database interactions using a connection pool. 
    Supports executing queries, managing connections, and performing common database 
    operations like table creation, insertion, updating, and deletion.
    """

    def __init__(self):
        """
        Initializes the Database class and sets the connection pool to None.
        """
        self.connection_pool = None

    def load_database_config(self):
        """
        Loads database configuration parameters from environment variables using dotenv.
        
        Returns:
            db_config (dict): A dictionary containing the database connection parameters.
        
        Raises:
            ValueError: If any required environment variable is missing.
        """
        load_dotenv()
        db_config = {
            'host': os.getenv('DB_HOST'),
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT')
        }

        # Check for missing environment variables.
        missing_vars = [key for key, value in db_config.items() if value is None]
        if missing_vars:
            raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")
        
        return db_config

    def initialize_pool(self, minconn=1, maxconn=10):
        """
        Initializes a connection pool with the specified minimum and maximum number of connections.
        
        Args:
            minconn (int): Minimum number of connections in the pool.
            maxconn (int): Maximum number of connections in the pool.
        
        Raises:
            Exception: If there is an error initializing the connection pool.
        """
        try:
            db_config = self.load_database_config()
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=minconn,
                maxconn=maxconn,
                host=db_config['host'],
                dbname=db_config['dbname'],
                user=db_config['user'],
                password=db_config['password'],
                port=db_config['port'],
                cursor_factory=RealDictCursor  # Use RealDictCursor to return results as dictionaries.
            )
        except Exception as e:
            logger.error(f"Error initializing the connection pool: {e}")
            raise

    def get_connection(self):
        """
        Retrieves a connection from the pool.
        
        Returns:
            connection: A connection object from the pool.
        
        Raises:
            Exception: If the connection pool is not initialized.
        """
        if not self.connection_pool:
            raise Exception("Connection pool is not initialized")
        return self.connection_pool.getconn()

    def release_connection(self, conn):
        """
        Releases a connection back to the connection pool.
        
        Args:
            conn: The connection object to be released.
        """
        if conn:
            self.connection_pool.putconn(conn)

    def close_all_connections(self):
        """
        Closes all connections in the pool when shutting down the database or application.
        """
        if self.connection_pool:
            self.connection_pool.closeall()

    def execute_sql_query(self, query, params=None, batch=False):
        """
        Executes a SQL query with optional parameters, and supports batch execution for bulk inserts.
        
        Args:
            query (str): The SQL query to be executed.
            params (tuple or list): Parameters to be passed into the SQL query.
            batch (bool): If True, executes the query in batch mode using executemany().
        
        Returns:
            results (list or None): Query results for SELECT queries, None for other queries.
        
        Raises:
            psycopg2.DatabaseError: If there is a database error during query execution.
            Exception: If there is a general error during query execution.
        """
        connection = None
        try:
            connection = self.get_connection()
            with connection.cursor() as cursor:
                # Execute query in batch mode or single execution based on 'batch' flag.
                if batch and params:
                    cursor.executemany(query, params)
                elif params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Return results for SELECT queries; commit changes for others.
                if str(query).strip().upper().startswith("SELECT"):
                    results = cursor.fetchall()
                else:
                    results = ""
                    connection.commit()
        
        except psycopg2.DatabaseError as db_error:
            if connection:
                connection.rollback()  # Rollback in case of any errors.
            logger.error(f"Database error during query execution: {db_error}")
            raise
        except Exception as error:
            logger.error(f"Error executing query: {error}")
            raise
        finally:
            if connection:
                self.release_connection(connection)

        return results
    
    def create_table(self, table_name, columns, primary_key=None):
        """
        Creates a table in the database if it does not already exist.
        
        Args:
            table_name (str): Name of the table to be created.
            columns (dict): Dictionary with column names as keys and column attributes as values.
            primary_key (str): Optional primary key for the table.
        
        Raises:
            Exception: If there is an error executing the query.
        """
        column_definitions = []
        foreign_key_definitions = []

        # Construct SQL for column definitions and foreign key constraints.
        for column_name, column_info in columns.items():
            column_type = column_info.get('type', 'TEXT')
            constraints = column_info.get('constraints', '')

            if 'foreign_key' in column_info:
                references = column_info['references']
                on_delete_action = column_info.get('on_delete', 'NO ACTION')
                foreign_key_definitions.append(
                    f"FOREIGN KEY ({column_name}) REFERENCES {references} ON DELETE {on_delete_action}"
                )
            else:
                column_definitions.append(f"{column_name} {column_type} {constraints}".strip())

        columns_sql = ", ".join(column_definitions)
        foreign_keys_sql = ", " + ", ".join(foreign_key_definitions) if foreign_key_definitions else ""

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_sql}{foreign_keys_sql}
            );
        """
        self.execute_sql_query(create_table_query)

    def insert_data(self, table_name, columns, values, on_conflict="DO NOTHING"):
        """
        Inserts data into a table, with support for conflict resolution.
        
        Args:
            table_name (str): Name of the table for insertion.
            columns (list): List of column names to insert data into.
            values (list): A list of tuples, where each tuple contains values for a row.
            on_conflict (str): Conflict resolution strategy, default is 'DO NOTHING'.
        
        Raises:
            Exception: If there is an error executing the query.
        """
        columns_sql = sql.SQL(', ').join(map(sql.Identifier, columns))
        placeholders_sql = sql.SQL(', ').join(sql.Placeholder() for _ in columns)

        insert_query = sql.SQL("""
            INSERT INTO {table} ({columns})
            VALUES ({values})
            ON CONFLICT {conflict_action}
        """).format(
            table=sql.Identifier(table_name),
            columns=columns_sql,
            values=placeholders_sql,
            conflict_action=sql.SQL(on_conflict)
        )

        # Insert the data in batch mode.
        self.execute_sql_query(insert_query, values, batch=True)

    def update_records(self, table_name, set_columns, where_conditions, values):
        """
        Updates records in a table based on specified conditions.
        
        Args:
            table_name (str): The name of the table to update.
            set_columns (list): Columns to be updated.
            where_conditions (list): Conditions to filter which rows are updated.
            values (dict): A dictionary containing column-value pairs for the update operation.
        
        Raises:
            Exception: If there is an error executing the query.
        """
        set_sql = ', '.join([f"{col} = %({col})s" for col in set_columns])
        where_sql = ' AND '.join([f"{col} = %({col})s" for col in where_conditions])

        update_query = sql.SQL(f"""
            UPDATE {table_name}
            SET {set_sql}
            WHERE {where_sql}
        """)

        self.execute_sql_query(update_query, values)

    def delete_records(self, table_name, where_conditions=None):
        """
        Deletes records from a table based on specified conditions. If no conditions are provided, 
        all records in the table will be deleted.
        
        Args:
            table_name (str): The name of the table from which records are deleted.
            where_conditions (dict, optional): Conditions to specify which records to delete. 
            If None, deletes all records.
        
        Raises:
            Exception: If there is an error executing the query.
        """
        if where_conditions:
            # Generate WHERE clause if conditions are provided.
            where_sql = ' AND '.join([f"{col} = %({col})s" for col in where_conditions])
            delete_query = sql.SQL(f"""
                DELETE FROM {table_name}
                WHERE {where_sql}
            """)
            self.execute_sql_query(delete_query, where_conditions)
        else:
            # Delete all records if no conditions are provided.
            delete_query = sql.SQL(f"""
                DELETE FROM {table_name}
            """)
            self.execute_sql_query(delete_query)