# Databricks MCP Server

This is a Model Context Protocol (MCP) server for executing SQL queries against Databricks using the Statement Execution API.

## Features

- Execute SQL queries on Databricks
- List available schemas in a catalog
- List tables in a schema
- Describe table schemas

## Setup

1. Install the required dependencies:

```bash
pip install -r requirements.txt
```

2. Set up your environment variables:

   Option 1: Using a .env file (recommended)
   
   Create a .env file with your Databricks credentials:
   
   ```
   DATABRICKS_HOST=your-databricks-instance.cloud.databricks.com
   DATABRICKS_TOKEN=your-databricks-access-token
   DATABRICKS_SQL_WAREHOUSE_ID=your-sql-warehouse-id
   ```

   Option 2: Setting environment variables directly
   
   ```bash
   export DATABRICKS_HOST="your-databricks-instance.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-databricks-access-token"
   export DATABRICKS_SQL_WAREHOUSE_ID="your-sql-warehouse-id"
   ```

You can find your SQL warehouse ID in the Databricks UI under SQL Warehouses.

## Running the Server

To run the server:

```bash
python main.py
```

This will start the MCP server using stdio transport, which can be used with Agent Composer or other MCP clients.

## Available Tools

The server provides the following tools:

1. `execute_sql_query`: Execute a SQL query and return the results
   ```
   execute_sql_query(sql: str) -> str
   ```

2. `list_schemas`: List all available schemas in a specific catalog
   ```
   list_schemas(catalog: str) -> str
   ```

3. `list_tables`: List all tables in a specific schema
   ```
   list_tables(schema: str) -> str
   ```

4. `describe_table`: Describe a table's schema
   ```
   describe_table(table_name: str) -> str
   ```

## Example Usage

In Agent Composer or other MCP clients, you can use these tools like:

```
execute_sql_query("SELECT * FROM my_schema.my_table LIMIT 10")
list_schemas("my_catalog")
list_tables("my_catalog.my_schema")
describe_table("my_catalog.my_schema.my_table")
```

## Handling Long-Running Queries

The server is designed to handle long-running queries by polling the Databricks API until the query completes or times out. The default timeout is 10 minutes (60 retries with 10-second intervals), which can be adjusted in the `dbapi.py` file if needed.

## Dependencies

- httpx: For making HTTP requests to the Databricks API
- python-dotenv: For loading environment variables from .env file
- mcp: The Model Context Protocol library
- asyncio: For asynchronous operations

