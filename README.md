# Databricks MCP Server

This is a Model Context Protocol (MCP) server for executing SQL queries against Databricks using the Statement Execution API.
It can retrieve data by performing SQL requests using the Databricks API.
When used in an Agent mode, it can successfully iterate over a number of requests to perform complex tasks.
It is even better when coupled with Unity Catalog Metadata.

## Features

- Execute SQL queries on Databricks
- List available schemas in a catalog
- List tables in a schema
- Describe table schemas

## Setup

### System Requirements

- Python 3.10+
- If you plan to install via `uv`, ensure it's [installed](https://docs.astral.sh/uv/getting-started/installation/#__tabbed_1_1)

### Installation

1. Install the required dependencies:

```bash
pip install -r requirements.txt
```

Or if using `uv`:

```bash
uv pip install -r requirements.txt
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

## Permissions Requirements

Before using this MCP server, ensure that:

1. **SQL Warehouse Permissions**: The user associated with the provided token must have appropriate permissions to access the specified SQL warehouse. You can configure warehouse permissions in the Databricks UI under SQL Warehouses > [Your Warehouse] > Permissions.

2. **Token Permissions**: The personal access token used should have the minimum necessary permissions to perform the required operations. It is strongly recommended to:
   - Create a dedicated token specifically for this application
   - Grant read-only permissions where possible to limit security risks
   - Avoid using tokens with workspace-wide admin privileges

3. **Data Access Permissions**: The user associated with the token must have appropriate permissions to access the catalogs, schemas, and tables that will be queried.

To set SQL warehouse permissions via the Databricks REST API, you can use:
- `GET /api/2.0/sql/permissions/warehouses/{warehouse_id}` to check current permissions
- `PATCH /api/2.0/sql/permissions/warehouses/{warehouse_id}` to update permissions

For security best practices, consider regularly rotating your access tokens and auditing query history to monitor usage.

## Running the Server

### Standalone Mode

To run the server in standalone mode:

```bash
python main.py
```

This will start the MCP server using stdio transport, which can be used with Agent Composer or other MCP clients.

### Using with Cursor

To use this MCP server with [Cursor](https://cursor.sh/), you need to configure it in your Cursor settings:

1. Create a `.cursor` directory in your home directory if it doesn't already exist
2. Create or edit the `mcp.json` file in that directory:

```bash
mkdir -p ~/.cursor
touch ~/.cursor/mcp.json
```

3. Add the following configuration to the `mcp.json` file, replacing the directory path with the actual path to where you've installed this server:

```json
{
    "mcpServers": {
        "databricks": {
            "command": "uv",
            "args": [
                "--directory",
                "/path/to/your/mcp-databricks-server",
                "run",
                "main.py"
            ]
        }
    }
}
```

If you're not using `uv`, you can use `python` instead:

```json
{
    "mcpServers": {
        "databricks": {
            "command": "python",
            "args": [
                "/path/to/your/mcp-databricks-server/main.py"
            ]
        }
    }
}
```

4. Restart Cursor to apply the changes

Now you can use the Databricks MCP server directly within Cursor's AI assistant.

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

