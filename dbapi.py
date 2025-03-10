from typing import Any, Dict, Optional
import os
import asyncio
import httpx
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration constants
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
DATABRICKS_SQL_WAREHOUSE_ID = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")

# API endpoints
STATEMENTS_API = "/api/2.0/sql/statements"
STATEMENT_API = "/api/2.0/sql/statements/{statement_id}"


async def make_databricks_request(
    method: str,
    endpoint: str,
    json_data: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Make a request to the Databricks API with proper error handling."""
    url = f"{DATABRICKS_HOST}{endpoint}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    async with httpx.AsyncClient() as client:
        try:
            if method.lower() == "get":
                response = await client.get(url, headers=headers, params=params, timeout=30.0)
            elif method.lower() == "post":
                response = await client.post(url, headers=headers, json=json_data, timeout=30.0)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            error_message = f"HTTP error: {e.response.status_code}"
            try:
                error_detail = e.response.json()
                error_message += f" - {error_detail.get('message', '')}"
            except Exception:
                pass
            raise Exception(error_message)
        except Exception as e:
            raise Exception(f"Error making request to Databricks API: {str(e)}")


async def execute_statement(sql: str, warehouse_id: Optional[str] = None) -> Dict[str, Any]:
    """Execute a SQL statement and wait for its completion."""
    if not warehouse_id:
        warehouse_id = DATABRICKS_SQL_WAREHOUSE_ID
    
    if not warehouse_id:
        raise ValueError("Warehouse ID is required. Set DATABRICKS_SQL_WAREHOUSE_ID environment variable or provide it as a parameter.")
    
    # Create the statement
    statement_data = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "0s"  # Don't wait for completion in the initial request
    }
    
    response = await make_databricks_request("post", STATEMENTS_API, json_data=statement_data)
    statement_id = response.get("statement_id")
    
    if not statement_id:
        raise Exception("Failed to get statement ID from response")
    
    # Poll for statement completion
    max_retries = 60  # Maximum number of retries (10 minutes with 10-second intervals)
    retry_count = 0
    
    while retry_count < max_retries:
        statement_status = await make_databricks_request(
            "get", 
            STATEMENT_API.format(statement_id=statement_id)
        )
        
        status = statement_status.get("status", {}).get("state")
        
        if status == "SUCCEEDED":
            return statement_status
        elif status in ["FAILED", "CANCELED"]:
            error_message = statement_status.get("status", {}).get("error", {}).get("message", "Unknown error")
            raise Exception(f"Statement execution failed: {error_message}")
        
        # Wait before polling again
        await asyncio.sleep(10)
        retry_count += 1
    
    raise Exception("Statement execution timed out") 