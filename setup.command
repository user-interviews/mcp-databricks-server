#!/bin/bash

# Databricks MCP Server Setup for Cursor
# Double-click this file to install

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Databricks MCP Server Setup${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Get the directory where this script lives
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAIN_PY="$SCRIPT_DIR/main.py"
REQUIREMENTS="$SCRIPT_DIR/requirements.txt"

# Pre-configured values (same for all users)
DATABRICKS_HOST="dbc-d4dec202-4d5d.cloud.databricks.com"
DATABRICKS_WAREHOUSE_ID="dcb0351ff6ce7e58"

# Cursor config location
CURSOR_DIR="$HOME/.cursor"
MCP_JSON="$CURSOR_DIR/mcp.json"

# Check if main.py exists
if [ ! -f "$MAIN_PY" ]; then
    echo -e "${RED}Error: main.py not found at $MAIN_PY${NC}"
    echo "Make sure you're running this from the mcp-databricks-server folder."
    echo ""
    read -p "Press Enter to exit..."
    exit 1
fi

# Find Python 3.10+ (check brew paths first, then system python3)
echo -e "${YELLOW}Checking Python...${NC}"
PYTHON_CMD=""
for candidate in /opt/homebrew/bin/python3 /usr/local/bin/python3 python3; do
    if command -v "$candidate" &> /dev/null; then
        MINOR=$("$candidate" -c "import sys; print(sys.version_info.minor)" 2>/dev/null || echo "0")
        if [ "$MINOR" -ge 10 ]; then
            PYTHON_CMD="$candidate"
            break
        fi
    fi
done

if [ -z "$PYTHON_CMD" ]; then
    FOUND_VERSION=$(python3 --version 2>/dev/null || echo "None")
    echo -e "${RED}Error: Python 3.10 or newer is required, but found: $FOUND_VERSION${NC}"
    echo ""
    echo "To install a newer version:"
    echo "  Option 1: brew install python"
    echo "  Option 2: Download from https://www.python.org/downloads/"
    echo ""
    echo "After installing, verify with: python3 --version"
    echo ""
    read -p "Press Enter to exit..."
    exit 1
fi

PYTHON_VERSION=$("$PYTHON_CMD" --version)
echo -e "${GREEN}Found: $PYTHON_VERSION ($PYTHON_CMD)${NC}"
echo ""

# Create virtual environment and install dependencies
VENV_DIR="$SCRIPT_DIR/.venv"
echo -e "${YELLOW}Setting up virtual environment...${NC}"

if command -v uv &> /dev/null; then
    echo "Using uv (fast)..."
    uv venv "$VENV_DIR" --quiet 2>/dev/null || uv venv "$VENV_DIR"
    uv pip install -r "$REQUIREMENTS" --quiet -p "$VENV_DIR"
else
    echo "Using pip..."
    "$PYTHON_CMD" -m venv "$VENV_DIR"
    "$VENV_DIR/bin/pip" install -r "$REQUIREMENTS" --quiet
fi

# Use the venv's python for running the server
VENV_PYTHON="$VENV_DIR/bin/python"

echo -e "${GREEN}Dependencies installed!${NC}"
echo ""

# Prompt for Databricks token
echo -e "${YELLOW}You'll need your Databricks Personal Access Token.${NC}"
echo "To create one: Databricks UI → User Settings → Developer → Access Tokens"
echo ""
read -p "Paste your Databricks token here: " DATABRICKS_TOKEN

if [ -z "$DATABRICKS_TOKEN" ]; then
    echo -e "${RED}Error: No token provided.${NC}"
    echo ""
    read -p "Press Enter to exit..."
    exit 1
fi

# Validate token format (basic check)
if [[ ! "$DATABRICKS_TOKEN" =~ ^dapi ]]; then
    echo -e "${YELLOW}Warning: Token doesn't start with 'dapi'. Make sure you copied the full token.${NC}"
    read -p "Continue anyway? (y/n): " CONTINUE
    if [ "$CONTINUE" != "y" ]; then
        exit 1
    fi
fi

echo ""
echo -e "${YELLOW}Configuring Cursor...${NC}"

# Create .cursor directory if needed
mkdir -p "$CURSOR_DIR"

# Build the new server config
NEW_SERVER_CONFIG=$(cat <<EOF
{
    "command": "python3",
    "args": ["$MAIN_PY"],
    "env": {
        "DATABRICKS_HOST": "$DATABRICKS_HOST",
        "DATABRICKS_TOKEN": "$DATABRICKS_TOKEN",
        "DATABRICKS_SQL_WAREHOUSE_ID": "$DATABRICKS_WAREHOUSE_ID"
    }
}
EOF
)

# Check if mcp.json exists and merge, or create new
if [ -f "$MCP_JSON" ]; then
    echo "Found existing mcp.json, merging..."

    # Check if python3 can parse JSON (use it for merging)
    MERGED=$(python3 << PYEOF
import json
import sys

try:
    with open('$MCP_JSON', 'r') as f:
        config = json.load(f)
except:
    config = {}

if 'mcpServers' not in config:
    config['mcpServers'] = {}

config['mcpServers']['databricks'] = {
    "command": "$VENV_PYTHON",
    "args": ["$MAIN_PY"],
    "env": {
        "DATABRICKS_HOST": "$DATABRICKS_HOST",
        "DATABRICKS_TOKEN": "$DATABRICKS_TOKEN",
        "DATABRICKS_SQL_WAREHOUSE_ID": "$DATABRICKS_WAREHOUSE_ID"
    }
}

print(json.dumps(config, indent=2))
PYEOF
)
    echo "$MERGED" > "$MCP_JSON"
else
    echo "Creating new mcp.json..."
    cat > "$MCP_JSON" << EOF
{
  "mcpServers": {
    "databricks": {
      "command": "$VENV_PYTHON",
      "args": ["$MAIN_PY"],
      "env": {
        "DATABRICKS_HOST": "$DATABRICKS_HOST",
        "DATABRICKS_TOKEN": "$DATABRICKS_TOKEN",
        "DATABRICKS_SQL_WAREHOUSE_ID": "$DATABRICKS_WAREHOUSE_ID"
      }
    }
  }
}
EOF
fi

echo -e "${GREEN}Cursor configured!${NC}"
echo ""

# Success message
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Next steps:"
echo "  1. Quit Cursor completely (Cmd+Q)"
echo "  2. Reopen Cursor"
echo "  3. Try asking: \"List my Databricks catalogs\""
echo ""
echo -e "${BLUE}Config saved to: $MCP_JSON${NC}"
echo ""
read -p "Press Enter to close..."
