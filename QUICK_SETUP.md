# Databricks MCP Setup for Cursor

Get AI-powered access to your Databricks data in Cursor in 3 steps.

---

## Before You Start

You'll need your **Databricks Personal Access Token**:

1. Go to Databricks → Click your profile (top right) → **Settings**
2. Click **Developer** → **Access Tokens**
3. Click **Generate New Token**, give it a name, click **Generate**
4. **Copy the token** (you won't see it again!)

---

## Setup Steps

### Step 1: Download

Open Terminal and run:

```bash
git clone https://github.com/user-interviews/mcp-databricks-server.git
```

Or [download the ZIP](https://github.com/user-interviews/mcp-databricks-server/archive/refs/heads/main.zip) and unzip it.

### Step 2: Run Setup

1. Open the `mcp-databricks-server` folder in Finder
2. **Double-click `setup.command`**
3. If macOS asks to confirm, click **Open**
4. Paste your Databricks token when prompted
5. Wait for "Setup Complete!"

### Step 3: Restart Cursor

1. **Quit Cursor completely** (Cmd+Q)
2. Reopen Cursor

---

## Test It

In Cursor, try asking:

> "List my Databricks catalogs"

or

> "What tables are in the analytics schema?"

---

## Troubleshooting

**"Python 3 not found"**
→ Install Python from [python.org/downloads](https://www.python.org/downloads/)

**"Permission denied" when double-clicking**
→ Right-click `setup.command` → Open → Click "Open" in the dialog

**MCP not showing in Cursor**
→ Make sure you fully quit Cursor (Cmd+Q) and reopened it

**"Token invalid" errors**
→ Generate a new token in Databricks and run `setup.command` again

---

## Need Help?

Ask in Slack or contact the Data team.
