# mcp_oracle_scm

This is an MCP server for "mcp_oracle_scm", using the [python-sdk](https://github.com/modelcontextprotocol/python-sdk).

## Install Instructions

Before starting, please make sure you have followed the [Setup](https://github.com/squareup/mcp/blob/main/README.md#setup) first.

To install this MCP from artifactory:
```
uvx mcp_oracle_scm@latest
```

## Contribution Guide

Read this playbook that outlines some of the best practices for [designing goose extensions/MCPs](https://docs.google.com/document/d/1uDTEmVZ6FlNTncPRksTpCuiab5L-GKrpILay_Sq0lR4/).

### Getting Started

1. Initialize & activate your local Python environment:
   ```bash
   uv sync 
   source .venv/bin/activate
   ```

2. Start your server via the terminal by running: `mcp_oracle_scm`. It will appear to 'hang' (no logs), but your server is indeed running (on port 3000).

### Test with MCP Inspector

The following command will start your server as a subprocess and also start up Anthropic's [Inspector](https://modelcontextprotocol.io/docs/tools/inspector) tool in the browser for testing your MCP server.

```bash
uv run mcp dev src/mcp_oracle_scm/server.py
```

Open your browser to http://localhost:6274 to see the MCP Inspector UI.


### Integrating with Goose

In Goose:
1. Go to **Settings > Extensions > Add**.
2. Set **Type** to **StandardIO**.
3. Enter the absolute path to this project's CLI in your environment, for example:
   ```bash
   uv run /path/to/mcp_oracle_scm/.venv/bin/mcp_oracle_scm
   ```
4. Enable the extension and confirm that Goose identifies your tools.
