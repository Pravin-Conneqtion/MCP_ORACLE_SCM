# __init__.py

import argparse
from .server import mcp


def main():
    """
    Entry point for the MCP server.
    Parses any CLI arguments and starts the server.
    """
    parser = argparse.ArgumentParser(description="mcp_oracle_scm")
    parser.parse_args()
    mcp.run()

if __name__ == "__main__":
    main()
