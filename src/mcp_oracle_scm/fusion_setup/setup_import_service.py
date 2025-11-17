import aiohttp
import asyncio
import os
import json
import base64
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from mcp_oracle_scm.common.auth import OracleAuth
from mcp_oracle_scm.config.environment import ORACLE_CONFIGS
from mcp_oracle_scm.config.logger_config import LoggerConfig as Logger

OUTPUT_DIR = os.path.join(os.getcwd(), "output", "setup_imports")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# Utility
def _get_base_url_for_env(env: str) -> str:
    env_upper = env.upper()
    if env_upper not in ORACLE_CONFIGS:
        raise ValueError(f"Invalid environment '{env}'. Valid: {', '.join(ORACLE_CONFIGS.keys())}")
    return ORACLE_CONFIGS[env_upper]["base_url"]


class SetupTaskCSVImportService:
    """Service for importing setup task CSV files into Fusion."""

    def __init__(self, env: str = "DEV2"):
        self.env = env.upper()
        self.base_url = _get_base_url_for_env(self.env)
        self.auth = OracleAuth(self.env)
        Logger.log("Initialized SetupTaskCSVImportService", level="INFO", env=self.env, base_url=self.base_url)

    async def _get_headers(self) -> Dict[str, str]:
        token = self.auth.get_connection()   # SAME AUTH MECHANISM
        if not token:
            raise Exception("Failed to obtain OAuth access token")
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/vnd.oracle.adf.resourceitem+json"
        }

    async def start_import(self, task_code: str, base64_content: str):
        """
        POST /setupTaskCSVImports
        """
        url = f"{self.base_url}/fscmRestApi/resources/11.13.18.05/setupTaskCSVImports"
        payload = {
            "TaskCode": task_code,
            "SetupTaskCSVImportProcess": [
                {"TaskCode": task_code, "FileContent": base64_content}
            ]
        }

        headers = await self._get_headers()
        Logger.log("Starting setup CSV import", level="INFO", task_code=task_code)

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                text = await resp.text()

                if resp.status not in (200, 201):
                    Logger.log("Import start failed", level="ERROR", status=resp.status, response=text)
                    raise Exception(f"Start import failed -> {resp.status}: {text}")

                data = await resp.json()
                process_id = data["SetupTaskCSVImportProcess"][0]["ProcessId"]
                return {"process_id": process_id, "raw_response": data}

    async def poll_import_status(self, task_code: str, process_id: int):
        """
        GET until ProcessCompletedFlag=True
        """
        url = (
            f"{self.base_url}/fscmRestApi/resources/11.13.18.05/"
            f"setupTaskCSVImports/{task_code}/child/SetupTaskCSVImportProcess/{process_id}"
        )

        headers = await self._get_headers()

        Logger.log("Polling import status", level="INFO", url=url)

        async with aiohttp.ClientSession() as session:
            while True:
                async with session.get(url, headers=headers) as resp:
                    data = await resp.json()
                    completed = data.get("ProcessCompletedFlag")

                    completed_bool = (
                        completed.lower() == "true"
                        if isinstance(completed, str)
                        else bool(completed)
                    )

                    if completed_bool:
                        Logger.log("Import completed", level="INFO", process_id=process_id)
                        return data

                    await asyncio.sleep(5)

    async def download_process_log(self, task_code: str, process_id: int):
        """
        GET ProcessLog (plain text)
        """
        url = (
            f"{self.base_url}/fscmRestApi/resources/11.13.18.05/"
            f"setupTaskCSVImports/{task_code}/child/SetupTaskCSVImportProcess/{process_id}"
            f"/child/SetupTaskCSVImportProcessResult/{process_id}/enclosure/ProcessLog"
        )

        headers = await self._get_headers()

        Logger.log("Downloading import process log", level="INFO", url=url)

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                text = await resp.text()

                filename = f"{task_code}_{process_id}_import_log.txt"
                path = os.path.join(OUTPUT_DIR, filename)

                with open(path, "w", encoding="utf-8") as f:
                    f.write(text)

                return {"process_log_path": path, "log_text": text}

    async def run_import(self, task_code: str, file_path: str):
        """
        Import orchestration:
        1. read ZIP → base64 encode
        2. POST → start import
        3. poll status
        4. download import log
        """
        Logger.log("Running full import", level="INFO", file=file_path)

        with open(file_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")

        start_info = await self.start_import(task_code, encoded)
        process_id = start_info["process_id"]

        status_info = await self.poll_import_status(task_code, process_id)
        log_info = await self.download_process_log(task_code, process_id)

        return {
            "task_code": task_code,
            "process_id": process_id,
            "status": status_info,
            "log": log_info
        }
