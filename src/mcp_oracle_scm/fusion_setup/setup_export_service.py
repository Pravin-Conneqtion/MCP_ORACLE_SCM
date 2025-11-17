import aiohttp
import asyncio
import base64
import os
from typing import Dict, Any
from mcp_oracle_scm.config.environment import get_env_config
from mcp_oracle_scm.config.logger_config import LoggerConfig as Logger
from mcp_oracle_scm.common.auth import OracleAuth


class SetupTaskCSVExportService:
    def __init__(self, env: str):
        self.env = env
        self.config = get_env_config()
        self.base_url = self.config["base_url"]
        self.timeout = aiohttp.ClientTimeout(total=600)

        self.output_dir = "./output/setup_exports"
        os.makedirs(self.output_dir, exist_ok=True)

    async def _get_headers(self):
        """Generate authenticated headers."""
        auth = OracleAuth()
        token = auth.get_connection()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    # ------------------------------------------------------
    # STEP 1: START EXPORT PROCESS
    # ------------------------------------------------------
    async def start_export(self, task_code: str) -> Dict[str, Any]:
        url = f"{self.base_url}/fscmRestApi/resources/11.13.18.05/setupTaskCSVExports"

        payload = {
            "TaskCode": task_code,
            "SetupTaskCSVExportProcess": [{"TaskCode": task_code}]
        }

        headers = await self._get_headers()

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                body = await resp.json()
                Logger.log("Start Export Response", level="INFO", body=body)

                try:
                    process_id = body["SetupTaskCSVExportProcess"][0]["ProcessId"]
                    return {"success": True, "process_id": process_id, "response": body}
                except KeyError:
                    return {"success": False, "error": body}

    # ------------------------------------------------------
    # STEP 2: CHECK EXPORT STATUS (POLL)
    # ------------------------------------------------------
    async def check_export_status(self, task_code: str, process_id: int) -> Dict[str, Any]:
        url = (
            f"{self.base_url}/fscmRestApi/resources/11.13.18.05/"
            f"setupTaskCSVExports/{task_code}/child/SetupTaskCSVExportProcess/{process_id}"
        )

        headers = await self._get_headers()

        for _ in range(60):  # ~10 minutes max
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(url, headers=headers) as resp:
                    body = await resp.json()

                    completed = body.get("ProcessCompletedFlag")
                    Logger.log(
                        "Export Status Check",
                        level="INFO",
                        process_id=process_id,
                        completed=completed,
                    )

                    if completed:
                        return {"completed": True, "response": body}

            await asyncio.sleep(10)

        return {"completed": False, "error": "Export timeout"}

    # ------------------------------------------------------
    # STEP 3: DOWNLOAD BASE64 ZIP FILE
    # ------------------------------------------------------
    async def download_export_file(self, task_code: str, process_id: int) -> Dict[str, Any]:
        """
        FIXED VERSION:
        Oracle returns binary ZIP (octet-stream), NOT Base64.
        We save raw bytes directly.
        """
        url = (
            f"{self.base_url}/fscmRestApi/resources/11.13.18.05/"
            f"setupTaskCSVExports/{task_code}/child/SetupTaskCSVExportProcess/{process_id}"
            f"/child/SetupTaskCSVExportProcessResult/{process_id}/enclosure/FileContent"
        )

        headers = await self._get_headers()

        Logger.log("Downloading exported file (RAW ZIP)", url=url)

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url, headers=headers) as resp:

                if resp.status != 200:
                    text = await resp.text()
                    return {
                        "success": False,
                        "error": f"HTTP {resp.status}: {text}"
                    }

                # 🔥 ---- KEY CHANGE ----
                # Read raw bytes (ZIP file)
                zip_bytes = await resp.read()

                # Save as ZIP file
                filepath = os.path.join(self.output_dir, f"{task_code}_{process_id}.zip")

                with open(filepath, "wb") as f:
                    f.write(zip_bytes)

                Logger.log("ZIP file saved", level="INFO", path=filepath)

                return {
                    "success": True,
                    "file_path": filepath
                }

    # ------------------------------------------------------
    # MASTER EXPORT PIPELINE
    # ------------------------------------------------------
    async def export_setup_task(self, task_code: str) -> Dict[str, Any]:
        Logger.log("Starting Export Pipeline", task_code=task_code)

        start = await self.start_export(task_code)
        if not start["success"]:
            return start

        process_id = start["process_id"]

        status = await self.check_export_status(task_code, process_id)
        if not status["completed"]:
            return status

        download = await self.download_export_file(task_code, process_id)

        return {
            "task_code": task_code,
            "process_id": process_id,
            "export_completed": True,
            "download": download,
        }


# Factory method for MCP usage
def get_setup_export_service(env: str):
    return SetupTaskCSVExportService(env)
