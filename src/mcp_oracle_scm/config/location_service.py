"""Oracle Fusion Configuration Migration - Location Service"""

import aiohttp
import asyncio
import json
import csv
import os
from datetime import datetime
from typing import Dict, Any, List
from mcp_oracle_scm.common.auth import OracleAuth
from mcp_oracle_scm.config.environment import get_env_config
from mcp_oracle_scm.config.logger_config import LoggerConfig as Logger

# Load environment configuration
config = get_env_config()

# ✅ Base URL is at top level
FUSION_BASE_URL = config["base_url"]

# HCM Locations REST endpoint
FUSION_LOCATIONS_API = "/hcmRestApi/resources/11.13.18.05/locationsV2"
DEFAULT_PAGE_SIZE = 500  # number of records per call

# Output directory
OUTPUT_DIR = os.path.join(os.getcwd(), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


class OracleLocationManager:
    """Handles fetching all location data from Oracle Fusion HCM."""

    def __init__(self, config):
        self.base_url = config["base_url"]
        # ✅ FIX: OracleAuth() doesn’t take arguments
        self.auth =  OracleAuth()
        Logger.log(
            "Initialized OracleLocationManager",
            level="INFO",
            env=config.get("env"),
            base_url=self.base_url
        )

    async def _get_auth_header(self) -> Dict[str, str]:
        """Retrieve OAuth header for Fusion API calls."""
        access_token = self.auth.get_connection()
        if not access_token:
            raise Exception("Failed to get OAuth access token")
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

    async def fetch_all_locations(self) -> Dict[str, Any]:
        """Fetch all Fusion Locations and save them as JSON and CSV."""
        start_time = datetime.now()
        headers = await self._get_auth_header()

        all_locations: List[Dict[str, Any]] = []
        limit = DEFAULT_PAGE_SIZE
        offset = 0
        page = 1
        more_pages = True

        Logger.log("Starting Fusion Locations fetch", level="INFO")

        async with aiohttp.ClientSession() as session:
            while more_pages:
                url = f"{self.base_url}{FUSION_LOCATIONS_API}?onlyData=true&expand=all&limit={limit}&offset={offset}"
                Logger.log("Fetching Fusion Locations Page",
                           level="INFO", page=page, url=url)

                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        text = await response.text()
                        Logger.log("Fusion Locations API failed",
                                   level="ERROR",
                                   status=response.status,
                                   response=text)
                        raise Exception(f"Fusion API failed: {text}")

                    data = await response.json()
                    items = data.get("items", [])
                    all_locations.extend(items)

                    Logger.log("Fetched Page",
                               level="INFO",
                               page=page,
                               count=len(items))

                    if len(items) < limit:
                        more_pages = False
                    else:
                        offset += limit
                        page += 1

        # Save JSON
        json_path = os.path.join(OUTPUT_DIR, "all_locations.json")
        with open(json_path, "w", encoding="utf-8") as f_json:
            json.dump(all_locations, f_json, indent=2)

        # Save CSV
        csv_path = os.path.join(OUTPUT_DIR, "all_locations.csv")
        if all_locations:
            keys = sorted(all_locations[0].keys())
            with open(csv_path, "w", newline="", encoding="utf-8") as f_csv:
                writer = csv.DictWriter(f_csv, fieldnames=keys)
                writer.writeheader()
                writer.writerows(all_locations)

        elapsed = (datetime.now() - start_time).total_seconds()
        Logger.log("Fusion Locations Fetch Complete",
                   level="INFO",
                   total_records=len(all_locations),
                   time=f"{elapsed:.2f}s",
                   json_file=json_path,
                   csv_file=csv_path)

        return {
            "message": "Fusion Locations fetched successfully",
            "total_records": len(all_locations),
            "json_file": json_path,
            "csv_file": csv_path,
            "execution_time_seconds": elapsed
        }

    async def migrate_locations_to_target_instance(self, target_env: str = "DEV1") -> Dict[str, Any]:
        """
        Migrate all locations from source JSON (DEV1) to target instance (e.g., TEST).
        Reads JSON from output/all_locations.json and posts each location to target.
        """
        import json, aiohttp, os
        from datetime import datetime
        from mcp_oracle_scm.config.environment import ORACLE_CONFIGS

        start_time = datetime.now()
        json_path = os.path.join(OUTPUT_DIR, "all_locations.json")

        if not os.path.exists(json_path):
            raise FileNotFoundError("Source JSON file not found. Run fetch_all_locations() first.")

        with open(json_path, "r", encoding="utf-8") as f:
            locations = json.load(f)

        if not locations:
            raise ValueError("No locations found in JSON file.")

        Logger.log(f"Starting migration to target instance: {target_env}", level="INFO")

        # Validate and get target environment configuration
        if target_env not in ORACLE_CONFIGS:
            raise ValueError(f"Invalid target environment: {target_env}")

        target_config = ORACLE_CONFIGS[target_env]
        target_base_url = target_config["base_url"]

        target_auth = OracleAuth(target_env)
        headers = {
            "Authorization": f"Bearer {target_auth.get_connection()}",
            "Content-Type": "application/json"
        }

        success_count = 0
        fail_count = 0
        failed_records = []

        async with aiohttp.ClientSession() as session:
            for i, loc in enumerate(locations, start=1):
                try:
                    # Extract first address safely
                    address = (loc.get("addresses") or [{}])[0]
                    
                    payload = {
                        "LocationCode": loc.get("LocationCode"),
                        "LocationName": loc.get("LocationName"),
                        "ActiveStatus": loc.get("ActiveStatus", "A"),
                        "SetId": loc.get("SetId", 0),
                        "SetCode": loc.get("SetCode", "COMMON"),
                        "Description": loc.get("Description", ""),
                        "InventoryOrganizationId": loc.get("InventoryOrganizationId", 0),
                        "InventoryOrganizationName": loc.get("InventoryOrganizationName", ""),
                        "OfficialLanguageCode": loc.get("OfficialLanguageCode", "US"),
                        "EmailAddress": loc.get("EmailAddress", ""),
                        "ShipToSiteFlag": loc.get("ShipToSiteFlag", False),
                        "BillToSiteFlag": loc.get("BillToSiteFlag", False),
                        "ReceivingSiteFlag": loc.get("ReceivingSiteFlag", False),
                        "GeoHierarchyNodeCode": loc.get("GeoHierarchyNodeCode", ""),
                        "MainPhoneCountryCode": loc.get("MainPhoneCountryCode", ""),
                        "MainPhoneAreaCode": loc.get("MainPhoneAreaCode", ""),
                        "MainPhoneNumber": loc.get("MainPhoneNumber", ""),
                        "FaxCountryCode": loc.get("FaxCountryCode", ""),
                        "FaxAreaCode": loc.get("FaxAreaCode", ""),
                        "FaxNumber": loc.get("FaxNumber", ""),
                        "OfficeSiteFlag": loc.get("OfficeSiteFlag", False),
                        "BillToSiteFlag": loc.get("BillToSiteFlag", False),
                        "ReceivingSiteFlag": loc.get("ReceivingSiteFlag", False),
                        "addresses": [
                            {
                                "AddressUsageType": "MAIN",
                                "AddressLine1": address.get("AddressLine1", ""),
                                "AddressLine2": address.get("AddressLine2", ""),
                                "AddressLine3": address.get("AddressLine3", ""),
                                "AddressLine4": address.get("AddressLine4", ""),
                                "TownOrCity": address.get("TownOrCity", ""),
                                "Region1": address.get("Region1", ""),
                                "Region2": address.get("Region2", ""),
                                "Region3": address.get("Region3", ""),
                                "Country": address.get("Country", ""),
                                "PostalCode": address.get("PostalCode", "")
                            }
                        ]
                    }

                    post_url = f"{target_base_url}/hcmRestApi/resources/11.13.18.05/locationsV2"

                    async with session.post(post_url, headers=headers, json=payload) as response:
                        if response.status in [200, 201]:
                            success_count += 1
                            Logger.log(f"Created location {i}/{len(locations)}: {payload['LocationCode']}",
                                       level="INFO", status=response.status)
                        else:
                            text = await response.text()
                            fail_count += 1
                            failed_records.append({
                                "LocationCode": payload["LocationCode"],
                                "Status": response.status,
                                "Error": text,
                                "Payload": payload  # ✅ added full payload for debugging
                            })
                            Logger.log(f"Failed to create location {payload['LocationCode']}",
                                       level="ERROR", status=response.status, error=text, payload=payload)

                except Exception as e:
                    fail_count += 1
                    failed_records.append({
                        "LocationCode": loc.get("LocationCode", "UNKNOWN"),
                        "Error": str(e),
                        "Payload": loc  # include source data for failed record
                    })
                    Logger.log("Exception during location POST",
                               level="ERROR", error=str(e), payload=loc)

        elapsed = (datetime.now() - start_time).total_seconds()
        Logger.log("Migration complete",
                   level="INFO",
                   total_success=success_count,
                   total_failed=fail_count,
                   total_time=f"{elapsed:.2f}s")

        failed_path = os.path.join(OUTPUT_DIR, "failed_locations.json")
        if failed_records:
            with open(failed_path, "w", encoding="utf-8") as f:
                json.dump(failed_records, f, indent=2)

        return {
            "message": "Migration completed",
            "target_env": target_env,
            "total_records": len(locations),
            "successful": success_count,
            "failed": fail_count,
            "failed_records_file": failed_path if failed_records else None,
            "execution_time_seconds": elapsed
        }


def get_location_service() -> OracleLocationManager:
    """Return an instance of OracleLocationManager"""
    env_config = get_env_config()
    return OracleLocationManager(env_config)
