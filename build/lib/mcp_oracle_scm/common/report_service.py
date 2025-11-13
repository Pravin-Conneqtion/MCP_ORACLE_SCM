"""Oracle Report Service Module"""

import base64
import xml.etree.ElementTree as ET
from typing import Dict, Any, Optional, List, Tuple
import aiohttp
import csv
import io
import os
from datetime import datetime
import uuid
from pathlib import Path
from mcp_oracle_scm.common.auth import OracleAuth
from mcp_oracle_scm.config.environment import get_env_config
from mcp_oracle_scm.config.logger_config import LoggerConfig as Logger

class OracleReportService:
    """Oracle Report Service for executing and downloading BI reports."""
    
    def __init__(self):
        """Initialize using environment configuration."""
        config = get_env_config()
        self.base_url = config['base_url']
        self.auth = OracleAuth()
        self.base_url = self.base_url.rstrip('/')
        self.wsdl_url = f"{self.base_url}/xmlpserver/services/PublicReportWSSService?wsdl"
        self.soap_url = f"{self.base_url}/xmlpserver/services/PublicReportWSSService"
        
        # Set downloads directory
        self.downloads_dir = os.path.expanduser("~/Downloads")
        
        Logger.log("Initialized Oracle Report Service",
                  level="INFO",
                  base_url=self.base_url)
        Logger.log("Reports download location set",
                  level="INFO",
                  downloads_dir=self.downloads_dir)

    def _generate_output_filename(self, report_path: str) -> str:
        """Generate a unique filename for the report output."""
        # Extract report name from path
        report_name = report_path.split('/')[-1].replace('.xdo', '')
        # Generate timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # Generate unique identifier
        unique_id = str(uuid.uuid4())[:8]
        # Combine components
        filename = f"{report_name}_{timestamp}_{unique_id}.csv"
        return os.path.join(self.downloads_dir, filename)

    async def _make_soap_request(self, soap_body: str) -> str:
        """Make SOAP request to Oracle Report Service using OAuth."""
        # Get OAuth token
        access_token = self.auth.get_connection()
        if not access_token:
            raise Exception("Failed to get OAuth access token")

        headers = {
            'Content-Type': 'application/soap+xml;charset=UTF-8',
            'Authorization': f'Bearer {access_token}'
        }

        Logger.log("Making SOAP request",
                  level="INFO",
                  url=self.soap_url)
        Logger.log("Request details",
                  level="DEBUG",
                  body=soap_body)
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.soap_url, data=soap_body, headers=headers) as response:
                    response_text = await response.text()
                    Logger.log("Response received",
                             level="DEBUG",
                             status=response.status,
                             body=response_text)
                    
                    if response.status != 200:
                        Logger.log("SOAP request failed",
                                 level="ERROR",
                                 status=response.status,
                                 response=response_text)
                        raise Exception(f"SOAP request failed with status {response.status}: {response_text}")
                    return response_text
        except Exception as e:
            Logger.log("SOAP request error",
                      level="ERROR",
                      error=str(e))
            raise

    def _create_run_report_envelope(self, report_path: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        """Create SOAP envelope for runReport request."""
        # Create parameters XML if parameters provided
        params_xml = ""
        if parameters:
            for name, value in parameters.items():
                params_xml += f"""
                <pub:item>
                    <pub:name>{name}</pub:name>
                    <pub:values>
                        <pub:item>{value}</pub:item>
                    </pub:values>
                </pub:item>"""

        return f"""<?xml version="1.0" encoding="UTF-8"?>
        <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" 
                      xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService">
            <soap:Header/>
            <soap:Body>
                <pub:runReport>
                    <pub:reportRequest>
                        <pub:parameterNameValues>{params_xml}</pub:parameterNameValues>
                        <pub:reportAbsolutePath>{report_path}</pub:reportAbsolutePath>
                        <pub:sizeOfDataChunkDownload>1</pub:sizeOfDataChunkDownload>
                    </pub:reportRequest>
                </pub:runReport>
            </soap:Body>
        </soap:Envelope>"""

    def _create_download_chunk_envelope(self, file_id: str, begin_idx: int, chunk_size: int = 5000) -> str:
        """Create SOAP envelope for downloadReportDataChunk request."""
        return f"""<?xml version="1.0" encoding="UTF-8"?>
        <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" 
                      xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService">
            <soap:Header/>
            <soap:Body>
                <pub:downloadReportDataChunk>
                    <pub:fileID>{file_id}</pub:fileID>
                    <pub:beginIdx>{begin_idx}</pub:beginIdx>
                    <pub:size>{chunk_size}</pub:size>
                </pub:downloadReportDataChunk>
            </soap:Body>
        </soap:Envelope>"""

    def _parse_run_report_response(self, response_text: str) -> str:
        """Parse runReport response to get report file ID."""
        root = ET.fromstring(response_text)
        ns = {
            'env': 'http://www.w3.org/2003/05/soap-envelope',
            'ns2': 'http://xmlns.oracle.com/oxp/service/PublicReportService'
        }
        
        # Find reportFileID element
        file_id_elem = root.find('.//ns2:reportFileID', ns)
        if file_id_elem is not None:
            return file_id_elem.text
        raise Exception("Could not find reportFileID in response")

    def _parse_download_chunk_response(self, response_text: str) -> Tuple[str, int]:
        """Parse downloadReportDataChunk response to get chunk data and offset."""
        root = ET.fromstring(response_text)
        ns = {
            'env': 'http://www.w3.org/2003/05/soap-envelope',
            'ns2': 'http://xmlns.oracle.com/oxp/service/PublicReportService'
        }
        
        # Find data chunk and offset
        chunk_elem = root.find('.//ns2:reportDataChunk', ns)
        offset_elem = root.find('.//ns2:reportDataOffset', ns)
        
        if chunk_elem is not None and offset_elem is not None:
            return chunk_elem.text, int(offset_elem.text)
        raise Exception("Could not find reportDataChunk or reportDataOffset in response")

    async def get_report_data(self, report_path: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        """Run report and save data to a file.
        
        Args:
            report_path: Path to the report in Oracle BI
            parameters: Optional parameters for the report
            
        Returns:
            Path to the downloaded report file
        """
        try:
            Logger.log("Running report",
                      level="INFO",
                      report_path=report_path,
                      parameters=parameters)
            
            output_file = self._generate_output_filename(report_path)
            Logger.log("Report output location",
                      level="INFO",
                      output_file=output_file)
            
            # Step 1: Run report to get file ID
            run_report_envelope = self._create_run_report_envelope(report_path, parameters)
            response = await self._make_soap_request(run_report_envelope)
            file_id = self._parse_run_report_response(response)
            Logger.log("Report file ID received",
                      level="INFO",
                      file_id=file_id)
            
            # Step 2: Download report data in chunks and write to file
            begin_idx = 0
            chunk_size = 5000
            total_rows = 0
            headers_written = False
            
            # Initialize file for first chunk, then append subsequent chunks
            first_chunk = True
            while True:
                Logger.log("Downloading data chunk",
                          level="INFO",
                          start_index=begin_idx)
                
                download_envelope = self._create_download_chunk_envelope(file_id, begin_idx, chunk_size)
                response = await self._make_soap_request(download_envelope)
                chunk_data, offset = self._parse_download_chunk_response(response)
                
                if chunk_data:
                    # Decode base64 chunk
                    decoded_data = base64.b64decode(chunk_data).decode('utf-8')
                    
                    # Open in write mode for first chunk, append mode for subsequent chunks
                    mode = 'w' if first_chunk else 'a'
                    with open(output_file, mode, newline='', encoding='utf-8') as f:
                        f.write(decoded_data)
                    
                    # Count lines in this chunk (excluding empty lines)
                    chunk_lines = sum(1 for line in decoded_data.splitlines() if line.strip())
                    total_rows += chunk_lines
                    
                    Logger.log("Chunk processed",
                             level="INFO",
                             chunk_lines=chunk_lines,
                             total_rows=total_rows)
                    first_chunk = False
                
                if offset == -1:  # End of data
                    break
                    
                begin_idx += chunk_size
            
            Logger.log("Report download complete",
                      level="INFO",
                      total_rows=total_rows,
                      output_file=output_file)
            return output_file
            
        except Exception as e:
            Logger.log("Error getting report data",
                      level="ERROR",
                      error=str(e),
                      partial_file=output_file)
            raise
