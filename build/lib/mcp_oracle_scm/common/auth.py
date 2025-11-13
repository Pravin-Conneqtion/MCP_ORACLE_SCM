"""Oracle SCM Authentication Module"""

import os
from http.server import HTTPServer, BaseHTTPRequestHandler
import webbrowser
import requests
from urllib.parse import urlencode, parse_qs
import threading
import time
from typing import Optional, Dict, Tuple
import secrets
import hashlib
import base64
import string
import keyring
from mcp_oracle_scm.config.environment import get_env_config
from mcp_oracle_scm.config.logger_config import LoggerConfig as Logger

def generate_code_verifier() -> str:
    """Generate a code verifier for PKCE"""
    chars = string.ascii_letters + string.digits + "-._~"
    verifier = ''.join(secrets.choice(chars) for _ in range(128))
    Logger.log("Generated code verifier of length",
                      level="ERROR",
                      error_message=len(verifier))
   
    return verifier

def generate_code_challenge(verifier: str) -> str:
    """Generate a code challenge from the verifier using SHA256"""
    hash = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.b64encode(hash).decode().replace('+', '-').replace('/', '_').rstrip('=')
    Logger.log("Generated code challenge",
              level="DEBUG",
              challenge_length=len(challenge))
    return challenge

class OAuthCallbackHandler(BaseHTTPRequestHandler):
    auth_code = None
    
    def do_GET(self):
        """Handle the OAuth callback"""
        Logger.log("Received callback",
                  level="DEBUG",
                  path=self.path)
        query_components = parse_qs(self.path.split('?')[1]) if '?' in self.path else {}
        
        if 'code' in query_components:
            OAuthCallbackHandler.auth_code = query_components['code'][0]
            Logger.log("Received authorization code",
                      level="INFO")
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"Authorization successful! You can close this window.")
        else:
            Logger.log("No authorization code received in callback",
                      level="ERROR")
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"Authorization failed! No code received.")
            
    def log_message(self, format, *args):
        """Suppress default logging"""
        pass

class OracleAuth:
    def __init__(self):
        """Initialize authentication using environment configuration"""
        config = get_env_config()
        self.auth_url = config['auth_url']
        self.token_url = config['token_url']
        self.client_id = config['client_id']
        self.scope = config['scope']
        self.redirect_uri = "http://127.0.0.1:3009/callback"
        self.access_token = None
        self.token_expiry = None
        self.refresh_token = None
        Logger.log("Initialized OracleAuth",
                  level="INFO",
                  env=config['env'],
                  client_id=self.client_id)
    
    def save_to_keychain(self, token: str, expiry_time: float, refresh_token: Optional[str] = None):
        """Save authentication data to keychain"""
        try:
            keyring.set_password("mcp_oracle", "oauth_token", token)
            keyring.set_password("mcp_oracle", "oauth_token_expiry", str(expiry_time))
            if refresh_token:
                keyring.set_password("mcp_oracle", "oauth_refresh_token", refresh_token)
            Logger.log("Successfully saved token data to keychain",
                      level="INFO")
        except Exception as e:
            Logger.log("Error saving to keychain",
                      level="ERROR",
                      error=str(e))
            
    def load_from_keychain(self) -> Tuple[Optional[str], Optional[float], Optional[str]]:
        """Load authentication data from keychain"""
        try:
            token = keyring.get_password("mcp_oracle", "oauth_token")
            expiry_str = keyring.get_password("mcp_oracle", "oauth_token_expiry")
            refresh_token = keyring.get_password("mcp_oracle", "oauth_refresh_token")
            
            expiry = float(expiry_str) if expiry_str else None
            
            return token, expiry, refresh_token
        except Exception as e:
            Logger.log("Error loading from keychain",
                      level="ERROR",
                      error=str(e))
            return None, None, None
            
    def clear_keychain(self):
        """Clear authentication data from keychain"""
        try:
            keyring.delete_password("mcp_oracle", "oauth_token")
            keyring.delete_password("mcp_oracle", "oauth_token_expiry")
            keyring.delete_password("mcp_oracle", "oauth_refresh_token")
            Logger.log("Successfully cleared keychain data",
                      level="INFO")
        except Exception as e:
            Logger.log("Error clearing keychain",
                      level="ERROR",
                      error=str(e))
        
    def start_auth_server(self) -> Tuple[str, str]:
        """Start local server to receive OAuth callback"""
        Logger.log("Starting local authentication server",
                  level="INFO")
        try:
            server = HTTPServer(('127.0.0.1', 3009), OAuthCallbackHandler)
            server_thread = threading.Thread(target=server.serve_forever)
            server_thread.daemon = True
            server_thread.start()
            Logger.log("Local server started successfully",
                      level="DEBUG")
            
            # Reset the auth code
            OAuthCallbackHandler.auth_code = None
            
            # Generate PKCE verifier and challenge
            code_verifier = generate_code_verifier()
            code_challenge = generate_code_challenge(code_verifier)
            
            # Build authorization URL with PKCE
            auth_params = {
                'client_id': self.client_id,
                'response_type': 'code',
                'redirect_uri': self.redirect_uri,
                'scope': self.scope,
                'code_challenge': code_challenge,
                'code_challenge_method': 'S256'
            }
            auth_url = f"{self.auth_url}?{urlencode(auth_params)}"
            Logger.log("Authorization URL generated",
                      level="DEBUG",
                      url=auth_url)
            
            # Open browser for authorization
            Logger.log("Opening browser for authorization",
                      level="INFO")
            webbrowser.open(auth_url)
            
            # Wait for callback
            timeout = 300  # 5 minutes timeout
            start_time = time.time()
            while OAuthCallbackHandler.auth_code is None:
                if time.time() - start_time > timeout:
                    server.shutdown()
                    Logger.log("Authorization timed out after 5 minutes",
                             level="ERROR")
                    raise TimeoutError("Authorization timed out after 5 minutes")
                time.sleep(1)
                
            auth_code = OAuthCallbackHandler.auth_code
            Logger.log("Successfully received authorization code",
                      level="INFO")
            server.shutdown()
            return auth_code, code_verifier
        except Exception as e:
            Logger.log("Error in start_auth_server",
                      level="ERROR",
                      error=str(e))
            raise
    
    def get_token(self, auth_code: str, code_verifier: str) -> Dict[str, str]:
        """Exchange authorization code for access token using PKCE"""
        Logger.log("Exchanging authorization code for access token",
                  level="INFO")
        token_params = {
            'grant_type': 'authorization_code',
            'code': auth_code,
            'redirect_uri': self.redirect_uri,
            'client_id': self.client_id,
            'code_verifier': code_verifier
        }
        
        try:
            Logger.log("Making token request",
                      level="DEBUG",
                      url=self.token_url)
            response = requests.post(self.token_url, data=token_params)
            response.raise_for_status()
            
            token_data = response.json()
            Logger.log("Successfully retrieved token data",
                      level="INFO")
            
            self.access_token = token_data['access_token']
            Logger.log("Access token received",
                      level="DEBUG",
                      token_length=len(self.access_token))
            
            # Store token expiry if provided
            if 'expires_in' in token_data:
                self.token_expiry = time.time() + token_data['expires_in']
                Logger.log("Token expiry set",
                          level="DEBUG",
                          expires_in=token_data['expires_in'])
                
            # Store refresh token if provided
            if 'refresh_token' in token_data:
                self.refresh_token = token_data['refresh_token']
                Logger.log("Refresh token received",
                          level="DEBUG")
                
            # Save to keychain
            self.save_to_keychain(
                self.access_token,
                self.token_expiry,
                self.refresh_token if 'refresh_token' in token_data else None
            )
                
            return token_data
        except requests.exceptions.RequestException as e:
            Logger.log("Error getting token",
                      level="ERROR",
                      error=str(e),
                      response_text=e.response.text if hasattr(e.response, 'text') else None)
            raise
    
    def refresh_access_token(self) -> Optional[str]:
        """Refresh the access token using the refresh token"""
        # Try to get refresh token from instance or keychain
        refresh_token = self.refresh_token
        if not refresh_token:
            _, _, refresh_token = self.load_from_keychain()
            
        if not refresh_token:
            Logger.log("No refresh token available",
                      level="WARNING")
            return None
            
        Logger.log("Attempting to refresh access token",
                  level="INFO")
        token_params = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': self.client_id
        }
        
        try:
            response = requests.post(self.token_url, data=token_params)
            response.raise_for_status()
            token_data = response.json()
            
            self.access_token = token_data['access_token']
            Logger.log("Successfully refreshed access token",
                      level="INFO")
            
            if 'expires_in' in token_data:
                self.token_expiry = time.time() + token_data['expires_in']
                Logger.log("New token expiry set",
                          level="DEBUG",
                          expires_in=token_data['expires_in'])
                
            # Update refresh token if provided
            if 'refresh_token' in token_data:
                self.refresh_token = token_data['refresh_token']
                Logger.log("New refresh token received",
                          level="DEBUG")
                
            # Save updated tokens to keychain
            self.save_to_keychain(
                self.access_token,
                self.token_expiry,
                self.refresh_token if 'refresh_token' in token_data else None
            )
                
            return self.access_token
        except Exception as e:
            Logger.log("Error refreshing token",
                      level="ERROR",
                      error=str(e))
            # If refresh fails, clear stored tokens
            self.access_token = None
            self.refresh_token = None
            self.token_expiry = None
            self.clear_keychain()
            return None
    
    def get_connection(self) -> Optional[str]:
        """Get a valid access token, requesting a new one if necessary"""
        Logger.log("Getting connection/access token",
                  level="INFO")
        
        # First try instance variables
        if self.access_token and self.token_expiry:
            if time.time() < self.token_expiry - 300:  # 5 minute buffer
                Logger.log("Using existing valid access token from instance",
                          level="INFO")
                return self.access_token
                
        # Then try keychain
        token, expiry, _ = self.load_from_keychain()
        if token and expiry:
            try:
                expiry_float = float(expiry)
                if time.time() < expiry_float - 300:  # 5 minute buffer
                    Logger.log("Using existing valid access token from keychain",
                             level="INFO")
                    self.access_token = token
                    self.token_expiry = expiry_float
                    return token
            except ValueError:
                Logger.log("Invalid expiry time format in keychain",
                          level="ERROR")
        
        # Try to refresh the token
        refreshed_token = self.refresh_access_token()
        if refreshed_token:
            Logger.log("Successfully refreshed access token",
                      level="INFO")
            return refreshed_token

        # Need new token through full auth flow
        Logger.log("Initiating full authentication flow",
                  level="INFO")
        try:
            auth_code, code_verifier = self.start_auth_server()
            token_data = self.get_token(auth_code, code_verifier)
            return token_data['access_token']
        except Exception as e:
            Logger.log("Error in authentication flow",
                      level="ERROR",
                      error=str(e))
            return None
            
    def close_connection(self):
        """Clean up any resources"""
        Logger.log("Closing connection and clearing tokens",
                  level="INFO")
        self.access_token = None
        self.token_expiry = None
        self.refresh_token = None
        self.clear_keychain()

# Initialize singleton instance
_oracle_auth = None

def get_oracle_auth() -> OracleAuth:
    """Get the singleton OracleAuth instance"""
    global _oracle_auth
    if _oracle_auth is None:
        _oracle_auth = OracleAuth()
    return _oracle_auth

async def get_oauth_headers() -> Dict[str, str]:
    """Get OAuth headers for API requests"""
    auth = get_oracle_auth()
    access_token = auth.get_connection()
    if not access_token:
        Logger.log("Failed to get access token",
                  level="ERROR")
        raise Exception("Failed to get access token")
    
    return {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }