import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import inspect

@dataclass
class LoggerConfig:
    """
    Singleton logger configuration that automatically handles debug settings,
    log levels, and service context.
    """
    
    # Environment variable names
    ENV_DEBUG_ENABLED = "MCP_DEBUG_ENABLED"
    ENV_DEBUG_LEVEL = "MCP_DEBUG_LEVEL"
    ENV_DEBUG_LOCATION = "MCP_DEBUG_LOCATION"

    # Default values
    DEFAULT_ENABLED = "No"
    DEFAULT_LEVEL = "ERROR"
    DEFAULT_LOCATION = str(Path.home() / "mcp_logs")

    # Valid log levels
    VALID_LOG_LEVELS = {
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL"
    }

    # Levels that should always be logged
    ALWAYS_LOG_LEVELS = {
        "WARNING",
        "ERROR",
        "CRITICAL"
    }

    # Singleton instance
    _instance = None
    _logger = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LoggerConfig, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize the logger configuration"""
        self.debug_enabled = os.getenv(self.ENV_DEBUG_ENABLED, self.DEFAULT_ENABLED).upper() == "YES"
        self.debug_level = os.getenv(self.ENV_DEBUG_LEVEL, self.DEFAULT_LEVEL).upper()
        if self.debug_level not in self.VALID_LOG_LEVELS:
            self.debug_level = self.DEFAULT_LEVEL
            
        self.debug_location = os.getenv(self.ENV_DEBUG_LOCATION, self.DEFAULT_LOCATION)

        # Ensure log directory exists
        log_dir = Path(self.debug_location).expanduser()
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Warning: Could not create log directory {self.debug_location}: {e}")
            log_dir = Path(self.DEFAULT_LOCATION).expanduser()
            log_dir.mkdir(parents=True, exist_ok=True)

        # Initialize logging if enabled
        if self.debug_enabled:
            log_file = log_dir / f"mcp_oracle_scm_{datetime.now().strftime('%Y%m%d')}.log"
            logging.basicConfig(
                filename=str(log_file),
                level=getattr(logging, self.debug_level, logging.ERROR),
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                force=True  # Ensure we can reinitialize if needed
            )
            self._logger = logging.getLogger('mcp_oracle_scm')

    @staticmethod
    def _get_caller_service() -> str:
        """
        Get the name of the service calling the log method.
        Walks up the call stack to find the service module name.
        """
        frame = inspect.currentframe()
        try:
            while frame:
                module_name = frame.f_globals.get('__name__', '')
                if 'service' in module_name.lower():
                    # Extract service name from the module path
                    parts = module_name.split('.')
                    for part in parts:
                        if 'service' in part.lower():
                            return part.replace('_service', '').replace('Service', '')
                frame = frame.f_back
            return 'unknown'
        finally:
            del frame  # Avoid reference cycles

    @classmethod
    def log(cls, message: str, level: Optional[str] = None, **kwargs) -> None:
        """
        Log a message with automatic service detection and environment-based configuration.
        WARNING, ERROR, and CRITICAL messages are always logged regardless of debug level.
        Other messages are logged only if they match the environment's debug level.
        
        Args:
            message: The message to log
            level: The specific log level for this message (optional)
            **kwargs: Additional context to include in the log message
        """
        instance = cls()
        if not instance.debug_enabled or not instance._logger:
            return

        # Process the log level
        message_level = level.upper() if level else instance.debug_level
        if message_level not in cls.VALID_LOG_LEVELS:
            message_level = instance.debug_level

        # Determine if we should log this message
        should_log = (message_level in cls.ALWAYS_LOG_LEVELS or 
                     message_level == instance.debug_level)
        
        if not should_log:
            return

        # Get the service name and format the message
        service_name = cls._get_caller_service()
        log_parts = [f"[{service_name}]", message]
        
        if kwargs:
            context = ' '.join(f"{k}={v}" for k, v in kwargs.items())
            log_parts.append(f"- {context}")

        final_message = ' '.join(log_parts)
        log_level = getattr(logging, message_level)
        instance._logger.log(log_level, final_message)

    @classmethod
    def get_current_settings(cls) -> Dict[str, str]:
        """Get current logger settings for debugging"""
        instance = cls()
        return {
            'debug_enabled': str(instance.debug_enabled),
            'debug_level': instance.debug_level,
            'debug_location': instance.debug_location
        }