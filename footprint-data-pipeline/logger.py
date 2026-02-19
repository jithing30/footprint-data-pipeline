# src/logger.py

import logging
import os
from datetime import datetime


class AppLogger:
    """
    Centralized application logger
    """

    def __init__(
            self,
            name: str = "app_logger",
            log_level: str = "INFO",
            log_dir: str = "logs"
    ):
        self.name = name
        self.log_level = log_level.upper()
        self.log_dir = log_dir

        self._logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.name)

        # Prevent duplicate handlers
        if logger.handlers:
            return logger

        logger.setLevel(self.log_level)

        # Create log directory if missing
        os.makedirs(self.log_dir, exist_ok=True)

        # File name with timestamp
        log_file = os.path.join(
            self.log_dir,
            f"{self.name}_{datetime.now().strftime('%Y%m%d')}.log"
        )

        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s"
        )

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        # Attach handlers
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        return logger

    # Proxy methods
    def debug(self, msg: str):
        self._logger.debug(msg)

    def info(self, msg: str):
        self._logger.info(msg)

    def warning(self, msg: str):
        self._logger.warning(msg)

    def error(self, msg: str, exc_info=False):
        self._logger.error(msg, exc_info=exc_info)

    def critical(self, msg: str):
        self._logger.critical(msg)
