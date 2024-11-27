import logging
import logging.config
from colorlog import ColoredFormatter


def setup_logging():
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {"format": "%(asctime)s - %(levelname)s - %(message)s"},
                "detailed": {
                    "format": "%(asctime)s - %(levelname)s - %(message)s - %(funcName)s - %(lineno)d"
                },
                "colored": {
                    "()": ColoredFormatter,
                    "format": "%(log_color)s%(asctime)s - %(levelname)s - %(message)s%(reset)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                    "log_colors": {
                        "DEBUG": "cyan",
                        "INFO": "green",
                        "WARNING": "yellow",
                        "ERROR": "red",
                        "CRITICAL": "bold_red",
                    },
                },
            },
            "handlers": {
                "file": {
                    "level": "DEBUG",
                    "class": "logging.FileHandler",
                    "filename": "app.log",
                    "formatter": "default",
                },
                "console": {
                    "level": "INFO",
                    "class": "logging.StreamHandler",
                    "formatter": "colored",
                },
            },
            "loggers": {
                "": {  # Logger root
                    "handlers": ["file", "console"],
                    "level": "DEBUG",
                    "propagate": True,
                }
            },
        }
    )
