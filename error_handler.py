# error_handler.py
"""
Centralized error handling and logging system for LoL Analytics Application.
Provides consistent error handling, logging, and recovery strategies.
"""

import logging
import traceback
import functools
from typing import Callable, Any, Optional
from datetime import datetime
import sqlite3
import os


class AppLogger:
    """Centralized logging configuration"""

    _loggers = {}

    @classmethod
    def get_logger(cls, name: str = "lol_analytics") -> logging.Logger:
        """Get or create a logger with the specified name"""
        if name in cls._loggers:
            return cls._loggers[name]

        logger = logging.getLogger(name)

        # Avoid duplicate handlers
        if logger.handlers:
            return logger

        logger.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # File handler
        try:
            log_dir = os.path.join(os.path.dirname(__file__), 'logs')
            os.makedirs(log_dir, exist_ok=True)

            file_handler = logging.FileHandler(
                os.path.join(log_dir, 'app.log'),
                encoding='utf-8'
            )
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Could not create file handler: {e}")

        cls._loggers[name] = logger
        return logger


# Global logger instance
logger = AppLogger.get_logger()


class DatabaseError(Exception):
    """Custom exception for database operations"""
    pass


class APIError(Exception):
    """Custom exception for API operations"""
    pass


class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass


class DatabaseContextManager:
    """Context manager for database operations with automatic rollback"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def __enter__(self):
        """Open database connection"""
        try:
            self.conn = sqlite3.connect(self.db_path, timeout=10.0)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            logger.debug(f"Database connection opened: {self.db_path}")
            return self.cursor
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise DatabaseError(f"Database connection failed: {e}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close database connection with proper cleanup"""
        if exc_type is not None:
            # An error occurred, rollback
            if self.conn:
                try:
                    self.conn.rollback()
                    logger.warning(f"Transaction rolled back due to error: {exc_val}")
                except sqlite3.Error as rollback_error:
                    logger.error(f"Rollback failed: {rollback_error}")
        else:
            # No error, commit
            if self.conn:
                try:
                    self.conn.commit()
                    logger.debug("Transaction committed successfully")
                except sqlite3.Error as commit_error:
                    logger.error(f"Commit failed: {commit_error}")
                    raise DatabaseError(f"Commit failed: {commit_error}")

        # Always close the connection
        if self.conn:
            try:
                self.conn.close()
                logger.debug("Database connection closed")
            except sqlite3.Error as close_error:
                logger.error(f"Failed to close connection: {close_error}")

        # Don't suppress exceptions
        return False


def handle_errors(
        error_message: str = "An error occurred",
        return_value: Any = None,
        log_level: str = "error"
):
    """
    Decorator for error handling with logging.

    Args:
        error_message: Custom error message to log
        return_value: Value to return on error
        log_level: Logging level (error, warning, critical)
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except sqlite3.Error as e:
                log_func = getattr(logger, log_level, logger.error)
                log_func(f"{error_message} in {func.__name__}: {e}")
                log_func(f"Traceback: {traceback.format_exc()}")
                return return_value
            except Exception as e:
                log_func = getattr(logger, log_level, logger.error)
                log_func(f"Unexpected error in {func.__name__}: {e}")
                log_func(f"Traceback: {traceback.format_exc()}")
                return return_value

        return wrapper

    return decorator


def safe_db_operation(
        db_path: str,
        operation: Callable,
        error_message: str = "Database operation failed",
        default_return: Any = None
):
    """
    Execute a database operation with comprehensive error handling.

    Args:
        db_path: Path to database
        operation: Function that takes cursor as argument
        error_message: Error message for logging
        default_return: Value to return on error

    Returns:
        Result of operation or default_return on error
    """
    try:
        with DatabaseContextManager(db_path) as cursor:
            result = operation(cursor)
            logger.debug(f"Database operation completed successfully: {operation.__name__}")
            return result
    except DatabaseError as e:
        logger.error(f"{error_message}: {e}")
        return default_return
    except Exception as e:
        logger.error(f"Unexpected error during database operation: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return default_return


class RetryStrategy:
    """Retry strategy for transient failures"""

    @staticmethod
    def retry_on_failure(
            max_retries: int = 3,
            delay: float = 1.0,
            backoff_multiplier: float = 2.0,
            exceptions: tuple = (Exception,)
    ):
        """
        Decorator to retry function on failure.

        Args:
            max_retries: Maximum number of retry attempts
            delay: Initial delay between retries (seconds)
            backoff_multiplier: Multiplier for exponential backoff
            exceptions: Tuple of exceptions to catch
        """

        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                current_delay = delay
                last_exception = None

                for attempt in range(max_retries + 1):
                    try:
                        return func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        if attempt < max_retries:
                            logger.warning(
                                f"Attempt {attempt + 1}/{max_retries + 1} failed for "
                                f"{func.__name__}: {e}. Retrying in {current_delay}s..."
                            )
                            import time
                            time.sleep(current_delay)
                            current_delay *= backoff_multiplier
                        else:
                            logger.error(
                                f"All {max_retries + 1} attempts failed for {func.__name__}: {e}"
                            )

                raise last_exception

            return wrapper

        return decorator


def validate_input(
        value: Any,
        field_name: str,
        allowed_values: Optional[set] = None,
        max_length: Optional[int] = None,
        pattern: Optional[str] = None
) -> bool:
    """
    Validate input against constraints.

    Args:
        value: Value to validate
        field_name: Name of field for error messages
        allowed_values: Set of allowed values
        max_length: Maximum length for strings
        pattern: Regex pattern for validation

    Returns:
        True if valid

    Raises:
        ValidationError if invalid
    """
    if value is None:
        raise ValidationError(f"{field_name} cannot be None")

    if allowed_values and value not in allowed_values:
        raise ValidationError(
            f"{field_name} must be one of {allowed_values}, got: {value}"
        )

    if max_length and isinstance(value, str) and len(value) > max_length:
        raise ValidationError(
            f"{field_name} exceeds maximum length of {max_length}"
        )

    if pattern and isinstance(value, str):
        import re
        if not re.match(pattern, value):
            raise ValidationError(
                f"{field_name} does not match required pattern: {pattern}"
            )

    return True


def log_performance(func: Callable) -> Callable:
    """Decorator to log function execution time"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        if duration > 1.0:  # Log if takes more than 1 second
            logger.info(
                f"{func.__name__} completed in {duration:.2f}s"
            )

        return result

    return wrapper


# Example usage functions
@handle_errors(error_message="Failed to fetch data", return_value=[])
def example_safe_function():
    """Example of using error handling decorator"""
    # Your code here
    pass


def example_db_operation():
    """Example of using database context manager"""
    from config import DATABASE_PATH

    def insert_data(cursor):
        cursor.execute(
            "INSERT INTO example_table (column1, column2) VALUES (?, ?)",
            ("value1", "value2")
        )
        return cursor.lastrowid

    result = safe_db_operation(
        DATABASE_PATH,
        insert_data,
        error_message="Failed to insert data",
        default_return=None
    )

    return result


if __name__ == "__main__":
    # Test logging configuration
    logger.info("Logger initialized successfully")
    logger.debug("Debug message")
    logger.warning("Warning message")
    logger.error("Error message")

    # Test database context manager
    try:
        from config import DATABASE_PATH

        with DatabaseContextManager(DATABASE_PATH) as cursor:
            cursor.execute("SELECT 1")
            logger.info("Database context manager test successful")
    except Exception as e:
        logger.error(f"Database context manager test failed: {e}")