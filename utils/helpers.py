"""
Utility functions for the animal scraper application.
"""

import logging
import time
import re
from pathlib import Path
from typing import Optional, List
from urllib.parse import urlparse

from config import LOG_LEVEL, LOG_FORMAT


def setup_logging() -> logging.Logger:
    """
    Set up logging configuration for the application.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format=LOG_FORMAT,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('../animal_scraper.log')
        ]
    )
    return logging.getLogger(__name__)


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a string to be used as a filename.
    
    Args:
        filename (str): Original filename string
        
    Returns:
        str: Sanitized filename safe for filesystem
    """
    # Remove or replace problematic characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove extra spaces and convert to lowercase
    sanitized = re.sub(r'\s+', '_', sanitized.strip().lower())
    # Remove leading/trailing underscores and limit length
    sanitized = sanitized.strip('_')[:50]
    return sanitized if sanitized else 'unknown_animal'


def get_file_extension_from_url(url: str) -> str:
    """
    Extract file extension from URL.
    
    Args:
        url (str): URL to extract extension from
        
    Returns:
        str: File extension including the dot, or .jpg as default
    """
    parsed_url = urlparse(url)
    path = Path(parsed_url.path)
    extension = path.suffix.lower()
    
    # Default to .jpg if no extension or unsupported extension
    from config import SUPPORTED_IMAGE_EXTENSIONS
    if not extension or extension not in SUPPORTED_IMAGE_EXTENSIONS:
        return '.jpg'
    
    return extension


def create_local_image_path(animal_name: str, image_url: str) -> Path:
    """
    Create a local file path for an animal image.
    
    Args:
        animal_name (str): Name of the animal
        image_url (str): URL of the image
        
    Returns:
        Path: Local file path for the image
    """
    from config import IMAGE_DOWNLOAD_DIR
    
    sanitized_name = sanitize_filename(animal_name)
    extension = get_file_extension_from_url(image_url)
    filename = f"{sanitized_name}_image{extension}"
    
    return IMAGE_DOWNLOAD_DIR / filename


def retry_with_backoff(func, max_retries: int = 3, initial_delay: float = 1.0):
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        func: Function to retry
        max_retries (int): Maximum number of retry attempts
        initial_delay (float): Initial delay in seconds
        
    Returns:
        Result of the function or raises the last exception
    """
    def wrapper(*args, **kwargs):
        last_exception = None
        delay = initial_delay
        
        for attempt in range(max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < max_retries:
                    logger = logging.getLogger(__name__)
                    logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    logger.error(f"All {max_retries + 1} attempts failed. Last error: {e}")
                    
        raise last_exception
    
    return wrapper


def normalize_text(text: str) -> str:
    """
    Normalize text by removing extra whitespace and cleaning up formatting.
    
    Args:
        text (str): Text to normalize
        
    Returns:
        str: Normalized text
    """
    if not text:
        return ""
    
    # Remove extra whitespace and newlines
    normalized = re.sub(r'\s+', ' ', text.strip())
    
    # Remove common Wikipedia formatting artifacts
    normalized = re.sub(r'\[\d+\]', '', normalized)  # Remove reference numbers
    normalized = re.sub(r'\([^)]*edit[^)]*\)', '', normalized, flags=re.IGNORECASE)  # Remove edit links
    
    return normalized.strip()


def is_valid_animal_name(name: str) -> bool:
    """
    Check if a string appears to be a valid animal name.
    
    Args:
        name (str): Name to validate
        
    Returns:
        bool: True if the name appears valid
    """
    if not name or len(name) < 2:
        return False
    
    # Filter out common non-animal entries
    invalid_terms = [
        'see also', 'references', 'external links', 'notes',
        'male', 'female', 'young', 'group', 'adjective'
    ]
    
    name_lower = name.lower()
    return not any(term in name_lower for term in invalid_terms)


def extract_animal_info_from_row(row_cells: List[str]) -> Optional[dict]:
    """
    Extract animal information from a table row.
    
    Args:
        row_cells (List[str]): List of cell contents from a table row
        
    Returns:
        Optional[dict]: Dictionary with animal info or None if invalid
    """
    if len(row_cells) < 2:
        return None
    
    animal_name = normalize_text(row_cells[0])
    
    # The collateral adjective is typically in the second or third column
    collateral_adjective = ""
    for i in range(1, min(len(row_cells), 4)):  # Check up to 3 columns after animal name
        cell_content = normalize_text(row_cells[i])
        if cell_content and cell_content.lower() not in ['—', '-', 'n/a', 'none', '']:
            collateral_adjective = cell_content
            break
    
    if not animal_name or not collateral_adjective or not is_valid_animal_name(animal_name):
        return None
    
    return {
        'animal': animal_name,
        'adjective': collateral_adjective
    }
