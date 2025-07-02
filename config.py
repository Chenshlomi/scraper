"""
Configuration module for the animal scraper application.
Contains all configurable parameters and constants.
"""

from pathlib import Path

# Wikipedia URL for the list of animal names
WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_animal_names"

# Image download configuration
IMAGE_DOWNLOAD_DIR = Path("/tmp")
IMAGE_DOWNLOAD_TIMEOUT = 30  # seconds
MAX_CONCURRENT_DOWNLOADS = 10  # Respectful to Wikipedia's servers
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds, will use exponential backoff

# User agent to identify our scraper politely
USER_AGENT = "Mozilla/5.0 (compatible; AnimalScraper/1.0; Educational Purpose)"

# Request configuration
REQUEST_TIMEOUT = 15  # seconds
REQUEST_DELAY = 0.1  # seconds between requests to be respectful

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# File extensions for image downloads
SUPPORTED_IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg']

# Maximum image file size (in bytes) - 15MB
MAX_IMAGE_SIZE = 15 * 1024 * 1024
