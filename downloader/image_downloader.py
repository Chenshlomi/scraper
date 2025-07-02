"""
Image downloading module with threading support for concurrent downloads.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    IMAGE_DOWNLOAD_DIR, IMAGE_DOWNLOAD_TIMEOUT, MAX_CONCURRENT_DOWNLOADS,
    MAX_RETRIES, RETRY_DELAY, USER_AGENT, MAX_IMAGE_SIZE
)
from utils.helpers import create_local_image_path, sanitize_filename


class ImageDownloader:
    """
    Handles concurrent downloading of animal images with proper error handling and rate limiting.
    """

    def __init__(self):
        """Initialize the image downloader with session configuration."""
        self.logger = logging.getLogger(__name__)

        # Create download directory if it doesn't exist
        IMAGE_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

        # Configure session with retry strategy
        self.session = requests.Session()

        # Disable SSL verification for demo purposes
        self.session.verify = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.logger.warning("SSL verification disabled for image downloads - not recommended for production")

        # Set up retry strategy for robust downloading
        retry_strategy = Retry(
            total=MAX_RETRIES,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=RETRY_DELAY
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
       # self.session.mount("https://", adapter)

        # Set headers
        self.session.headers.update({
            'User-Agent': USER_AGENT,
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache'
        })

        # Track download statistics
        self.download_stats = {
            'attempted': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }

    def _download_single_image(self, animal_name: str, image_url: str) -> Tuple[bool, str, Optional[Path]]:
        """
        Download a single image for an animal.

        Args:
            animal_name (str): Name of the animal
            image_url (str): URL of the image to download

        Returns:
            Tuple[bool, str, Optional[Path]]: (success, message, local_path)
        """
        try:
            self.download_stats['attempted'] += 1

            # Create local file path
            local_path = create_local_image_path(animal_name, image_url)

            # Skip if file already exists
            if local_path.exists():
                self.logger.debug(f"Image already exists: {local_path}")
                self.download_stats['skipped'] += 1
                return True, "File already exists", local_path

            self.logger.info(f"Downloading image for {animal_name}: {image_url}")

            # Download with streaming to handle large files
            response = self.session.get(
                image_url,
                timeout=IMAGE_DOWNLOAD_TIMEOUT,
                stream=True
            )
            response.raise_for_status()

            # Check content type
            content_type = response.headers.get('content-type', '').lower()
            if not content_type.startswith('image/'):
                self.logger.warning(f"Non-image content type for {image_url}: {content_type}")
                self.download_stats['failed'] += 1
                return False, f"Non-image content type: {content_type}", None

            # Check file size
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > MAX_IMAGE_SIZE:
                self.logger.warning(f"Image too large for {animal_name}: {content_length} bytes")
                self.download_stats['failed'] += 1
                return False, f"Image too large: {content_length} bytes", None

            # Download the image
            total_size = 0
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        total_size += len(chunk)

                        # Check size during download
                        if total_size > MAX_IMAGE_SIZE:
                            f.close()
                            local_path.unlink()  # Delete partial file
                            self.logger.warning(f"Image too large during download for {animal_name}")
                            self.download_stats['failed'] += 1
                            return False, "Image too large during download", None

            # Verify the downloaded file
            if not local_path.exists() or local_path.stat().st_size == 0:
                self.logger.error(f"Downloaded file is empty or doesn't exist: {local_path}")
                if local_path.exists():
                    local_path.unlink()
                self.download_stats['failed'] += 1
                return False, "Downloaded file is empty", None

            self.logger.info(f"Successfully downloaded image for {animal_name}: {local_path} ({total_size} bytes)")
            self.download_stats['successful'] += 1
            return True, f"Downloaded successfully ({total_size} bytes)", local_path

        except requests.exceptions.Timeout:
            self.logger.warning(f"Timeout downloading image for {animal_name}: {image_url}")
            self.download_stats['failed'] += 1
            return False, "Download timeout", None

        except requests.exceptions.RequestException as e:
            self.logger.warning(f"Request error downloading image for {animal_name}: {e}")
            self.download_stats['failed'] += 1
            return False, f"Request error: {str(e)}", None

        except OSError as e:
            self.logger.error(f"File system error downloading image for {animal_name}: {e}")
            self.download_stats['failed'] += 1
            return False, f"File system error: {str(e)}", None

        except Exception as e:
            self.logger.error(f"Unexpected error downloading image for {animal_name}: {e}")
            self.download_stats['failed'] += 1
            return False, f"Unexpected error: {str(e)}", None

    def _get_fallback_image_urls(self, animal_name: str) -> List[str]:
        """
        Get fallback image URLs for an animal from common sources.

        Args:
            animal_name (str): Name of the animal

        Returns:
            List[str]: List of potential image URLs
        """
        fallback_urls = []

        # Try to construct URLs for common animal image sources
        # This is a basic approach - in production, we might use proper APIs
        animal_clean = sanitize_filename(animal_name)

        # Wikipedia Commons often has animal images
        commons_url = f"http://commons.wikimedia.org/wiki/File:{animal_name.replace(' ', '_')}.jpg"
        fallback_urls.append(commons_url)

        # Note: In a real production system, we would use proper APIs like:
        # - Wikimedia Commons API
        # - iNaturalist API
        # - Biodiversity Heritage Library
        # - Encyclopedia of Life API

        return fallback_urls

    def download_images_concurrently(self, animal_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Download images for all animals concurrently using threading.

        Args:
            animal_data (List[Dict[str, str]]): List of animal data dictionaries

        Returns:
            List[Dict[str, str]]: Updated animal data with local image paths
        """
        self.logger.info(f"Starting concurrent download of images for {len(animal_data)} animals")

        # Reset statistics
        self.download_stats = {
            'attempted': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }

        # Prepare download tasks
        download_tasks = []
        animal_to_data_map = {}  # Map to update original data

        for idx, animal_entry in enumerate(animal_data):
            animal_name = animal_entry['animal']
            image_url = animal_entry.get('primary_image_url')

            # Skip if no image URL available
            if not image_url:
                # Try to find fallback images
                fallback_urls = self._get_fallback_image_urls(animal_name)
                if fallback_urls:
                    image_url = fallback_urls[0]
                    self.logger.debug(f"Using fallback image URL for {animal_name}: {image_url}")
                else:
                    self.logger.debug(f"No image URL available for {animal_name}")
                    animal_entry['local_image_path'] = None
                    animal_entry['download_status'] = "No image URL available"
                    continue

            download_tasks.append((animal_name, image_url))
            animal_to_data_map[(animal_name, image_url)] = idx

        self.logger.info(f"Prepared {len(download_tasks)} download tasks")

        # Execute downloads concurrently
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS) as executor:
            # Submit all download tasks
            future_to_task = {
                executor.submit(self._download_single_image, animal_name, image_url): (animal_name, image_url)
                for animal_name, image_url in download_tasks
            }

            # Process completed downloads
            for future in as_completed(future_to_task):
                animal_name, image_url = future_to_task[future]

                try:
                    success, message, local_path = future.result()

                    # Update the original animal data
                    data_idx = animal_to_data_map.get((animal_name, image_url))
                    if data_idx is not None:
                        animal_data[data_idx]['local_image_path'] = str(local_path) if local_path else None
                        animal_data[data_idx]['download_status'] = message
                        animal_data[data_idx]['download_success'] = success

                    if success:
                        self.logger.debug(f"✓ {animal_name}: {message}")
                    else:
                        self.logger.warning(f"✗ {animal_name}: {message}")

                except Exception as e:
                    self.logger.error(f"Exception in download task for {animal_name}: {e}")
                    # Update with error status
                    data_idx = animal_to_data_map.get((animal_name, image_url))
                    if data_idx is not None:
                        animal_data[data_idx]['local_image_path'] = None
                        animal_data[data_idx]['download_status'] = f"Exception: {str(e)}"
                        animal_data[data_idx]['download_success'] = False

        # Log final statistics
        self.logger.info("Image download completed!")
        self.logger.info(f"Statistics: {self.download_stats['successful']} successful, "
                        f"{self.download_stats['failed']} failed, "
                        f"{self.download_stats['skipped']} skipped, "
                        f"{self.download_stats['attempted']} total attempted")

        return animal_data

    def cleanup_failed_downloads(self):
        """Clean up any partially downloaded or empty image files."""
        self.logger.info("Cleaning up failed downloads...")

        if not IMAGE_DOWNLOAD_DIR.exists():
            return

        cleaned_count = 0
        for image_file in IMAGE_DOWNLOAD_DIR.glob("*_image.*"):
            try:
                if image_file.stat().st_size == 0:
                    image_file.unlink()
                    cleaned_count += 1
                    self.logger.debug(f"Removed empty file: {image_file}")
            except Exception as e:
                self.logger.warning(f"Error cleaning up {image_file}: {e}")

        if cleaned_count > 0:
            self.logger.info(f"Cleaned up {cleaned_count} empty image files")

    def get_download_statistics(self) -> Dict[str, int]:
        """Get download statistics."""
        return self.download_stats.copy()

    def close(self):
        """Close the session and clean up resources."""
        self.cleanup_failed_downloads()
        self.session.close()
        self.logger.info("Image downloader session closed")
