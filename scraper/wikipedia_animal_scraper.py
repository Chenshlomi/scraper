"""
Web scraping module for extracting animal names and collateral adjectives from Wikipedia.
FIXED VERSION - addresses the bugs:
1. Handle 304 status codes in Wikipedia API calls
2. Split collateral adjectives on "," and <br> tags, format with ", " in reports
3. Only parse tables with collateral adjective column
4. Extract only collateral adjective values for each animal
5. Use Wikipedia REST API to get proper animal images
"""

import logging
import time
import re
from typing import List, Dict, Optional, Tuple
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup, Tag

from config import (
    WIKIPEDIA_URL, USER_AGENT, REQUEST_TIMEOUT, REQUEST_DELAY
)
from utils.helpers import normalize_text, retry_with_backoff


class WikipediaAnimalScraper:
    """
    Scraper class for extracting animal data from Wikipedia's list of animal names.
    Fixed to properly handle collateral adjectives, animal images, and API status codes.
    """

    def __init__(self):
        """Initialize the scraper with session and logging."""
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': USER_AGENT})

        # For demo purposes, disable SSL verification
        # In production, always use proper SSL verification
        self.session.verify = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.logger.warning("SSL verification disabled - not recommended for production")

        # Cache for parsed pages and API responses to avoid re-downloading
        self._page_cache = {}
        self._api_cache = {}

        # Wikipedia REST API base URL for getting animal summaries and images
        self.wiki_api_base = "https://en.wikipedia.org/api/rest_v1/page/summary/"

    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a webpage with error handling and caching.

        Args:
            url (str): URL to fetch

        Returns:
            Optional[BeautifulSoup]: Parsed HTML or None if failed
        """
        if url in self._page_cache:
            self.logger.debug(f"Using cached page for {url}")
            return self._page_cache[url]

        @retry_with_backoff
        def fetch_page():
            self.logger.info(f"Fetching page: {url}")
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.text

        try:
            html_content = fetch_page()
            soup = BeautifulSoup(html_content, 'html.parser')
            self._page_cache[url] = soup

            # Be respectful to the server
            time.sleep(REQUEST_DELAY)

            return soup

        except Exception as e:
            self.logger.error(f"Failed to fetch page {url}: {e}")
            return None

    def _identify_collateral_adjective_tables(self, soup: BeautifulSoup) -> List[Tag]:
        """
        Identify tables that contain a "collateral adjective" column.

        Args:
            soup (BeautifulSoup): Parsed HTML of the Wikipedia page

        Returns:
            List[Tag]: List of table elements that contain collateral adjective data
        """
        valid_tables = []
        tables = soup.find_all('table', class_='wikitable')

        self.logger.info(f"Found {len(tables)} total tables, filtering for collateral adjective tables")

        for table_idx, table in enumerate(tables):
            # Look for header row to identify column structure
            header_row = table.find('tr')
            if not header_row:
                continue

            header_cells = header_row.find_all(['th', 'td'])
            header_texts = [cell.get_text(strip=True).lower() for cell in header_cells]

            # Check if this table has a collateral adjective column
            # Look for variations of "collateral adjective" in headers
            collateral_indicators = [
                'collateral adjective', 'adjective', 'collateral',
                'adjectival', 'relating adjective'
            ]

            has_collateral_column = any(
                any(indicator in header_text for indicator in collateral_indicators)
                for header_text in header_texts
            )

            if has_collateral_column:
                self.logger.info(f"Table {table_idx + 1} contains collateral adjective column")
                self.logger.debug(f"Headers: {header_texts}")
                valid_tables.append(table)
            else:
                self.logger.debug(f"Table {table_idx + 1} skipped - no collateral adjective column")
                self.logger.debug(f"Headers: {header_texts}")

        self.logger.info(f"Found {len(valid_tables)} tables with collateral adjective columns")
        return valid_tables

    def _find_collateral_adjective_column_index(self, header_row: Tag) -> int:
        """
        Find the exact index of the collateral adjective column.

        Args:
            header_row (Tag): Header row of the table

        Returns:
            int: Index of the collateral adjective column, -1 if not found
        """
        header_cells = header_row.find_all(['th', 'td'])

        for idx, cell in enumerate(header_cells):
            cell_text = cell.get_text(strip=True).lower()

            # Look for collateral adjective indicators
            if any(indicator in cell_text for indicator in [
                'collateral adjective', 'adjective', 'collateral',
                'adjectival', 'relating adjective'
            ]):
                self.logger.debug(f"Found collateral adjective column at index {idx}: '{cell_text}'")
                return idx

        return -1

    def _extract_animal_name_and_link(self, cell: Tag) -> Tuple[str, Optional[str]]:
        """
        Extract animal name and Wikipedia link from a table cell.

        Args:
            cell (Tag): Table cell containing animal information

        Returns:
            Tuple[str, Optional[str]]: (animal_name, wiki_link_suffix)
        """
        # Get the text content
        animal_name = cell.get_text(strip=True)
        animal_name = normalize_text(animal_name)

        # Look for Wikipedia link
        wiki_link = None
        link_tag = cell.find('a', href=True)
        if link_tag:
            href = link_tag.get('href', '')
            # Extract link without /wiki/ prefix as required
            if href.startswith('/wiki/'):
                wiki_link = href[6:]  # Remove '/wiki/' prefix
                self.logger.debug(f"Found wiki link for {animal_name}: {wiki_link}")

        return animal_name, wiki_link

    def _get_animal_image_from_api(self, animal_wiki_name: str) -> Optional[str]:
        """
        FIXED: Get animal image URL using Wikipedia REST API with proper 304 handling.

        Args:
            animal_wiki_name (str): Wikipedia page name (without /wiki/ prefix)

        Returns:
            Optional[str]: Image URL from thumbnail.source or None if not found
        """
        if not animal_wiki_name:
            return None

        # Check cache first
        if animal_wiki_name in self._api_cache:
            self.logger.debug(f"Using cached API response for {animal_wiki_name}")
            return self._api_cache[animal_wiki_name]

        try:
            # Construct API URL - need to URL encode the animal name
            encoded_name = quote(animal_wiki_name, safe='')
            api_url = f"{self.wiki_api_base}{encoded_name}"

            self.logger.debug(f"Fetching API data for {animal_wiki_name}: {api_url}")

            response = self.session.get(api_url, timeout=REQUEST_TIMEOUT)

            # FIX: Handle 304 Not Modified responses
            if response.status_code == 304:
                self.logger.debug(f"Received 304 Not Modified for {animal_wiki_name}, content not changed")
                # For 304, we should still try to get the image URL if we have cached data
                # But since we don't have the previous response, we'll treat it as no image available
#                self._api_cache[animal_wiki_name] = None
 #               return None

            response.raise_for_status()

            api_data = response.json()

            # Extract thumbnail source as specified in the requirements
            thumbnail = api_data.get('thumbnail')
            if thumbnail and 'source' in thumbnail:
                image_url = thumbnail['source']
                self.logger.info(f"Found image for {animal_wiki_name}: {image_url}")
                self._api_cache[animal_wiki_name] = image_url
                return image_url
            else:
                self.logger.debug(f"No thumbnail found for {animal_wiki_name}")
                self._api_cache[animal_wiki_name] = None
                return None

        except requests.exceptions.RequestException as e:
            self.logger.warning(f"API request failed for {animal_wiki_name}: {e}")
            self._api_cache[animal_wiki_name] = None
            return None
        except ValueError as e:
            self.logger.warning(f"JSON parsing failed for {animal_wiki_name}: {e}")
            self._api_cache[animal_wiki_name] = None
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error getting image for {animal_wiki_name}: {e}")
            self._api_cache[animal_wiki_name] = None
            return None


    def _extract_table_data(self, tables: List[Tag]) -> List[Dict[str, str]]:
        """
        Extract animal data from valid tables containing collateral adjectives.
        Fixed to properly handle HTML and comma-separated adjectives.

        Args:
            tables (List[Tag]): List of valid table elements

        Returns:
            List[Dict[str, str]]: List of animal-adjective pairs
        """
        animal_data = []

        for table_idx, table in enumerate(tables):
            self.logger.info(f"Processing table {table_idx + 1} of {len(tables)}")

            # Find header row and locate collateral adjective column
            header_row = table.find('tr')
            if not header_row:
                self.logger.warning(f"No header row found in table {table_idx + 1}")
                continue

            adjective_column_idx = self._find_collateral_adjective_column_index(header_row)
            if adjective_column_idx == -1:
                self.logger.warning(f"No collateral adjective column found in table {table_idx + 1}")
                continue

            # Process data rows (skip header)
            data_rows = table.find_all('tr')[1:]
            self.logger.info(f"Processing {len(data_rows)} data rows in table {table_idx + 1}")

            for row_idx, row in enumerate(data_rows):
                try:
                    cells = row.find_all(['td', 'th'])

                    # Ensure we have enough columns
                    if len(cells) <= max(0, adjective_column_idx):
                        self.logger.debug(f"Row {row_idx + 1} has insufficient columns")
                        continue

                    # Extract animal name from first column (usually)
                    animal_cell = cells[0]
                    animal_name, wiki_link = self._extract_animal_name_and_link(animal_cell)

                    if not animal_name or len(animal_name) < 2:
                        self.logger.debug(f"Invalid animal name in row {row_idx + 1}: '{animal_name}'")
                        continue

                    # Extract ONLY the collateral adjective column value
                    adjective_cell = cells[adjective_column_idx]

                    # FIX: Handle both HTML and text content for proper <br> tag processing
                    adjective_html = str(adjective_cell)
                    adjective_text = adjective_cell.get_text(strip=True)
                    adjective_text = normalize_text(adjective_text)

                    # Skip empty or invalid adjectives
                    if not adjective_text or adjective_text.lower() in ['—', '-', 'n/a', 'none', '']:
                        self.logger.debug(f"No valid adjective for {animal_name}")
                        continue

                    # Get image URL using Wikipedia API
                    image_url = None
                    if wiki_link:
                        image_url = self._get_animal_image_from_api(wiki_link)

                    # FIX: Handle multiple adjectives separated by commas, semicolons, and <br> tags
                    adjectives = self._split_multiple_adjectives(adjective_html, adjective_text)

                    # Create entries for each adjective (as required)
                    for adjective in adjectives:
                        animal_entry = {
                            'animal': animal_name,
                            'adjective': adjective,
                            'wiki_link': wiki_link,
                            'primary_image_url': image_url
                        }
                        animal_data.append(animal_entry)
                        self.logger.debug(f"Added: {animal_name} -> {adjective}")

                except Exception as e:
                    self.logger.warning(f"Error processing row {row_idx + 1} in table {table_idx + 1}: {e}")
                    continue

        self.logger.info(f"Extracted {len(animal_data)} animal-adjective pairs total")
        return animal_data

    def _split_multiple_adjectives(self, adjective_html: str, adjective_text: str) -> List[str]:
        """
        FIXED: Split text that might contain multiple adjectives, handling both commas and <br> tags.

        Args:
            adjective_html (str): HTML content of the adjective cell
            adjective_text (str): Plain text content of the adjective cell

        Returns:
            List[str]: List of individual adjectives
        """
        # First, handle <br> tags in HTML by replacing them with commas
        html_with_br_replaced = re.sub(r'<br[^>]*>', ',', adjective_html, flags=re.IGNORECASE)

        # Extract text from the modified HTML
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_with_br_replaced, 'html.parser')
        text_with_br_as_comma = soup.get_text(strip=True)
        text_with_br_as_comma = normalize_text(text_with_br_as_comma)

        # Use the processed text if it's different from original, otherwise use original
        final_text = text_with_br_as_comma if text_with_br_as_comma != adjective_text else adjective_text

        # Common separators for multiple adjectives
        separators = [',', ';', ' or ', ' and ', '/', ' & ']

        adjectives = [final_text]

        # Apply each separator
        for separator in separators:
            new_adjectives = []
            for adj in adjectives:
                if separator in adj:
                    split_parts = adj.split(separator)
                    new_adjectives.extend(split_parts)
                else:
                    new_adjectives.append(adj)
            adjectives = new_adjectives

        # Clean up and filter out empty strings
        cleaned_adjectives = []
        for adj in adjectives:
            cleaned = normalize_text(adj)
            if cleaned and len(cleaned) > 1 and cleaned.lower() not in ['n/a', 'none', '—', '-']:
                cleaned_adjectives.append(cleaned)

        return cleaned_adjectives if cleaned_adjectives else [final_text]

    def scrape_animals_and_adjectives(self) -> List[Dict[str, str]]:
        """
        FIXED main method to scrape animal names and their collateral adjectives.
        Implements all bug fixes including 304 handling and improved adjective splitting.

        Returns:
            List[Dict[str, str]]: List of dictionaries containing animal and adjective data
        """
        self.logger.info("Starting FIXED scraper for animal data from Wikipedia")

        # Get the main Wikipedia page
        soup = self._get_page(WIKIPEDIA_URL)
        if not soup:
            self.logger.error("Failed to fetch the main Wikipedia page")
            return []

        # Only get tables that contain collateral adjective columns
        valid_tables = self._identify_collateral_adjective_tables(soup)
        if not valid_tables:
            self.logger.error("No tables with collateral adjective columns found")
            return []

        # Extract data properly with correct column handling, API images, and fixed splitting
        animal_data = self._extract_table_data(valid_tables)

        if not animal_data:
            self.logger.warning("No animal data found in valid tables")
            return []

        self.logger.info(f"Successfully scraped data for {len(animal_data)} animal-adjective pairs")

        # Log some statistics
        animals_with_images = sum(1 for entry in animal_data if entry.get('primary_image_url'))
        unique_animals = len(set(entry['animal'] for entry in animal_data))
        unique_adjectives = len(set(entry['adjective'] for entry in animal_data))

        self.logger.info(f"Statistics: {unique_animals} unique animals, "
                         f"{unique_adjectives} unique adjectives, "
                         f"{animals_with_images} entries with images")

        return animal_data

    def get_formatted_adjectives_report(self, animal_data: List[Dict[str, str]]) -> str:
        """
        FIXED: Generate a formatted report of animals and their adjectives with proper ", " separation.

        Args:
            animal_data (List[Dict[str, str]]): List of animal-adjective pairs

        Returns:
            str: Formatted report string
        """
        from collections import defaultdict

        # Group adjectives by animal
        animal_adjectives = defaultdict(list)
        for entry in animal_data:
            animal_adjectives[entry['animal']].append(entry['adjective'])

        # Sort animals alphabetically
        sorted_animals = sorted(animal_adjectives.keys())

        report_lines = ["Animal Adjectives Report", "=" * 50, ""]

        for animal in sorted_animals:
            adjectives = sorted(set(animal_adjectives[animal]))  # Remove duplicates and sort
            # FIX: Use ", " as delimiter between adjectives in the report
            adjectives_str = ", ".join(adjectives)
            report_lines.append(f"{animal}: {adjectives_str}")

        report_lines.append("")
        report_lines.append(f"Total unique animals: {len(sorted_animals)}")
        total_adjective_pairs = sum(len(adj_list) for adj_list in animal_adjectives.values())
        report_lines.append(f"Total animal-adjective pairs: {total_adjective_pairs}")

        return "\n".join(report_lines)

    def close(self):
        """Close the session and clean up resources."""
        self.session.close()
        self.logger.info("Fixed scraper session closed")