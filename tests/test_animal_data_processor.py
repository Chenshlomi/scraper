"""
Comprehensive test suite for the Animal Scraper Application.
Tests cover data processing, web scraping, and validation logic.
"""

import pytest
from unittest.mock import Mock, patch

import requests

# Import the modules to test
from processor.data_processor import AnimalDataProcessor
from scraper.wikipedia_animal_scraper import WikipediaAnimalScraper

class TestAnimalDataProcessor:
    """Test cases for the AnimalDataProcessor class."""

    @pytest.fixture
    def processor(self):
        """Create a fresh AnimalDataProcessor instance for each test."""
        return AnimalDataProcessor()

    @pytest.fixture
    def sample_valid_data(self):
        """Sample valid animal data for testing."""
        return [
            {
                'animal': 'Lion',
                'adjective': 'Leonine',
                'wiki_link': 'Lion',
                'primary_image_url': 'http://example.com/lion.jpg'
            },
            {
                'animal': 'Eagle',
                'adjective': 'Aquiline',
                'wiki_link': 'Eagle',
                'primary_image_url': 'http://example.com/eagle.jpg'
            },
            {
                'animal': 'Dog',
                'adjective': 'Canine',
                'wiki_link': 'Dog',
                'primary_image_url': None
            }
        ]

    @pytest.fixture
    def sample_invalid_data(self):
        """Sample invalid animal data for testing validation."""
        return [
            {
                'animal': '',  # Empty animal name
                'adjective': 'Invalid',
                'wiki_link': 'Test',
                'primary_image_url': None
            },
            {
                'animal': 'Cat',
                'adjective': '',  # Empty adjective
                'wiki_link': 'Cat',
                'primary_image_url': None
            },
            {
                'animal': 'Bear',
                'adjective': 'n/a',  # Invalid adjective
                'wiki_link': 'Bear',
                'primary_image_url': None
            },
            {
                'animal': 'Wolf',
                'adjective': 'Wolf',  # Animal name same as adjective
                'wiki_link': 'Wolf',
                'primary_image_url': None
            }
        ]

    def test_validate_animal_entry_valid_data(self, processor, sample_valid_data):
        """Test validation of valid animal entries."""
        for entry in sample_valid_data:
            assert processor.validate_animal_entry(entry) is True

    def test_validate_animal_entry_invalid_data(self, processor, sample_invalid_data):
        """Test validation rejects invalid animal entries."""
        for entry in sample_invalid_data:
            assert processor.validate_animal_entry(entry) is False

    def test_validate_animal_entry_missing_fields(self, processor):
        """Test validation handles missing required fields."""
        # Missing animal field
        entry_no_animal = {'adjective': 'Feline'}
        assert processor.validate_animal_entry(entry_no_animal) is False

        # Missing adjective field
        entry_no_adjective = {'animal': 'Cat'}
        assert processor.validate_animal_entry(entry_no_adjective) is False

        # Empty entry
        empty_entry = {}
        assert processor.validate_animal_entry(empty_entry) is False

    def test_deduplicate_entries_removes_duplicates(self, processor):
        """Test deduplication removes exact duplicates."""
        data_with_duplicates = [
            {'animal': 'Lion', 'adjective': 'Leonine'},
            {'animal': 'Lion', 'adjective': 'Leonine'},  # Duplicate
            {'animal': 'Eagle', 'adjective': 'Aquiline'},
            {'animal': 'Lion', 'adjective': 'Royal'},  # Different adjective, should keep
        ]

        deduplicated = processor.deduplicate_entries(data_with_duplicates)

        assert len(deduplicated) == 3
        assert processor.processing_stats['duplicate_entries'] == 1

        # Verify the correct entries remain
        animals_adjectives = [(entry['animal'], entry['adjective']) for entry in deduplicated]
        expected = [('Lion', 'Leonine'), ('Eagle', 'Aquiline'), ('Lion', 'Royal')]

        # Sort both lists for comparison since order might vary
        assert sorted(animals_adjectives) == sorted(expected)


    def test_process_animal_data_integration(self, processor):
        """Test the complete data processing pipeline."""
        raw_data = [
            {'animal': 'Lion', 'adjective': 'Leonine'},
            {'animal': 'Lion', 'adjective': 'Leonine'},  # Duplicate
            {'animal': '', 'adjective': 'Invalid'},  # Invalid
            {'animal': 'Eagle', 'adjective': 'Aquiline'},
            {'animal': 'Dog', 'adjective': 'Canine'},
        ]

        processed_data, analysis = processor.process_animal_data(raw_data)

        # Should have 3 valid entries after processing
        assert len(processed_data) == 3

        # Check statistics
        stats = processor.get_processing_statistics()
        assert stats['original_entries'] == 5
        assert stats['valid_entries'] == 4  # 4 passed validation
        assert stats['duplicate_entries'] == 1
        assert stats['unique_animals'] == 3
        assert stats['unique_adjectives'] == 3

        # Verify analysis is returned
        assert isinstance(analysis, dict)
        assert 'animals_by_adjective_count' in analysis

    def test_sort_entries(self, processor, sample_valid_data):
        """Test sorting functionality."""
        # Add metadata needed for sorting
        enriched_data = processor.enrich_entries_with_metadata(sample_valid_data)

        # Test sort by animal (default)
        sorted_by_animal = processor.sort_entries(enriched_data, 'animal')
        animal_names = [entry['animal'] for entry in sorted_by_animal]
        assert animal_names == sorted(animal_names)

        # Test sort by adjective
        sorted_by_adjective = processor.sort_entries(enriched_data, 'adjective')
        adjective_names = [entry['adjective'] for entry in sorted_by_adjective]
        assert adjective_names == sorted(adjective_names)

        # Test sort by image availability
        sorted_by_image = processor.sort_entries(enriched_data, 'has_image')
        # Should have entries with images first, then without
        has_image_values = [entry['has_image'] for entry in sorted_by_image]
        # Find the first False value
        first_false_index = next((i for i, val in enumerate(has_image_values) if not val), len(has_image_values))
        # All True values should come before False values
        assert all(has_image_values[:first_false_index])

    def test_get_processing_statistics(self, processor):
        """Test statistics retrieval."""
        # Initially should have zero stats
        initial_stats = processor.get_processing_statistics()
        assert initial_stats['original_entries'] == 0
        assert initial_stats['valid_entries'] == 0

        # Process some data
        raw_data = [
            {'animal': 'Lion', 'adjective': 'Leonine'},
            {'animal': 'Eagle', 'adjective': 'Aquiline'},
        ]

        processor.process_animal_data(raw_data)

        # Stats should be updated
        final_stats = processor.get_processing_statistics()
        assert final_stats['original_entries'] == 2
        assert final_stats['valid_entries'] == 2


class TestWikipediaAnimalScraper:
    """Test cases for the WikipediaAnimalScraper class."""

    @pytest.fixture
    def scraper(self):
        """Create a fresh WikipediaAnimalScraper instance for each test."""
        return WikipediaAnimalScraper()

    @pytest.fixture
    def mock_html_table(self):
        """Mock HTML table content for testing."""
        return """
        <table class="wikitable">
            <tr>
                <th>Animal</th>
                <th>Collateral Adjective</th>
                <th>Other Info</th>
            </tr>
            <tr>
                <td><a href="/wiki/Lion">Lion</a></td>
                <td>Leonine</td>
                <td>King of jungle</td>
            </tr>
            <tr>
                <td><a href="/wiki/Eagle">Eagle</a></td>
                <td>Aquiline, Majestic</td>
                <td>Bird of prey</td>
            </tr>
            <tr>
                <td><a href="/wiki/Dog">Dog</a></td>
                <td>Canine<br>Loyal</td>
                <td>Domestic animal</td>
            </tr>
        </table>
        """

    def test_extract_animal_name_and_link(self, scraper):
        """Test extraction of animal names and Wikipedia links."""
        from bs4 import BeautifulSoup

        # Test with link
        html_with_link = '<td><a href="/wiki/Lion">Lion</a></td>'
        soup = BeautifulSoup(html_with_link, 'html.parser')
        cell = soup.find('td')

        animal_name, wiki_link = scraper._extract_animal_name_and_link(cell)
        assert animal_name == 'Lion'
        assert wiki_link == 'Lion'

        # Test without link
        html_without_link = '<td>Tiger</td>'
        soup = BeautifulSoup(html_without_link, 'html.parser')
        cell = soup.find('td')

        animal_name, wiki_link = scraper._extract_animal_name_and_link(cell)
        assert animal_name == 'Tiger'
        assert wiki_link is None

    def test_split_multiple_adjectives_comma_separated(self, scraper):
        """Test splitting comma-separated adjectives."""
        html_content = '<td>Aquiline, Majestic</td>'
        text_content = 'Aquiline, Majestic'

        adjectives = scraper._split_multiple_adjectives(html_content, text_content)
        assert len(adjectives) == 2
        assert 'Aquiline' in adjectives
        assert 'Majestic' in adjectives

    def test_split_multiple_adjectives_br_tags(self, scraper):
        """Test splitting adjectives separated by <br> tags."""
        html_content = '<td>Canine<br>Loyal</td>'
        text_content = 'Canine Loyal'  # How it would appear after get_text()

        adjectives = scraper._split_multiple_adjectives(html_content, text_content)
        assert len(adjectives) == 2
        assert 'Canine' in adjectives
        assert 'Loyal' in adjectives

    def test_split_multiple_adjectives_mixed_separators(self, scraper):
        """Test splitting adjectives with mixed separators."""
        html_content = '<td>Leonine, Royal and Majestic</td>'
        text_content = 'Leonine, Royal and Majestic'

        adjectives = scraper._split_multiple_adjectives(html_content, text_content)
        assert len(adjectives) == 3
        assert 'Leonine' in adjectives
        assert 'Royal' in adjectives
        assert 'Majestic' in adjectives

    def test_split_multiple_adjectives_single_adjective(self, scraper):
        """Test that single adjectives are returned as-is."""
        html_content = '<td>Feline</td>'
        text_content = 'Feline'

        adjectives = scraper._split_multiple_adjectives(html_content, text_content)
        assert len(adjectives) == 1
        assert adjectives[0] == 'Feline'

    def test_find_collateral_adjective_column_index(self, scraper):
        """Test finding the correct column index for collateral adjectives."""
        from bs4 import BeautifulSoup

        # Test with "Collateral Adjective" header
        html = """
        <tr>
            <th>Animal</th>
            <th>Collateral Adjective</th>
            <th>Other</th>
        </tr>
        """
        soup = BeautifulSoup(html, 'html.parser')
        header_row = soup.find('tr')

        index = scraper._find_collateral_adjective_column_index(header_row)
        assert index == 1

        # Test with "Adjective" header
        html_simple = """
        <tr>
            <th>Animal</th>
            <th>Adjective</th>
        </tr>
        """
        soup = BeautifulSoup(html_simple, 'html.parser')
        header_row = soup.find('tr')

        index = scraper._find_collateral_adjective_column_index(header_row)
        assert index == 1

        # Test with no matching header
        html_no_match = """
        <tr>
            <th>Animal</th>
            <th>Description</th>
        </tr>
        """
        soup = BeautifulSoup(html_no_match, 'html.parser')
        header_row = soup.find('tr')

        index = scraper._find_collateral_adjective_column_index(header_row)
        assert index == -1

    @patch('requests.Session.get')
    def test_get_animal_image_from_api_success(self, mock_get, scraper):
        """Test successful image URL retrieval from Wikipedia API."""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'thumbnail': {
                'source': 'https://upload.wikimedia.org/wikipedia/commons/thumb/7/73/Lion.jpg/250px-Lion.jpg',
                'width': 250,
                'height': 188
            }
        }
        mock_get.return_value = mock_response

        image_url = scraper._get_animal_image_from_api('Lion')

        assert image_url == 'https://upload.wikimedia.org/wikipedia/commons/thumb/7/73/Lion.jpg/250px-Lion.jpg'
        mock_get.assert_called_once()

    @patch('requests.Session.get')
    def test_get_animal_image_from_api_no_thumbnail(self, mock_get, scraper):
        """Test API response without thumbnail."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'title': 'Lion',
            'extract': 'The lion is a large cat...'
            # No thumbnail field
        }
        mock_get.return_value = mock_response

        image_url = scraper._get_animal_image_from_api('Lion')

        assert image_url is None
        mock_get.assert_called_once()

    @patch('requests.Session.get')
    def test_get_animal_image_from_api_304_status(self, mock_get, scraper):
        """Test handling of 304 Not Modified responses."""
        mock_response = Mock()
        mock_response.status_code = 304
        mock_get.return_value = mock_response

        image_url = scraper._get_animal_image_from_api('Lion')

        # Should handle 304 gracefully and return None
        assert image_url is None
        mock_get.assert_called_once()

    @patch('requests.Session.get')
    def test_get_animal_image_from_api_request_exception(self, mock_get, scraper):
        """Test handling of request exceptions."""
        mock_get.side_effect = requests.exceptions.RequestException("Network error")

        image_url = scraper._get_animal_image_from_api('Lion')

        assert image_url is None
        mock_get.assert_called_once()

    def test_get_formatted_adjectives_report(self, scraper):
        """Test generation of formatted adjectives report."""
        animal_data = [
            {'animal': 'Lion', 'adjective': 'Leonine'},
            {'animal': 'Lion', 'adjective': 'Royal'},
            {'animal': 'Eagle', 'adjective': 'Aquiline'},
            {'animal': 'Dog', 'adjective': 'Canine'},
        ]

        report = scraper.get_formatted_adjectives_report(animal_data)

        # Check that report contains expected content
        assert 'Animal Adjectives Report' in report
        assert 'Lion: Leonine, Royal' in report
        assert 'Eagle: Aquiline' in report
        assert 'Dog: Canine' in report
        assert 'Total unique animals: 3' in report
        assert 'Total animal-adjective pairs: 4' in report

    def test_identify_collateral_adjective_tables(self, scraper, mock_html_table):
        """Test identification of tables with collateral adjective columns."""
        from bs4 import BeautifulSoup

        # HTML with both valid and invalid tables
        html_content = f"""
        <html>
            <body>
                {mock_html_table}
                <table class="wikitable">
                    <tr>
                        <th>Animal</th>
                        <th>Habitat</th>
                        <th>Diet</th>
                    </tr>
                    <tr>
                        <td>Lion</td>
                        <td>Savanna</td>
                        <td>Carnivore</td>
                    </tr>
                </table>
            </body>
        </html>
        """

        soup = BeautifulSoup(html_content, 'html.parser')
        valid_tables = scraper._identify_collateral_adjective_tables(soup)

        # Should only find the table with collateral adjective column
        assert len(valid_tables) == 1

        # Verify it's the correct table
        header_row = valid_tables[0].find('tr')
        headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        assert 'Collateral Adjective' in headers

    def test_close_session(self, scraper):
        """Test that session is properly closed."""
        # Mock the session
        scraper.session = Mock()

        scraper.close()

        scraper.session.close.assert_called_once()


# Integration test for the complete workflow
class TestIntegration:
    """Integration tests for the complete animal scraper workflow."""

    @patch('scraper.wikipedia_animal_scraper.WikipediaAnimalScraper._get_page')
    @patch('scraper.wikipedia_animal_scraper.WikipediaAnimalScraper._get_animal_image_from_api')
    def test_complete_scraping_and_processing_workflow(self, mock_get_image, mock_get_page):
        """Test the complete workflow from scraping to processing."""
        from bs4 import BeautifulSoup

        # Mock the Wikipedia page
        mock_html = """
        <html>
            <body>
                <table class="wikitable">
                    <tr>
                        <th>Animal</th>
                        <th>Collateral Adjective</th>
                    </tr>
                    <tr>
                        <td><a href="/wiki/Lion">Lion</a></td>
                        <td>Leonine</td>
                    </tr>
                    <tr>
                        <td><a href="/wiki/Eagle">Eagle</a></td>
                        <td>Aquiline, Majestic</td>
                    </tr>
                    <tr>
                        <td><a href="/wiki/Dog">Dog</a></td>
                        <td>Canine</td>
                    </tr>
                </table>
            </body>
        </html>
        """

        mock_get_page.return_value = BeautifulSoup(mock_html, 'html.parser')
        mock_get_image.return_value = 'http://example.com/image.jpg'

        # Create scraper and processor
        scraper = WikipediaAnimalScraper()
        processor = AnimalDataProcessor()

        # Run the workflow
        raw_data = scraper.scrape_animals_and_adjectives()
        processed_data, analysis = processor.process_animal_data(raw_data)

        # Verify results
        assert len(processed_data) > 0

        # Should have entries for Lion, Eagle (with 2 adjectives), and Dog
        animals = [entry['animal'] for entry in processed_data]
        assert 'Lion' in animals
        assert 'Eagle' in animals
        assert 'Dog' in animals

        # Eagle should have 2 entries (2 adjectives)
        eagle_entries = [entry for entry in processed_data if entry['animal'] == 'Eagle']
        assert len(eagle_entries) == 2

        # All entries should have required fields
        for entry in processed_data:
            assert 'animal' in entry
            assert 'adjective' in entry
            assert 'animal_normalized' in entry
            assert 'adjective_normalized' in entry

        # Analysis should contain expected data
        assert isinstance(analysis, dict)
        assert 'unique_animals' in processor.get_processing_statistics()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])