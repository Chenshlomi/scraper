import logging
import time
from pathlib import Path
from typing import List, Dict
from collections import defaultdict



class HTMLFormatter:
    """
    HTML formatter class that generates rich, interactive HTML reports
    showing animals with their lists of collateral adjectives and images.
    """

    def __init__(self):
        """Initialize the HTML formatter with logging."""
        self.logger = logging.getLogger(__name__)

        # Track generation statistics
        self.generation_stats = {
            'total_animals': 0,
            'animals_with_images': 0,
            'total_adjectives': 0,
            'animals_with_multiple_adjectives': 0,
            'generation_time': 0
        }

    def _generate_html_head(self, title: str = "Animal Collateral Adjectives Report") -> str:
        """
        Generate the HTML head section with CSS styling.

        Args:
            title (str): Page title

        Returns:
            str: HTML head section with embedded CSS
        """
        # Using inline CSS for self-contained HTML file
        # Modern, responsive design with animal-themed colors
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        /* Modern CSS reset and base styles */
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #2c3e50;
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            min-height: 100vh;
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}

        .header {{
            text-align: center;
            margin-bottom: 40px;
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 8px 25px rgba(0,0,0,0.1);
        }}

        .header h1 {{
            color: #2c3e50;
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
        }}

        .header .subtitle {{
            color: #7f8c8d;
            font-size: 1.2em;
            font-style: italic;
        }}

        .stats-panel {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }}

        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }}

        .stat-card:hover {{
            transform: translateY(-5px);
        }}

        .stat-number {{
            font-size: 2em;
            font-weight: bold;
            color: #e74c3c;
            display: block;
        }}

        .stat-label {{
            color: #7f8c8d;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}

        .animals-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 25px;
            margin-top: 30px;
        }}

        .animal-card {{
            background: white;
            border-radius: 15px;
            overflow: hidden;
            box-shadow: 0 8px 25px rgba(0,0,0,0.1);
            transition: all 0.3s ease;
            position: relative;
        }}

        .animal-card:hover {{
            transform: translateY(-8px);
            box-shadow: 0 15px 40px rgba(0,0,0,0.15);
        }}

        .animal-image {{
            width: 100%;
            height: 200px;
            object-fit: cover;
            border-bottom: 3px solid #3498db;
        }}

        .animal-image.placeholder {{
            background: linear-gradient(45deg, #bdc3c7, #ecf0f1);
            display: flex;
            align-items: center;
            justify-content: center;
            color: #7f8c8d;
            font-size: 1.2em;
            font-weight: bold;
        }}

        .animal-content {{
            padding: 20px;
        }}

        .animal-name {{
            font-size: 1.4em;
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 15px;
            text-transform: capitalize;
        }}

        .adjectives-section {{
            margin-top: 15px;
        }}

        .adjectives-label {{
            font-weight: bold;
            color: #34495e;
            margin-bottom: 8px;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}

        .adjectives-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
        }}

        .adjective-tag {{
            background: linear-gradient(45deg, #3498db, #2980b9);
            color: white;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 500;
            text-transform: lowercase;
            box-shadow: 0 2px 5px rgba(52, 152, 219, 0.3);
            transition: all 0.2s ease;
        }}

        .adjective-tag:hover {{
            transform: scale(1.05);
            box-shadow: 0 4px 10px rgba(52, 152, 219, 0.4);
        }}

        .multiple-adjectives {{
            border-left: 4px solid #e74c3c;
        }}

        .image-info {{
            position: absolute;
            top: 10px;
            right: 10px;
            background: rgba(0,0,0,0.7);
            color: white;
            padding: 5px 8px;
            border-radius: 15px;
            font-size: 0.8em;
        }}

        .footer {{
            text-align: center;
            margin-top: 50px;
            padding: 30px;
            background: white;
            border-radius: 15px;
            box-shadow: 0 8px 25px rgba(0,0,0,0.1);
        }}

        .footer p {{
            color: #7f8c8d;
            margin-bottom: 10px;
        }}

        .search-box {{
            width: 100%;
            max-width: 400px;
            padding: 12px 20px;
            border: 2px solid #ddd;
            border-radius: 25px;
            font-size: 1em;
            margin: 20px auto;
            display: block;
            transition: border-color 0.3s ease;
        }}

        .search-box:focus {{
            outline: none;
            border-color: #3498db;
        }}

        .hidden {{
            display: none !important;
        }}

        /* Responsive design */
        @media (max-width: 768px) {{
            .animals-grid {{
                grid-template-columns: 1fr;
            }}

            .header h1 {{
                font-size: 2em;
            }}

            .stats-panel {{
                grid-template-columns: repeat(2, 1fr);
            }}
        }}
    </style>
</head>"""

    def _group_animals_by_name(self, animal_data: List[Dict[str, str]]) -> Dict[str, Dict]:
        """
        Group animal data by animal name, collecting all adjectives for each animal.
        This is the core logic for creating the animal -> adjectives mapping.

        Args:
            animal_data (List[Dict[str, str]]): List of animal-adjective pairs

        Returns:
            Dict[str, Dict]: Grouped data with animal names as keys
        """
        self.logger.info("Grouping animals by name and collecting adjectives...")

        # Use defaultdict to automatically create empty lists for new animals
        grouped_animals = defaultdict(lambda: {
            'adjectives': set(),  # Use set to avoid duplicates
            'image_path': None,
            'image_url': None,
            'wiki_link': None,
            'has_image': False
        })

        # Process each animal-adjective pair
        for entry in animal_data:
            animal_name = entry['animal'].strip()
            adjective = entry['adjective'].strip()

            # Skip invalid entries (defensive programming)
            if not animal_name or not adjective:
                self.logger.debug(f"Skipping invalid entry: {entry}")
                continue

            # Add adjective to the set (automatically handles duplicates)
            grouped_animals[animal_name]['adjectives'].add(adjective)

            # Store image information (prefer local path over URL)
            if entry.get('local_image_path') and not grouped_animals[animal_name]['image_path']:
                grouped_animals[animal_name]['image_path'] = entry['local_image_path']
                grouped_animals[animal_name]['has_image'] = True
                self.logger.debug(f"Added local image for {animal_name}: {entry['local_image_path']}")

            # Fallback to primary image URL if no local image
            if entry.get('primary_image_url') and not grouped_animals[animal_name]['image_url']:
                grouped_animals[animal_name]['image_url'] = entry['primary_image_url']

            # Store wiki link
            if entry.get('wiki_link') and not grouped_animals[animal_name]['wiki_link']:
                grouped_animals[animal_name]['wiki_link'] = entry['wiki_link']

        # Convert sets to sorted lists for consistent display
        final_grouped = {}
        for animal_name, data in grouped_animals.items():
            final_grouped[animal_name] = {
                'adjectives': sorted(list(data['adjectives'])),  # Convert set to sorted list
                'image_path': data['image_path'],
                'image_url': data['image_url'],
                'wiki_link': data['wiki_link'],
                'has_image': data['has_image'],
                'adjective_count': len(data['adjectives'])
            }

        self.logger.info(f"Grouped {len(animal_data)} entries into {len(final_grouped)} unique animals")
        return final_grouped

    def _calculate_statistics(self, grouped_animals: Dict[str, Dict]) -> Dict[str, int]:
        """
        Calculate comprehensive statistics for the HTML report.

        Args:
            grouped_animals (Dict[str, Dict]): Grouped animal data

        Returns:
            Dict[str, int]: Statistics dictionary
        """
        stats = {
            'total_animals': len(grouped_animals),
            'animals_with_images': sum(1 for data in grouped_animals.values() if data['has_image']),
            'total_adjectives': sum(len(data['adjectives']) for data in grouped_animals.values()),
            'animals_with_multiple_adjectives': sum(
                1 for data in grouped_animals.values() if len(data['adjectives']) > 1),
            'unique_adjectives': len(set(
                adj for data in grouped_animals.values()
                for adj in data['adjectives']
            ))
        }

        # Update class statistics
        self.generation_stats.update(stats)

        return stats

    def _generate_stats_panel_html(self, stats: Dict[str, int]) -> str:
        """
        Generate HTML for the statistics panel.

        Args:
            stats (Dict[str, int]): Statistics data

        Returns:
            str: HTML for statistics panel
        """
        return f"""
        <div class="stats-panel">
            <div class="stat-card">
                <span class="stat-number">{stats['total_animals']}</span>
                <span class="stat-label">Total Animals</span>
            </div>
            <div class="stat-card">
                <span class="stat-number">{stats['total_adjectives']}</span>
                <span class="stat-label">Total Adjectives</span>
            </div>
            <div class="stat-card">
                <span class="stat-number">{stats['unique_adjectives']}</span>
                <span class="stat-label">Unique Adjectives</span>
            </div>
            <div class="stat-card">
                <span class="stat-number">{stats['animals_with_multiple_adjectives']}</span>
                <span class="stat-label">Multiple Adjectives</span>
            </div>
            <div class="stat-card">
                <span class="stat-number">{stats['animals_with_images']}</span>
                <span class="stat-label">With Images</span>
            </div>
        </div>
        """

    def _generate_animal_card_html(self, animal_name: str, animal_data: Dict) -> str:
        """
        Generate HTML for a single animal card.

        Args:
            animal_name (str): Name of the animal
            animal_data (Dict): Animal data including adjectives and image info

        Returns:
            str: HTML for animal card
        """
        # Determine if this animal has multiple adjectives (for special styling)
        has_multiple = len(animal_data['adjectives']) > 1
        card_class = "animal-card multiple-adjectives" if has_multiple else "animal-card"

        # Generate image HTML - prefer local path, fallback to URL, then placeholder
        image_html = ""
        if animal_data['image_path']:
            # Local image path - convert to relative path for HTML
            image_path = str(animal_data['image_path'])
            image_html = f'<img src="file://{image_path}" alt="{animal_name}" class="animal-image" loading="lazy">'
            image_info = '<div class="image-info">📁 Local</div>'
        elif animal_data['image_url']:
            # Remote image URL
            image_html = f'<img src="{animal_data["image_url"]}" alt="{animal_name}" class="animal-image" loading="lazy">'
            image_info = '<div class="image-info">🌐 Remote</div>'
        else:
            # No image available - show placeholder
            image_html = f'<div class="animal-image placeholder">🐾 {animal_name}</div>'
            image_info = '<div class="image-info">❌ No Image</div>'

        # Generate adjective tags HTML
        adjective_tags = []
        for adjective in animal_data['adjectives']:
            adjective_tags.append(f'<span class="adjective-tag">{adjective}</span>')

        adjectives_html = '\n                    '.join(adjective_tags)

        # Generate the complete card HTML
        return f"""
            <div class="{card_class}" data-animal="{animal_name.lower()}">
                {image_html}
                {image_info}
                <div class="animal-content">
                    <h3 class="animal-name">{animal_name}</h3>
                    <div class="adjectives-section">
                        <div class="adjectives-label">
                            Collateral Adjectives ({len(animal_data['adjectives'])})
                        </div>
                        <div class="adjectives-list">
                            {adjectives_html}
                        </div>
                    </div>
                </div>
            </div>"""

    def _generate_search_and_filter_js(self) -> str:
        """
        Generate JavaScript for search and filter functionality.

        Returns:
            str: JavaScript code for interactive features
        """
        return """
        <script>
            // Search functionality
            function filterAnimals() {
                const searchTerm = document.getElementById('searchBox').value.toLowerCase();
                const animalCards = document.querySelectorAll('.animal-card');
                let visibleCount = 0;

                animalCards.forEach(card => {
                    const animalName = card.dataset.animal;
                    const adjectives = card.querySelectorAll('.adjective-tag');
                    let matches = false;

                    // Check if animal name matches
                    if (animalName.includes(searchTerm)) {
                        matches = true;
                    }

                    // Check if any adjective matches
                    adjectives.forEach(tag => {
                        if (tag.textContent.toLowerCase().includes(searchTerm)) {
                            matches = true;
                        }
                    });

                    if (matches || searchTerm === '') {
                        card.classList.remove('hidden');
                        visibleCount++;
                    } else {
                        card.classList.add('hidden');
                    }
                });

                // Update results count
                document.getElementById('resultsCount').textContent = 
                    `Showing ${visibleCount} animals`;
            }

            // Add event listener when page loads
            document.addEventListener('DOMContentLoaded', function() {
                const searchBox = document.getElementById('searchBox');
                if (searchBox) {
                    searchBox.addEventListener('input', filterAnimals);
                }
            });
        </script>"""

    def generate_html_report(self, animal_data: List[Dict[str, str]],
                             output_file: str = "animal_report.html") -> str:
        """
        Generate a comprehensive HTML report showing animals with their collateral adjectives.

        Args:
            animal_data (List[Dict[str, str]]): List of animal-adjective pairs
            output_file (str): Output HTML file name

        Returns:
            str: Path to generated HTML file
        """
        start_time = time.time()
        self.logger.info(f"Generating HTML report for {len(animal_data)} animal entries...")

        # Group animals by name and collect their adjectives
        grouped_animals = self._group_animals_by_name(animal_data)

        # Calculate statistics for the report
        stats = self._calculate_statistics(grouped_animals)

        # Generate HTML components
        html_head = self._generate_html_head()
        stats_panel = self._generate_stats_panel_html(stats)

        # Generate animal cards (sorted alphabetically)
        animal_cards = []
        sorted_animals = sorted(grouped_animals.keys(), key=str.lower)

        self.logger.info(f"Generating HTML cards for {len(sorted_animals)} animals...")

        for animal_name in sorted_animals:
            animal_data_entry = grouped_animals[animal_name]
            card_html = self._generate_animal_card_html(animal_name, animal_data_entry)
            animal_cards.append(card_html)

        # Join all animal cards
        animals_grid_html = '\n        '.join(animal_cards)

        # Generate search and filter JavaScript
        javascript = self._generate_search_and_filter_js()

        # Generate the complete HTML document
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")

        complete_html = f"""{html_head}
<body>
    <div class="container">
        <div class="header">
            <h1>🐾 Animal Collateral Adjectives</h1>
            <p class="subtitle">A comprehensive collection of animals and their descriptive adjectives</p>
            <p style="color: #95a5a6; font-size: 0.9em; margin-top: 10px;">
                Generated on {current_time} | Source: Wikipedia
            </p>
        </div>

        {stats_panel}

        <div style="text-align: center; margin-bottom: 30px;">
            <input type="text" id="searchBox" class="search-box" 
                   placeholder="🔍 Search animals or adjectives...">
            <p id="resultsCount" style="color: #7f8c8d; margin-top: 10px;">
                Showing {stats['total_animals']} animals
            </p>
        </div>

        <div class="animals-grid">
            {animals_grid_html}
        </div>

        <div class="footer">
            <p><strong>Data Source:</strong> <a href="https://en.wikipedia.org/wiki/List_of_animal_names" target="_blank">Wikipedia: List of Animal Names</a></p>
            <p><strong>Images:</strong> Downloaded to /tmp/ directory when available</p>
            <p><strong>Generation Time:</strong> {time.time() - start_time:.2f} seconds</p>
            <p style="font-size: 0.8em; margin-top: 15px;">
                This report shows each animal with all of its collateral adjectives.<br>
                Animals with multiple adjectives are highlighted with a colored border.
            </p>
        </div>
    </div>

    {javascript}
</body>
</html>"""

        # Write to file
        try:
            output_path = Path(output_file)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(complete_html)

            # Update generation statistics
            self.generation_stats['generation_time'] = time.time() - start_time

            self.logger.info(f"HTML report generated successfully: {output_path.absolute()}")
            self.logger.info(f"Report statistics: {stats}")
            self.logger.info(f"Generation time: {self.generation_stats['generation_time']:.2f} seconds")

            return str(output_path.absolute())

        except Exception as e:
            self.logger.error(f"Failed to write HTML file {output_file}: {e}")
            raise

    def get_generation_statistics(self) -> Dict[str, int]:
        """Get HTML generation statistics."""
        return self.generation_stats.copy()