"""
Data processing module for handling and enriching animal data.
"""

import logging
from collections import defaultdict
from typing import List, Dict, Tuple

from utils.helpers import normalize_text, is_valid_animal_name


class AnimalDataProcessor:
    """
    Processes and enriches animal data with additional metadata and validation.
    """
    
    def __init__(self):
        """Initialize the data processor."""
        self.logger = logging.getLogger(__name__)
        
        # Track processed data for statistics
        self.processing_stats = {
            'original_entries': 0,
            'valid_entries': 0,
            'duplicate_entries': 0,
            'multiple_adjectives': 0,
            'animals_with_images': 0,
            'unique_animals': 0,
            'unique_adjectives': 0
        }
    
    def validate_animal_entry(self, entry: Dict[str, str]) -> bool:
        """
        Validate a single animal entry for completeness and correctness.
        
        Args:
            entry (Dict[str, str]): Animal entry to validate
            
        Returns:
            bool: True if entry is valid
        """
        # Check required fields
        if not entry.get('animal') or not entry.get('adjective'):
            self.logger.debug(f"Missing required fields in entry: {entry}")
            return False
        
        animal_name = entry['animal'].strip()
        adjective = entry['adjective'].strip()
        
        # Validate animal name
        if not is_valid_animal_name(animal_name):
            self.logger.debug(f"Invalid animal name: {animal_name}")
            return False
        
        # Check for reasonable adjective
        if len(adjective) < 2 or adjective.lower() in ['n/a', 'none', '—', '-']:
            self.logger.debug(f"Invalid adjective: {adjective}")
            return False
        
        # Filter out entries where animal name and adjective are the same
        if animal_name.lower() == adjective.lower():
            self.logger.debug(f"Animal name same as adjective: {animal_name}")
            return False
        
        return True
    
    def deduplicate_entries(self, animal_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Remove duplicate entries while preserving multiple adjectives per animal.
        
        Args:
            animal_data (List[Dict[str, str]]): List of animal entries
            
        Returns:
            List[Dict[str, str]]: Deduplicated list
        """
        seen_combinations = set()
        deduplicated_data = []
        
        for entry in animal_data:
            # Create a key for deduplication (animal + adjective combination)
            animal_key = normalize_text(entry['animal']).lower()
            adjective_key = normalize_text(entry['adjective']).lower()
            combination_key = (animal_key, adjective_key)
            
            if combination_key not in seen_combinations:
                seen_combinations.add(combination_key)
                deduplicated_data.append(entry)
            else:
                self.processing_stats['duplicate_entries'] += 1
                self.logger.debug(f"Duplicate entry removed: {entry['animal']} -> {entry['adjective']}")
        
        return deduplicated_data
    
    def enrich_entries_with_metadata(self, animal_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Enrich animal entries with additional metadata and analysis.
        
        Args:
            animal_data (List[Dict[str, str]]): List of animal entries
            
        Returns:
            List[Dict[str, str]]: Enriched list with metadata
        """
        enriched_data = []
        
        for entry in animal_data:
            enriched_entry = entry.copy()
            
            # Add normalized versions for consistent processing
            enriched_entry['animal_normalized'] = normalize_text(entry['animal'])
            enriched_entry['adjective_normalized'] = normalize_text(entry['adjective'])
            
            # Add word counts
            enriched_entry['animal_word_count'] = len(entry['animal'].split())
            enriched_entry['adjective_word_count'] = len(entry['adjective'].split())
            
            # Add character counts
            enriched_entry['animal_char_count'] = len(entry['animal'])
            enriched_entry['adjective_char_count'] = len(entry['adjective'])
            
            # Check if it's a compound animal name
            enriched_entry['is_compound_animal'] = ' ' in entry['animal'] or '-' in entry['animal']
            
            # Check if it's a compound adjective
            enriched_entry['is_compound_adjective'] = ' ' in entry['adjective'] or '-' in entry['adjective']
            
            # Add image status
            enriched_entry['has_image'] = bool(entry.get('local_image_path'))
            enriched_entry['image_download_successful'] = entry.get('download_success', False)
            
            enriched_data.append(enriched_entry)
        
        return enriched_data
    
    def analyze_data_patterns(self, animal_data: List[Dict[str, str]]) -> Dict:
        """
        Analyze patterns in the animal data for insights.
        
        Args:
            animal_data (List[Dict[str, str]]): List of animal entries
            
        Returns:
            Dict: Analysis results and patterns
        """
        analysis = {
            'animals_by_adjective_count': defaultdict(int),
            'adjectives_by_animal_count': defaultdict(int),
            'most_common_adjective_endings': defaultdict(int),
            'most_common_animal_types': defaultdict(int),
            'compound_animals': [],
            'compound_adjectives': [],
            'longest_animal_names': [],
            'longest_adjectives': [],
            'animals_with_multiple_adjectives': defaultdict(list)
        }
        
        # Group animals by their adjectives and vice versa
        animal_to_adjectives = defaultdict(set)
        adjective_to_animals = defaultdict(set)
        
        for entry in animal_data:
            animal = entry['animal_normalized']
            adjective = entry['adjective_normalized']
            
            animal_to_adjectives[animal].add(adjective)
            adjective_to_animals[adjective].add(animal)
        
        # Count occurrences
        for animal, adjectives in animal_to_adjectives.items():
            analysis['animals_by_adjective_count'][len(adjectives)] += 1
            if len(adjectives) > 1:
                analysis['animals_with_multiple_adjectives'][animal] = list(adjectives)
        
        for adjective, animals in adjective_to_animals.items():
            analysis['adjectives_by_animal_count'][len(animals)] += 1
        
        # Analyze adjective endings (common suffixes)
        for entry in animal_data:
            adjective = entry['adjective_normalized'].lower()
            if len(adjective) >= 3:
                # Check common endings
                for suffix_len in [2, 3, 4]:
                    if len(adjective) >= suffix_len:
                        suffix = adjective[-suffix_len:]
                        analysis['most_common_adjective_endings'][suffix] += 1
        
        # Find compound names
        for entry in animal_data:
            if entry.get('is_compound_animal'):
                analysis['compound_animals'].append(entry['animal'])
            if entry.get('is_compound_adjective'):
                analysis['compound_adjectives'].append(entry['adjective'])
        
        # Find longest names
        sorted_by_animal_length = sorted(animal_data, key=lambda x: len(x['animal']), reverse=True)
        sorted_by_adjective_length = sorted(animal_data, key=lambda x: len(x['adjective']), reverse=True)
        
        analysis['longest_animal_names'] = [
            (entry['animal'], len(entry['animal'])) 
            for entry in sorted_by_animal_length[:10]
        ]
        analysis['longest_adjectives'] = [
            (entry['adjective'], len(entry['adjective'])) 
            for entry in sorted_by_adjective_length[:10]
        ]
        
        return analysis
    
    def process_animal_data(self, raw_animal_data: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], Dict]:
        """
        Main processing method that validates, deduplicates, and enriches animal data.
        
        Args:
            raw_animal_data (List[Dict[str, str]]): Raw animal data from scraper
            
        Returns:
            Tuple[List[Dict[str, str]], Dict]: (processed_data, analysis_results)
        """
        self.logger.info(f"Processing {len(raw_animal_data)} raw animal entries")
        
        # Reset statistics
        self.processing_stats = {
            'original_entries': len(raw_animal_data),
            'valid_entries': 0,
            'duplicate_entries': 0,
            'multiple_adjectives': 0,
            'animals_with_images': 0,
            'unique_animals': 0,
            'unique_adjectives': 0
        }
        
        # Step 1: Validate entries
        valid_entries = []
        for entry in raw_animal_data:
            if self.validate_animal_entry(entry):
                valid_entries.append(entry)
                self.processing_stats['valid_entries'] += 1
            else:
                self.logger.debug(f"Invalid entry filtered out: {entry}")
        
        self.logger.info(f"Validated {len(valid_entries)} entries out of {len(raw_animal_data)}")
        
        # Step 2: Deduplicate
        deduplicated_entries = self.deduplicate_entries(valid_entries)
        self.logger.info(f"After deduplication: {len(deduplicated_entries)} entries")
        
        # Step 3: Enrich with metadata
        enriched_entries = self.enrich_entries_with_metadata(deduplicated_entries)
        
        # Step 4: Calculate final statistics
        unique_animals = set(entry['animal_normalized'].lower() for entry in enriched_entries)
        unique_adjectives = set(entry['adjective_normalized'].lower() for entry in enriched_entries)
        
        self.processing_stats['unique_animals'] = len(unique_animals)
        self.processing_stats['unique_adjectives'] = len(unique_adjectives)
        self.processing_stats['animals_with_images'] = sum(
            1 for entry in enriched_entries if entry.get('has_image', False)
        )
        
        # Count animals with multiple adjectives
        animal_adjective_count = defaultdict(int)
        for entry in enriched_entries:
            animal_adjective_count[entry['animal_normalized'].lower()] += 1
        
        self.processing_stats['multiple_adjectives'] = sum(
            1 for count in animal_adjective_count.values() if count > 1
        )
        
        # Step 5: Perform analysis
        analysis_results = self.analyze_data_patterns(enriched_entries)
        
        self.logger.info("Data processing completed successfully")
        self.logger.info(f"Final statistics: {self.processing_stats}")
        
        return enriched_entries, analysis_results
    
    def get_processing_statistics(self) -> Dict[str, int]:
        """Get processing statistics."""
        return self.processing_stats.copy()
    
    def sort_entries(self, animal_data: List[Dict[str, str]], sort_by: str = 'animal') -> List[Dict[str, str]]:
        """
        Sort animal entries by specified field.
        
        Args:
            animal_data (List[Dict[str, str]]): List of animal entries
            sort_by (str): Field to sort by ('animal', 'adjective', or 'has_image')
            
        Returns:
            List[Dict[str, str]]: Sorted list
        """
        if sort_by == 'animal':
            return sorted(animal_data, key=lambda x: x['animal_normalized'].lower())
        elif sort_by == 'adjective':
            return sorted(animal_data, key=lambda x: x['adjective_normalized'].lower())
        elif sort_by == 'has_image':
            # Sort by image availability, then by animal name
            return sorted(animal_data, key=lambda x: (not x.get('has_image', False), x['animal_normalized'].lower()))
        else:
            self.logger.warning(f"Unknown sort field: {sort_by}, defaulting to 'animal'")
            return sorted(animal_data, key=lambda x: x['animal_normalized'].lower())
