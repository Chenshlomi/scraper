"""
Main entry point for the animal scraper application.
This module orchestrates the entire process of scraping, processing, and displaying animal data.
"""

import time
import sys
from pathlib import Path
from typing import List, Dict

from formatter.html_formatter import HTMLFormatter
# Import our custom modules
from utils.helpers import setup_logging
from scraper.wikipedia_animal_scraper import WikipediaAnimalScraper
from downloader.image_downloader import ImageDownloader
from processor.data_processor import AnimalDataProcessor



class AnimalScraperApplication:
    """
    Main application class that orchestrates the entire animal scraping process.
    """
    
    def __init__(self):
        """Initialize the application with all necessary components."""
        # Set up logging first
        self.logger = setup_logging()
        self.logger.info("Starting Animal Scraper Application")
        
        # Initialize all components
        self.scraper = WikipediaAnimalScraper()
        self.image_downloader = ImageDownloader()
        self.data_processor = AnimalDataProcessor()
        self.output_formatter = HTMLFormatter()
        
        # Track application statistics
        self.app_stats = {
            'start_time': time.time(),
            'scraping_time': 0,
            'processing_time': 0,
            'download_time': 0,
            'total_time': 0
        }
    
    def run_scraping_phase(self) -> List[Dict[str, str]]:
        """
        Execute the web scraping phase to get raw animal data.
        
        Returns:
            List[Dict[str, str]]: Raw animal data from Wikipedia
        """
        self.logger.info("Phase 1: Web Scraping")
        phase_start = time.time()
        
        try:
            # Scrape the Wikipedia page for animal data
            raw_animal_data = self.scraper.scrape_animals_and_adjectives()
            
            if not raw_animal_data:
                self.logger.error("No animal data was scraped from Wikipedia")
                return []
            
            self.logger.info(f"Successfully scraped {len(raw_animal_data)} animal entries")

            # Log some sample entries for verification
            self.logger.info("Sample scraped entries:")
            for i, entry in enumerate(raw_animal_data[:3]):
                self.logger.info(f"  {i+1}. {entry['animal']} -> {entry['adjective']}")

            self.app_stats['scraping_time'] = time.time() - phase_start
            return raw_animal_data
            
        except Exception as e:
            self.logger.error(f"Error during scraping phase: {e}")
            raise
    
    def run_processing_phase(self, raw_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Execute the data processing phase to clean and enrich the data.
        
        Args:
            raw_data (List[Dict[str, str]]): Raw animal data from scraping
            
        Returns:
            List[Dict[str, str]]: Processed and enriched animal data
        """
        self.logger.info("Phase 2: Data Processing")
        phase_start = time.time()
        
        try:
            # Process the raw data (validate, deduplicate, enrich)
            processed_data, analysis_results = self.data_processor.process_animal_data(raw_data)
            
            if not processed_data:
                self.logger.error("No valid data after processing")
                return []
            
            self.logger.info(f"Processing completed: {len(processed_data)} valid entries")
            
            # Log processing statistics
            proc_stats = self.data_processor.get_processing_statistics()
            self.logger.info(f"Processing stats: {proc_stats}")
            
            self.app_stats['processing_time'] = time.time() - phase_start
            return processed_data
            
        except Exception as e:
            self.logger.error(f"Error during processing phase: {e}")
            raise
    
    def run_download_phase(self, processed_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Execute the image downloading phase using threading for concurrent downloads.
        
        Args:
            processed_data (List[Dict[str, str]]): Processed animal data
            
        Returns:
            List[Dict[str, str]]: Data with local image paths added
        """
        self.logger.info("Phase 3: Image Downloading (with Threading)")
        phase_start = time.time()
        
        try:
            # This is where we use threading - the ImageDownloader will
            # download multiple images concurrently using ThreadPoolExecutor
            data_with_images = self.image_downloader.download_images_concurrently(processed_data)
            
            # Log download statistics
            download_stats = self.image_downloader.get_download_statistics()
            self.logger.info(f"Download stats: {download_stats}")
            
            # Show some successful downloads
            successful_downloads = [
                entry for entry in data_with_images 
                if entry.get('download_success', False)
            ][:5]
            
            if successful_downloads:
                self.logger.info("Sample successful downloads:")
                for entry in successful_downloads:
                    self.logger.info(f"  ✓ {entry['animal']}: {entry['local_image_path']}")
            
            self.app_stats['download_time'] = time.time() - phase_start
            return data_with_images
            
        except Exception as e:
            self.logger.error(f"Error during download phase: {e}")
            raise
    
    def run_output_phase(self, final_data: List[Dict[str, str]]) -> None:
        """
        Execute the output phase to display and save results.
        
        Args:
            final_data (List[Dict[str, str]]): Final processed data with images
        """
        self.logger.info("Phase 4: Output Generation")
        
        try:
            self.output_formatter.generate_html_report(final_data)
            self.logger.info("Output phase completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during output phase: {e}")
            raise
    
    def calculate_final_statistics(self) -> None:
        """Calculate and log final application statistics."""
        self.app_stats['total_time'] = time.time() - self.app_stats['start_time']
        
        self.logger.info("=" * 60)
        self.logger.info("FINAL APPLICATION STATISTICS")
        self.logger.info("=" * 60)
        self.logger.info(f"Scraping time: {self.app_stats['scraping_time']:.2f}s")
        self.logger.info(f"Processing time: {self.app_stats['processing_time']:.2f}s")
        self.logger.info(f"Download time: {self.app_stats['download_time']:.2f}s")
        self.logger.info(f"Total execution time: {self.app_stats['total_time']:.2f}s")
        
        # Calculate efficiency metrics
        if self.app_stats['total_time'] > 0:
            scraping_percent = (self.app_stats['scraping_time'] / self.app_stats['total_time']) * 100
            processing_percent = (self.app_stats['processing_time'] / self.app_stats['total_time']) * 100
            download_percent = (self.app_stats['download_time'] / self.app_stats['total_time']) * 100
            
            self.logger.info(f"Time breakdown: Scraping {scraping_percent:.1f}%, "
                           f"Processing {processing_percent:.1f}%, "
                           f"Downloading {download_percent:.1f}%")
    
    def cleanup_resources(self) -> None:
        """Clean up resources and close connections."""
        self.logger.info("Cleaning up resources...")
        
        try:
            self.scraper.close()
            self.image_downloader.close()
            self.logger.info("All resources cleaned up successfully")
        except Exception as e:
            self.logger.warning(f"Error during cleanup: {e}")
    
    def run(self) -> bool:
        """
        Main execution method that runs the entire animal scraping pipeline.
        
        Returns:
            bool: True if execution completed successfully
        """
        self.logger.info("Starting Animal Scraper Pipeline")
        
        try:
            # Phase 1: Web Scraping
            raw_data = self.run_scraping_phase()
            if not raw_data:
                self.logger.error("Scraping phase failed - aborting")
                return False
            
            # Phase 2: Data Processing 
            processed_data = self.run_processing_phase(raw_data)
            if not processed_data:
                self.logger.error("Processing phase failed - aborting")
                return False
            
            # Phase 3: Image Downloading (with Threading)
            # This is where the threading magic happens - multiple images downloaded concurrently
            final_data = self.run_download_phase(processed_data)
            
            # Phase 4: Output Generation
            self.run_output_phase(final_data)
            
            # Calculate final statistics
            self.calculate_final_statistics()
            
            self.logger.info("Animal Scraper Pipeline completed successfully!")
            return True
            
        except KeyboardInterrupt:
            self.logger.info("Application interrupted by user")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error in main pipeline: {e}", exc_info=True)
            return False
        finally:
            # Always clean up resources
            self.cleanup_resources()


def main():
    """
    Entry point for the command-line application.
    """
    print("=" * 80)
    print("🐾 WIKIPEDIA ANIMAL NAMES AND COLLATERAL ADJECTIVES SCRAPER")
    print("=" * 80)
    print("This application demonstrates production-grade Python software engineering:")
    print("• Modular architecture with clear separation of concerns")
    print("• Threading for concurrent image downloads")
    print("• Comprehensive error handling and logging")
    print("• Data validation and processing pipeline")
    print("• HTML formatter")
    print("• Professional code structure and documentation")
    print("-" * 80)
    
    # Check if /tmp directory is accessible
    tmp_dir = Path("/tmp")
    if not tmp_dir.exists() or not tmp_dir.is_dir():
        print("Error: /tmp directory not accessible. Images will not be downloaded.")
        print("Please ensure /tmp directory exists and is writable.")
        return 1
    
    # Create and run the application
    app = AnimalScraperApplication()
    
    try:
        success = app.run()
        
        if success:
            print("\nApplication completed successfully!")
            print("Check the generated output files for detailed results.")
            print("Check /tmp directory for downloaded animal images.")
            return 0
        else:
            print("\nApplication completed with errors.")
            return 1
            
    except Exception as e:
        print(f"\nFatal error: {e}")
        return 1


if __name__ == "__main__":
    # Set up proper exit handling
    exit_code = main()
    sys.exit(exit_code)
