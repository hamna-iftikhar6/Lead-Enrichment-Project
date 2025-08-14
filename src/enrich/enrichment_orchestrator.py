import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
from datetime import datetime

# Import our modules
from src.preprocessing.farm_report_processor import process_farm_report
from src.scraping.fps_scraper import enrich_borrowers, enrich_lenders

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnrichmentOrchestrator:
    """
    Main orchestrator for the lead enrichment pipeline.
    Coordinates preprocessing and enrichment phases.
    """
    
    def __init__(self, config_file: str = "config/enrichment_config.json"):
        self.config_file = config_file
        self.config = self.load_config()
        self.output_dir = Path("FARM Reports")
        self.output_dir.mkdir(exist_ok=True)
        
        # Create logs directory
        self.logs_dir = Path("logs")
        self.logs_dir.mkdir(exist_ok=True)
        
        # Set up logging to file
        self.setup_logging()
    
    def setup_logging(self):
        """Set up logging to both console and file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.logs_dir / f"enrichment_{timestamp}.log"
        
        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(file_handler)
        
        logger.info(f"Enrichment session started. Log file: {log_file}")
    
    def load_config(self) -> Dict:
        """Load configuration from file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                logger.info("Configuration loaded successfully")
                return config
            else:
                logger.warning("No configuration file found, using defaults")
                return self.get_default_config()
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return self.get_default_config()
    
    def get_default_config(self) -> Dict:
        """Get default configuration"""
        return {
            'lead_type': 'Individual',
            'max_leads': 1000,
            'batch_size': 20,
            'min_delay': 3,
            'max_delay': 8,
            'max_retries': 3,
            'min_confidence': 0.7,
            'handle_duplicates': True,
            'validate_data': True,
            'output_format': 'CSV',
            'include_original': True,
            'create_summary': True,
            'use_proxy': False,
            'rotate_user_agents': True,
            'maintain_sessions': True,
            'continue_on_error': True,
            'logging_level': 'INFO',
            'save_intermediate': True,
            'sources': {
                'FastPeopleSearch': {
                    'enabled': True,
                    'priority': 1,
                    'settings': {
                        'extract_phones': True,
                        'extract_emails': True,
                        'extract_age': True,
                        'extract_relatives': True,
                        'extract_addresses': True
                    }
                }
            }
        }
    
    def validate_input_file(self, input_file: str) -> bool:
        """Validate input file exists and is readable"""
        if not os.path.exists(input_file):
            logger.error(f"Input file not found: {input_file}")
            return False
        
        # Check file extension
        valid_extensions = ['.csv', '.xlsx', '.xls']
        file_ext = Path(input_file).suffix.lower()
        
        if file_ext not in valid_extensions:
            logger.error(f"Unsupported file format: {file_ext}")
            return False
        
        logger.info(f"Input file validated: {input_file}")
        return True
    
    def run_preprocessing(self, input_file: str) -> Tuple[str, str]:
        """
        Run the preprocessing phase to separate borrowers and lenders
        
        Args:
            input_file: Path to the farm report file
            
        Returns:
            Tuple of (borrowers_file_path, lenders_file_path)
        """
        logger.info("Starting preprocessing phase...")
        
        try:
            # Validate input file
            if not self.validate_input_file(input_file):
                raise ValueError("Invalid input file")
            
            # Process farm report
            borrowers_path, lenders_path = process_farm_report(input_file, str(self.output_dir))
            
            logger.info(f"Preprocessing completed successfully")
            logger.info(f"Borrowers file: {borrowers_path}")
            logger.info(f"Lenders file: {lenders_path}")
            
            return borrowers_path, lenders_path
            
        except Exception as e:
            logger.error(f"Error in preprocessing phase: {e}")
            raise
    
    def run_enrichment(self, borrowers_file: str = None, lenders_file: str = None):
        """
        Run the enrichment phase using FPS scraper
        
        Args:
            borrowers_file: Path to borrowers CSV file
            lenders_file: Path to lenders CSV file
        """
        logger.info("Starting enrichment phase...")
        
        try:
            # Check if files exist
            if borrowers_file and os.path.exists(borrowers_file):
                logger.info(f"Enriching borrowers from: {borrowers_file}")
                enrich_borrowers(borrowers_file)
            
            if lenders_file and os.path.exists(lenders_file):
                logger.info(f"Enriching lenders from: {lenders_file}")
                enrich_lenders(lenders_file)
            
            logger.info("Enrichment phase completed successfully")
            
        except Exception as e:
            logger.error(f"Error in enrichment phase: {e}")
            raise
    
    def create_summary_report(self, borrowers_file: str = None, lenders_file: str = None):
        """Create a summary report of the enrichment results"""
        logger.info("Creating summary report...")
        
        try:
            summary_data = {
                'timestamp': datetime.now().isoformat(),
                'config': self.config,
                'borrowers': {},
                'lenders': {}
            }
            
            # Analyze borrowers results
            if borrowers_file and os.path.exists(borrowers_file):
                borrowers_df = pd.read_csv(borrowers_file)
                summary_data['borrowers'] = {
                    'total_records': len(borrowers_df),
                    'enriched_records': borrowers_df['phone1'].notna().sum(),
                    'enrichment_rate': (borrowers_df['phone1'].notna().sum() / len(borrowers_df)) * 100,
                    'columns_with_data': {
                        'phones': borrowers_df[['phone1', 'phone2', 'phone3', 'phone4', 'phone5']].notna().any(axis=1).sum(),
                        'emails': borrowers_df['emails'].notna().sum(),
                        'age': borrowers_df['age'].notna().sum(),
                        'relatives': borrowers_df['relatives'].notna().sum(),
                        'addresses': borrowers_df['previous_addresses'].notna().sum()
                    }
                }
            
            # Analyze lenders results
            if lenders_file and os.path.exists(lenders_file):
                lenders_df = pd.read_csv(lenders_file)
                summary_data['lenders'] = {
                    'total_records': len(lenders_df),
                    'enriched_records': lenders_df['phone1'].notna().sum(),
                    'enrichment_rate': (lenders_df['phone1'].notna().sum() / len(lenders_df)) * 100,
                    'columns_with_data': {
                        'phones': lenders_df[['phone1', 'phone2', 'phone3', 'phone4', 'phone5']].notna().any(axis=1).sum(),
                        'emails': lenders_df['emails'].notna().sum(),
                        'age': lenders_df['age'].notna().sum(),
                        'relatives': lenders_df['relatives'].notna().sum(),
                        'addresses': lenders_df['previous_addresses'].notna().sum()
                    }
                }
            
            # Save summary report
            summary_file = self.output_dir / f"enrichment_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(summary_file, 'w') as f:
                json.dump(summary_data, f, indent=2)
            
            logger.info(f"Summary report saved to: {summary_file}")
            return summary_data
            
        except Exception as e:
            logger.error(f"Error creating summary report: {e}")
            return None
    
    def run_full_pipeline(self, input_file: str):
        """
        Run the complete enrichment pipeline
        
        Args:
            input_file: Path to the raw farm report file
        """
        logger.info("Starting full enrichment pipeline...")
        
        try:
            # Phase 1: Preprocessing
            logger.info("=== Phase 1: Preprocessing ===")
            borrowers_file, lenders_file = self.run_preprocessing(input_file)
            
            # Phase 2: Enrichment
            logger.info("=== Phase 2: Enrichment ===")
            self.run_enrichment(borrowers_file, lenders_file)
            
            # Phase 3: Summary Report
            logger.info("=== Phase 3: Summary Report ===")
            summary = self.create_summary_report(borrowers_file, lenders_file)
            
            logger.info("Full pipeline completed successfully!")
            return {
                'borrowers_file': borrowers_file,
                'lenders_file': lenders_file,
                'summary': summary
            }
            
        except Exception as e:
            logger.error(f"Error in full pipeline: {e}")
            raise

def run_enrichment_pipeline(input_file: str, config_file: str = None):
    """
    Main function to run the enrichment pipeline
    
    Args:
        input_file: Path to the raw farm report file
        config_file: Path to configuration file (optional)
    """
    try:
        # Initialize orchestrator
        orchestrator = EnrichmentOrchestrator(config_file)
        
        # Run full pipeline
        results = orchestrator.run_full_pipeline(input_file)
        
        print("✅ Enrichment pipeline completed successfully!")
        print(f"📁 Borrowers file: {results['borrowers_file']}")
        print(f"📁 Lenders file: {results['lenders_file']}")
        
        if results['summary']:
            print(f"📊 Enrichment Summary:")
            print(f"   Borrowers: {results['summary']['borrowers']['enrichment_rate']:.1f}% enriched")
            print(f"   Lenders: {results['summary']['lenders']['enrichment_rate']:.1f}% enriched")
        
        return results
        
    except Exception as e:
        print(f"❌ Error in enrichment pipeline: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    input_file = "FARM Reports/raw_farm_report.csv"
    
    if os.path.exists(input_file):
        run_enrichment_pipeline(input_file)
    else:
        print(f"Input file not found: {input_file}")
        print("Please place your farm report file in the FARM Reports directory") 