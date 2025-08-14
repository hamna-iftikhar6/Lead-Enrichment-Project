"""
Scraping module for data enrichment from various sources.
"""

from .fps_scraper import FPSScraper, enrich_borrowers, enrich_lenders

__all__ = ['FPSScraper', 'enrich_borrowers', 'enrich_lenders'] 