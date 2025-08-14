# Bayesian Lead Enrichment Engine

A comprehensive lead enrichment pipeline that processes farm reports, separates borrowers and lenders, and enriches contact data using multiple sources including FastPeopleSearch.

## 🚀 Features

### Phase 1: Streamlit Web Application
- **Home Page**: Comprehensive app overview and instructions
- **Upload & Inspect**: File upload with data analysis and visualizations
- **Parameter Configuration**: Advanced settings for enrichment process

### Phase 2: Data Preprocessing
- **Farm Report Processing**: Automatically separates borrowers and lenders
- **Data Cleaning**: Removes business entities and normalizes names
- **Column Identification**: Smart detection of borrower/lender columns
- **Address Standardization**: Formats addresses for enrichment

### Phase 3: Parameter Configuration
- **Lead Type Selection**: Individual, Business, or Mixed
- **Enrichment Sources**: Configure multiple data sources
- **Processing Limits**: Set batch sizes and lead counts
- **Timing Configuration**: Adjust delays and retry settings
- **Data Quality Settings**: Confidence thresholds and validation

### Phase 4: Enrichment Integration
- **FastPeopleSearch Scraper**: Enhanced with anti-detection measures
- **Modular Design**: Easy to add new data sources
- **Retry Logic**: Handles failures and access denied errors
- **Progress Tracking**: Saves intermediate results
- **Summary Reports**: Detailed enrichment statistics

## 📁 Project Structure

```
project-root/
│
├── app.py                          # Main Streamlit application
├── pages/
│   ├── 1_Upload_and_Inspect.py    # Phase 2: Upload & Preview UI
│   └── 2_Parameter_Form.py        # Phase 3: Parameter Configuration
│
├── src/
│   ├── preprocessing/
│   │   ├── __init__.py
│   │   └── farm_report_processor.py # Farm report data processing
│   ├── scraping/
│   │   ├── __init__.py
│   │   └── fps_scraper.py         # FastPeopleSearch scraper
│   ├── enrich/
│   │   ├── __init__.py
│   │   └── enrichment_orchestrator.py # Main pipeline orchestrator
│   ├── scorer/
│   │   └── __init__.py
│   └── utils/
│       └── __init__.py
│
├── FARM Reports/                   # Output directory for processed files
├── config/                         # Configuration files
├── logs/                          # Log files
├── pyproject.toml                 # Poetry configuration
└── README.md                      # This file
```

## 🛠️ Installation

### Prerequisites
- Python 3.8+
- Chrome browser (for web scraping)
- ChromeDriver (automatically managed by undetected-chromedriver)

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd lead-enrichment
   ```

2. **Install dependencies with Poetry**
   ```bash
   poetry install
   ```

3. **Activate the virtual environment**
   ```bash
   poetry shell
   ```

4. **Run the Streamlit application**
   ```bash
   poetry run streamlit run app.py
   ```

## 📊 Usage

### 1. Web Application (Phases 1-3)

1. **Start the app**: `poetry run streamlit run app.py`
2. **Upload Farm Report**: Use the "Upload & Inspect" page to upload your CSV/Excel file
3. **Configure Parameters**: Use the "Parameter Configuration" page to set enrichment settings
4. **Review Data**: Analyze your farm report data with built-in visualizations

### 2. Command Line Processing (Phase 4)

1. **Place your farm report** in the `FARM Reports/` directory
2. **Run the enrichment pipeline**:
   ```bash
   poetry run python -m src.enrich.enrichment_orchestrator
   ```

### 3. Programmatic Usage

```python
from src.enrich import run_enrichment_pipeline

# Run the complete pipeline
results = run_enrichment_pipeline("FARM Reports/raw_farm_report.csv")

print(f"Borrowers enriched: {results['borrowers_file']}")
print(f"Lenders enriched: {results['lenders_file']}")
```

## 🔧 Configuration

### Parameter Configuration (Phase 3)

The system supports comprehensive configuration through the web interface:

- **Lead Type**: Individual, Business, or Mixed
- **Processing Limits**: Maximum leads and batch sizes
- **Timing**: Delays between requests and retry settings
- **Enrichment Sources**: Enable/disable data sources
- **Data Quality**: Confidence thresholds and validation
- **Output Settings**: Format and summary options

### Configuration File

Settings are saved to `config/enrichment_config.json`:

```json
{
  "lead_type": "Individual",
  "max_leads": 1000,
  "batch_size": 20,
  "min_delay": 3,
  "max_delay": 8,
  "max_retries": 3,
  "sources": {
    "FastPeopleSearch": {
      "enabled": true,
      "priority": 1,
      "settings": {
        "extract_phones": true,
        "extract_emails": true,
        "extract_age": true,
        "extract_relatives": true,
        "extract_addresses": true
      }
    }
  }
}
```

## 📈 Data Processing Pipeline

### Phase 1: Preprocessing
1. **Load Farm Report**: CSV or Excel file
2. **Identify Columns**: Automatically detect borrower/lender columns
3. **Clean Data**: Remove business entities and normalize names
4. **Separate Datasets**: Create borrowers.csv and lenders.csv
5. **Add Enrichment Columns**: Prepare for data enrichment

### Phase 2: Enrichment
1. **Load Processed Data**: Read borrowers/lenders CSV files
2. **Configure Scraper**: Apply timing and retry settings
3. **Search & Extract**: Query FastPeopleSearch for each person
4. **Handle Variations**: Try different name/address combinations
5. **Save Results**: Update CSV files with enriched data

### Phase 3: Summary
1. **Analyze Results**: Calculate enrichment rates
2. **Generate Report**: Create detailed summary
3. **Export Data**: Save enriched datasets

## 🔍 Enrichment Data Sources

### FastPeopleSearch
- **Phone Numbers**: Up to 5 phone numbers per person
- **Email Addresses**: Available email addresses
- **Age Information**: Current age if available
- **Relatives**: Family members and associates
- **Address History**: Previous and current addresses
- **Background Info**: Marital status and background summary
- **FAQs**: Frequently asked questions about the person

### Future Sources (Planned)
- **Spokeo**: Comprehensive people search
- **PeopleDataLabs**: Professional contact data
- **LinkedIn**: Professional profiles

## 📊 Output Files

### Processed Files
- `borrowers.csv`: Separated borrower data with enrichment columns
- `lenders.csv`: Separated lender data with enrichment columns
- `borrowers_enriched.csv`: Final enriched borrower data
- `lenders_enriched.csv`: Final enriched lender data

### Summary Reports
- `enrichment_summary_YYYYMMDD_HHMMSS.json`: Detailed enrichment statistics
- `logs/enrichment_YYYYMMDD_HHMMSS.log`: Processing logs

## 🛡️ Anti-Detection Features

### FastPeopleSearch Scraper
- **Undetected ChromeDriver**: Bypasses bot detection
- **Random Delays**: Variable timing between requests
- **User Agent Rotation**: Multiple browser signatures
- **Session Management**: Maintains browser sessions
- **Access Denied Handling**: Automatic retry with progressive delays
- **Proxy Support**: Configurable proxy usage

## 🔧 Advanced Features

### Data Quality
- **Confidence Scoring**: Rate match quality
- **Duplicate Detection**: Remove duplicate records
- **Data Validation**: Verify phone numbers and emails
- **Error Handling**: Graceful failure recovery

### Performance
- **Batch Processing**: Process records in configurable batches
- **Progress Tracking**: Save intermediate results
- **Resume Capability**: Continue from interruption
- **Logging**: Comprehensive activity logging

### Extensibility
- **Modular Design**: Easy to add new data sources
- **Configuration Driven**: Settings via JSON files
- **Plugin Architecture**: Extensible scraper framework

## 🚨 Important Notes

### Legal Compliance
- **Respectful Scraping**: Built-in delays and rate limiting
- **Terms of Service**: Ensure compliance with data source terms
- **Data Privacy**: Handle personal data responsibly

### Technical Requirements
- **Chrome Browser**: Required for web scraping
- **Internet Connection**: Stable connection for data enrichment
- **Storage Space**: Adequate space for processed files

### Best Practices
- **Start Small**: Test with small datasets first
- **Monitor Logs**: Check log files for issues
- **Backup Data**: Keep original files safe
- **Validate Results**: Review enriched data quality

## 🤝 Contributing

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/new-scraper`
3. **Add your changes**: Implement new data sources or improvements
4. **Test thoroughly**: Ensure all functionality works
5. **Submit pull request**: Include detailed description

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the logs in the `logs/` directory
2. Review the configuration in `config/enrichment_config.json`
3. Ensure Chrome and ChromeDriver are properly installed
4. Verify your farm report format matches expected structure

## 🔄 Version History

- **v0.1.0**: Initial implementation with FastPeopleSearch integration
- **Phase 1**: Streamlit web application with upload and inspection
- **Phase 2**: Data preprocessing and separation
- **Phase 3**: Parameter configuration interface
- **Phase 4**: Enhanced FPS scraper with anti-detection measures 
