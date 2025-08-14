import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import Tuple, List
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FarmReportProcessor:
    """
    Processes raw Farm Report data by keeping only selected columns,
    handling missing values, de-duplicating by owner, and splitting
    into individual vs business datasets.
    """

    def __init__(self, input_file: str, output_dir: str = "data"):
        self.input_file = input_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Columns to keep in the processed data
        self.selected_columns = [
            'APN', 'County Name', 'Property Address', 'City', 'State', 'ZIP Code',
            'Owner Name(s) Formatted', 'Owner 1 First Name', 'Mailing County Name', 'Mailing Address',
            'Mailing City', 'Mailing State', 'Mailing ZIP Code', 'Sale Type',
            'Bedrooms', 'Lot Size SF / Acre', 'Building / Living Area SF',
            'Detailed Property Type', 'Year Built', 'Property Type',
            'Delinquent Taxes', 'Foreclosure', '1st Mortgage Financing',
            '1st Mortgage Interest Rate', '1st Mortgage Lender Name - Originated',
            '1st Mortgage Loan Amount', '1st Mortgage Recording Date', 'Owner 2 First Name', 'Owner Type', 
            'Last Transaction Recording Date', 'Last Transaction Sale Date', 'Last Transaction Sale Price'
        ]
        
    def load_farm_report(self) -> pd.DataFrame:
        """Load and preprocess the farm report data."""
        try:
            # Convert input_file to Path object and resolve it
            file_path = Path(self.input_file).resolve()
            logger.info(f"Attempting to load file: {file_path}")
            
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
                
            # Load the file based on extension
            if file_path.suffix.lower() in ['.csv']:
                df = pd.read_csv(file_path)
            elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file format '{file_path.suffix}'. Please use CSV or Excel files.")

            # Drop duplicate columns by keeping the first occurrence to avoid DataFrame selections later
            if df.columns.duplicated().any():
                dup_count = int(df.columns.duplicated().sum())
                logger.warning(f"Detected {dup_count} duplicated column name(s); keeping first occurrence of each")
                df = df.loc[:, ~df.columns.duplicated()].copy()

            # Keep only the selected columns plus the separator column used for classification
            separator_column = 'Owner 1 First Name'
            missing_selected = [c for c in self.selected_columns if c not in df.columns]
            if missing_selected:
                logger.warning(f"Missing expected columns in input: {missing_selected}")

            working_columns: List[str] = [c for c in self.selected_columns if c in df.columns]
            if separator_column in df.columns:
                working_columns.append(separator_column)
            else:
                logger.warning(f"Separator column '{separator_column}' not found; all rows will be treated as business by default")

            # Ensure unique column selection order without duplicates
            seen = set()
            unique_working_columns = []
            for c in working_columns:
                if c not in seen:
                    unique_working_columns.append(c)
                    seen.add(c)

            df = df[unique_working_columns].copy()

            # Standardize dtypes and handle missing values for key numeric columns
            numeric_cols = [
                'Year Built', 'Bedrooms', 'Building / Living Area SF', 'Lot Size SF / Acre',
                '1st Mortgage Loan Amount', '1st Mortgage Interest Rate'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Use median for robustness to outliers
                    median_val = df[col].median()
                    df[col] = df[col].fillna(median_val)
                    if col == 'Year Built':
                        # Cast Year Built to integer if possible
                        df[col] = df[col].round().astype('Int64')

            # Clean up common text columns (strip whitespace)
            text_cols = [
                'APN', 'County Name', 'Property Address', 'City', 'State', 'ZIP Code',
                'Owner Name(s) Formatted', 'Owner 1 First Name', 'Mailing County Name', 'Mailing Address',
                'Mailing City', 'Mailing State', 'Mailing ZIP Code', 'Sale Type',
                'Detailed Property Type', 'Property Type', '1st Mortgage Financing',
                '1st Mortgage Lender Name - Originated'
            ]
            for col in text_cols:
                if col in df.columns:
                    # If duplicate names slipped through, select first series
                    series_like = df[col]
                    if isinstance(series_like, pd.DataFrame):
                        series_like = series_like.iloc[:, 0]
                    series_like = series_like.astype(str).str.strip()
                    series_like = series_like.replace({'nan': np.nan, 'None': np.nan})
                    df[col] = series_like

            # Handle ZIP codes (preserve as strings, blank when missing)
            if 'Mailing ZIP Code' in df.columns:
                df['Mailing ZIP Code'] = pd.to_numeric(df['Mailing ZIP Code'], errors='coerce')
                df['Mailing ZIP Code'] = df['Mailing ZIP Code'].astype('Int64').astype(str).replace({'<NA>': np.nan})
            if 'ZIP Code' in df.columns:
                df['ZIP Code'] = pd.to_numeric(df['ZIP Code'], errors='coerce')
                df['ZIP Code'] = df['ZIP Code'].astype('Int64').astype(str).replace({'<NA>': np.nan})

            # Create full address columns
            if all(c in df.columns for c in ['Property Address', 'City', 'State']):
                df['Property Full Address'] = df.apply(
                    lambda x: f"{x.get('Property Address', '')}, {x.get('City', '')}, {x.get('State', '')}, {x.get('ZIP Code', '')}".strip(', '),
                    axis=1
                )
            if all(c in df.columns for c in ['Mailing Address', 'Mailing City', 'Mailing State']):
                df['Mailing Full Address'] = df.apply(
                    lambda x: f"{x.get('Mailing Address', '')}, {x.get('Mailing City', '')}, {x.get('Mailing State', '')}, {x.get('Mailing ZIP Code', '')}".strip(', '),
                    axis=1
                )

            # De-duplicate by unique owner name
            owner_col = 'Owner Name(s) Formatted'
            if owner_col in df.columns:
                before = len(df)
                df = df.drop_duplicates(subset=[owner_col]).reset_index(drop=True)
                logger.info(f"Deduplicated by owner name: {before} -> {len(df)} rows")

            logger.info(f"Loaded and preprocessed farm report with {len(df)} rows and {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Error loading farm report: {e}")
            raise
    
    def process_farm_report(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Main processing function to keep only required columns, clean,
        deduplicate, and split into individual vs business datasets.
        """
        df = self.load_farm_report()

        # Split based on 'Owner 1 First Name': empty => business, else => individual
        separator_column = 'Owner 1 First Name'
        if separator_column in df.columns:
            # Treat '', None/NaN, and literal 'nan'/'None' as missing (i.e., business)
            sep_series = df[separator_column]
            if isinstance(sep_series, pd.DataFrame):
                sep_series = sep_series.iloc[:, 0]
            s_clean = sep_series.astype('string').str.strip()
            s_clean = s_clean.replace({'': pd.NA, 'nan': pd.NA, 'NaN': pd.NA, 'NONE': pd.NA, 'None': pd.NA})
            is_individual = s_clean.notna()
        else:
            # If not present, default all to business
            is_individual = pd.Series([False] * len(df), index=df.index)

        # Prepare outputs (drop the separator column from final outputs)
        cols_for_output = [c for c in df.columns if c != separator_column]
        individual_df = df.loc[is_individual, cols_for_output].reset_index(drop=True)
        business_df = df.loc[~is_individual, cols_for_output].reset_index(drop=True)

        logger.info(f"Split into individual: {len(individual_df)} rows, business: {len(business_df)} rows")
        return individual_df, business_df

    def save_datasets(self, individual_df: pd.DataFrame, business_df: pd.DataFrame) -> Tuple[Path, Path]:
        """Save the processed datasets to CSV and Excel."""
        individual_path = self.output_dir / "individual.csv"
        business_path = self.output_dir / "business.csv"

        individual_df.to_csv(individual_path, index=False)
        business_df.to_csv(business_path, index=False)

        # Also save as Excel for convenience
        individual_xlsx = self.output_dir / "individual.xlsx"
        business_xlsx = self.output_dir / "business.xlsx"
        individual_df.to_excel(individual_xlsx, index=False)
        business_df.to_excel(business_xlsx, index=False)

        logger.info(f"Saved datasets: {individual_path}, {business_path}")
        return individual_path, business_path

def process_farm_report(input_file: str, output_dir: str = "data") -> Tuple[str, str]:
    """
    Process farm report and create individual/business datasets.

    Args:
        input_file: Path to the farm report file (CSV or Excel)
        output_dir: Directory to save processed files

    Returns:
        Tuple of (individual_file_path, business_file_path)
    """
    processor = FarmReportProcessor(input_file, output_dir)
    individual_df, business_df = processor.process_farm_report()
    individual_path, business_path = processor.save_datasets(individual_df, business_df)

    return str(individual_path), str(business_path)

if __name__ == "__main__":
    # Example direct run
    script_dir = Path(__file__).resolve().parents[2]  # project root
    example_input = script_dir / "FARM Reports" / "FARM REPORT - Mixed Lenders - 03042025023246.XLSX"
    example_output = script_dir / "scraped_data"

    print(f"Processing file: {example_input}")
    print(f"Output directory: {example_output}")

    try:
        individual_path, business_path = process_farm_report(str(example_input), str(example_output))
        print(f"Successfully processed files:\nIndividual: {individual_path}\nBusiness: {business_path}")
    except Exception as e:
        print(f"Error processing farm report: {e}")